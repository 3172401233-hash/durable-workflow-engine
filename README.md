# durable-workflow-engine

Python 持久工作流引擎：步骤日志、活动去重、超时恢复和补偿执行。
以订单编排为例：扣减库存 → 创建支付 → 发送通知，任何一步都是远程调用，
worker 可能在任意时刻崩溃。

## 核心模型

一切可见状态都来自**已提交的追加式事件日志**（`EventLog`，JSONL，append 时
`fsync`）。引擎不持有任何有意义的内存状态；`get_state()` 永远是对日志做 fold
的结果，换一个进程、换一个引擎实例读到完全一致。

```
durable_workflow/
  log.py      # 追加式事件日志（唯一事实来源）
  state.py    # fold_state: 事件流 -> 可见状态
  engine.py   # intent/result 日志、重放恢复、补偿
```

### 步骤执行协议

每个活动（activity）有稳定的 `activity_id` 和由之派生的幂等键
`{workflow_id}:{activity_id}`：

1. **执行前**提交 `intent_recorded`（含 action、参数、幂等键）；
2. 通过注入的 `executor.call(action, args, idempotency_key=..., timeout=...)`
   发起远程调用——引擎自身绝不直接触碰外部服务；
3. **完成后**提交 `activity_completed`（含结果）；失败则提交
   `activity_failed` 并按 `max_retries` 重试，重试用**同一个**幂等键。

### At-least-once 执行

引擎对外部服务只保证 **at-least-once**：worker 可能在"远程调用已生效、结果
尚未写入日志"的窗口崩溃，恢复后该步骤会被**重新执行**。因此：

- 远程服务必须按幂等键去重（第一次生效、后续返回首次结果），测试中
  `FakeExecutor` 演示了这种服务行为；
- "成功响应丢失"与"调用根本没发出"对引擎不可区分，统一靠幂等键安全重试；
- 从日志的视角看效果是 exactly-once：同一 `activity_id` 第二次 `execute`
  直接返回已提交的结果，不再调用 executor。

### 恢复

`recover()` 扫描日志，对每个未终结的流程**重放工作流函数**：已提交结果的
步骤直接返回日志中的结果，只有未完成的步骤才会真正调用 executor。工作流函数
因此必须是确定性的——同样的历史必须产生同样的 `activity_id` 序列。
`ctx.parallel(...)` 的每个分支有独立的 intent/result 事件，恢复时只重放
未完成的分支。

### 超时与取消

- `workflow_timeout` 在 `workflow_started` 里固化 deadline，每个步骤边界检查；
- `request_cancel(workflow_id)` 向日志提交 `cancel_requested`，下一个步骤
  边界生效（可跨进程）；
- 两者都会终止正向流程并触发已登记的补偿。

### 补偿边界

- 补偿在步骤**成功提交结果时**登记（`compensation_registered`），按注册的
  **逆序**执行，本身也是带幂等键（`...:compensate`）的远程调用；
- 补偿是 **best-effort**：只覆盖结果已提交的步骤。崩溃窗口中"远程已生效但
  结果未提交"的步骤不在补偿清单里——这正是远程服务必须幂等的原因；
- 补偿链中断（worker 崩溃）可由 `recover()` 从日志续跑，已完成的补偿不会
  重复执行；
- **补偿失败是显式终态**：提交 `compensation_failed` 并停止后续补偿，流程
  进入 `compensation_failed` 状态，等待人工介入，引擎不会静默重试或继续。

## 使用

```python
from durable_workflow import EventLog, WorkflowEngine

engine = WorkflowEngine(EventLog("events.jsonl"), executor=my_executor)

@engine.register("order")
def order(ctx, data):
    ctx.execute("reserve-inventory", "inventory.reserve", {"sku": data["sku"]},
                compensate=("inventory.release", {"sku": data["sku"]}))
    ctx.execute("create-payment", "payment.create", {"amount": data["amount"]},
                compensate=("payment.refund", {"amount": data["amount"]}))
    ctx.execute("send-notification", "notify.send", {"to": data["to"]})
    return "ordered"

state = engine.run("order", "wf-1", input={"sku": "SKU-1", "amount": 100,
                                           "to": "user@example.com"})
```

`executor` 是唯一注入点，协议为
`call(action, args, *, idempotency_key, timeout) -> result`，远程失败抛
`RemoteError`（可安全重试），超时抛 `ActivityTimeout`。

## 测试

```
python -m unittest
```

覆盖：成功响应丢失（幂等重试）、worker 崩溃后只重放未完成步骤、重复活动
去重、并行分支独立恢复、补偿失败进入显式终态、补偿链崩溃后续跑、取消/超时
触发逆序补偿、以及所有可见状态均可从已提交日志重建。
