import os
import tempfile
import time
import unittest

from durable_workflow import EventLog, RemoteError, WorkflowEngine


class WorkerCrash(BaseException):
    """Simulates the worker process dying mid-call (not a remote error)."""


class FakeExecutor:
    """In-memory remote service with an idempotency-key store.

    Side effects are applied at most once per idempotency key, mimicking a
    well-behaved remote service. Scripts let tests inject lost responses,
    crashes and failures.
    """

    def __init__(self):
        self.calls = []               # (action, idempotency_key)
        self.effects = []             # actions that actually mutated "remote" state
        self.store = {}               # idempotency_key -> result
        self.lose_response_once = set()
        self.crash_once = set()
        self.fail_always = {}
        self.sleep_for = {}
        self.on_call = None           # hook(action) after success

    def call(self, action, args, *, idempotency_key, timeout=None):
        self.calls.append((action, idempotency_key))
        if idempotency_key in self.store:
            return self.store[idempotency_key]
        if action in self.crash_once:
            self.crash_once.discard(action)
            raise WorkerCrash(f"worker died during {action}")
        if action in self.fail_always:
            raise self.fail_always[action]
        if action in self.sleep_for:
            time.sleep(self.sleep_for[action])
        result = {"action": action, "args": args}
        self.effects.append(action)
        self.store[idempotency_key] = result
        if action in self.lose_response_once:
            self.lose_response_once.discard(action)
            raise RemoteError("response lost after commit")
        if self.on_call is not None:
            self.on_call(action)
        return result

    def count(self, action):
        return sum(1 for a, _ in self.calls if a == action)

    def effect_count(self, action):
        return self.effects.count(action)


def order_workflow(ctx, data):
    ctx.execute("reserve-inventory", "inventory.reserve", {"sku": data["sku"]},
                compensate=("inventory.release", {"sku": data["sku"]}))
    ctx.execute("create-payment", "payment.create", {"amount": data["amount"]},
                compensate=("payment.refund", {"amount": data["amount"]}))
    ctx.execute("send-notification", "notify.send", {"to": data["to"]})
    return "ordered"


ORDER_INPUT = {"sku": "SKU-1", "amount": 100, "to": "user@example.com"}


class WorkflowTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.log_path = os.path.join(self.tmp.name, "events.jsonl")
        self.executor = FakeExecutor()
        self.engine = self._new_engine()

    def tearDown(self):
        self.tmp.cleanup()

    def _new_engine(self, executor=None, workflow=order_workflow, **kwargs):
        engine = WorkflowEngine(EventLog(self.log_path),
                                executor or self.executor, **kwargs)
        engine.register("order")(workflow)
        return engine

    def run_order(self, workflow_id="wf-1", **kwargs):
        return self.engine.run("order", workflow_id, input=dict(ORDER_INPUT), **kwargs)


class TestLostResponse(WorkflowTestCase):
    def test_success_response_lost_retries_with_same_idempotency_key(self):
        self.executor.lose_response_once.add("payment.create")

        def order_with_retry(ctx, data):
            ctx.execute("reserve-inventory", "inventory.reserve", {},
                        compensate=("inventory.release", {}))
            ctx.execute("create-payment", "payment.create", {}, max_retries=1,
                        compensate=("payment.refund", {}))
            return "ok"

        self.engine.register("order")(order_with_retry)
        state = self.run_order()

        self.assertEqual(state["status"], "completed")
        # Remote effect applied exactly once despite the retry.
        self.assertEqual(self.executor.effect_count("payment.create"), 1)
        # Both attempts used the same stable idempotency key.
        keys = [k for a, k in self.executor.calls if a == "payment.create"]
        self.assertEqual(len(keys), 2)
        self.assertEqual(keys[0], keys[1])
        self.assertEqual(keys[0], "wf-1:create-payment")


class TestWorkerCrash(WorkflowTestCase):
    def test_crash_replays_only_incomplete_steps(self):
        self.executor.crash_once.add("payment.create")

        with self.assertRaises(WorkerCrash):
            self.run_order()

        # A brand-new engine over the same committed log recovers.
        recovered = self._new_engine()
        self.assertEqual(recovered.recover(), ["wf-1"])
        state = recovered.get_state("wf-1")

        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["result"], "ordered")
        # Committed step not re-executed; crashed step retried with same key.
        self.assertEqual(self.executor.count("inventory.reserve"), 1)
        self.assertEqual(self.executor.count("payment.create"), 2)
        self.assertEqual(self.executor.count("notify.send"), 1)
        keys = [k for a, k in self.executor.calls if a == "payment.create"]
        self.assertEqual(keys[0], keys[1])
        # No duplicate remote side effects.
        self.assertEqual(self.executor.effect_count("inventory.reserve"), 1)
        self.assertEqual(self.executor.effect_count("payment.create"), 1)


class TestDuplicateActivity(WorkflowTestCase):
    def test_duplicate_activity_id_returns_committed_result(self):
        def duplicate(ctx, data):
            first = ctx.execute("reserve-inventory", "inventory.reserve", {})
            second = ctx.execute("reserve-inventory", "inventory.reserve", {})
            return first, second

        self.engine.register("order")(duplicate)
        state = self.run_order()

        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["result"][0], state["result"][1])
        self.assertEqual(self.executor.count("inventory.reserve"), 1)


class TestParallelBranches(WorkflowTestCase):
    def test_parallel_branches_recover_independently(self):
        def fan_out(ctx, data):
            branch_a, branch_b = ctx.parallel(
                {"activity_id": "branch-a", "action": "branch.a",
                 "compensate": ("undo.a", {})},
                {"activity_id": "branch-b", "action": "branch.b",
                 "compensate": ("undo.b", {})},
            )
            ctx.execute("join", "join", {})
            return [branch_a, branch_b]

        self.engine.register("order")(fan_out)
        self.executor.crash_once.add("join")

        with self.assertRaises(WorkerCrash):
            self.run_order()

        recovered = self._new_engine(workflow=fan_out)
        recovered.recover()
        state = recovered.get_state("wf-1")

        self.assertEqual(state["status"], "completed")
        self.assertEqual(
            set(state["activities"]),
            {"branch-a", "branch-b", "join"},
        )
        # Both branches committed before the crash: never re-executed.
        self.assertEqual(self.executor.count("branch.a"), 1)
        self.assertEqual(self.executor.count("branch.b"), 1)
        self.assertEqual(self.executor.count("join"), 2)


class TestCompensation(WorkflowTestCase):
    def test_compensation_failure_is_committed_and_visible(self):
        self.executor.fail_always["payment.create"] = RemoteError("card declined")
        self.executor.fail_always["inventory.release"] = RemoteError("inventory down")

        state = self.run_order()

        self.assertEqual(state["status"], "compensation_failed")
        self.assertEqual(state["reason"], "failed")
        self.assertEqual(state["compensations"][0]["activity_id"], "reserve-inventory")
        self.assertEqual(state["compensations"][0]["status"], "failed")
        # State comes from the committed log: a fresh engine sees the same.
        fresh = self._new_engine()
        self.assertEqual(fresh.get_state("wf-1")["status"], "compensation_failed")
        types = [e["type"] for e in fresh.log.events("wf-1")]
        self.assertIn("compensation_failed", types)

    def test_recovery_resumes_compensation_after_crash(self):
        self.executor.fail_always["payment.create"] = RemoteError("card declined")
        self.executor.crash_once.add("inventory.release")

        with self.assertRaises(WorkerCrash):
            self.run_order()

        recovered = self._new_engine()
        self.assertEqual(recovered.recover(), ["wf-1"])
        state = recovered.get_state("wf-1")

        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["reason"], "failed")
        # Compensation ran exactly once with effect despite the crash.
        self.assertEqual(self.executor.effect_count("inventory.release"), 1)
        self.assertEqual(self.executor.count("inventory.release"), 2)

    def test_compensations_run_in_reverse_order(self):
        self.executor.fail_always["notify.send"] = RemoteError("smtp down")

        state = self.run_order()

        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["reason"], "failed")
        comp_effects = [a for a in self.executor.effects
                        if a in ("inventory.release", "payment.refund")]
        self.assertEqual(comp_effects, ["payment.refund", "inventory.release"])


class TestCancelAndTimeout(WorkflowTestCase):
    def test_cancel_triggers_compensation(self):
        engine = self.engine

        def cancel_after_reserve(action):
            if action == "inventory.reserve":
                engine.request_cancel("wf-1")

        self.executor.on_call = cancel_after_reserve
        state = self.run_order()

        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["reason"], "cancelled")
        # Payment never started; only the inventory compensation ran.
        self.assertEqual(self.executor.count("payment.create"), 0)
        self.assertEqual(self.executor.effect_count("inventory.release"), 1)

    def test_timeout_triggers_compensation(self):
        self.executor.sleep_for["inventory.reserve"] = 0.1
        state = self.run_order(workflow_timeout=0.02)

        self.assertEqual(state["status"], "compensated")
        self.assertEqual(state["reason"], "timed_out")
        self.assertEqual(self.executor.count("payment.create"), 0)
        self.assertEqual(self.executor.effect_count("inventory.release"), 1)


class TestStateFromLog(WorkflowTestCase):
    def test_all_visible_state_is_folded_from_committed_log(self):
        state = self.run_order()
        self.assertEqual(state["status"], "completed")

        # A new engine with no in-memory history reconstructs everything.
        fresh = WorkflowEngine(EventLog(self.log_path), FakeExecutor())
        state = fresh.get_state("wf-1")

        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["result"], "ordered")
        self.assertEqual(
            {aid: a["status"] for aid, a in state["activities"].items()},
            {"reserve-inventory": "completed",
             "create-payment": "completed",
             "send-notification": "completed"},
        )
        self.assertEqual(
            state["activities"]["create-payment"]["idempotency_key"],
            "wf-1:create-payment",
        )
        self.assertEqual(
            [c["activity_id"] for c in state["compensations"]],
            ["reserve-inventory", "create-payment"],
        )


if __name__ == "__main__":
    unittest.main()
