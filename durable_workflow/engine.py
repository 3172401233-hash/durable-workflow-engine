"""Workflow engine: intent/result journaling, replay recovery, compensation."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from .state import fold_state


class RemoteError(Exception):
    """A remote call failed (or its response was lost). Safe to retry."""


class ActivityTimeout(RemoteError):
    """The executor reports that a remote call exceeded its timeout."""


class ActivityFailedError(Exception):
    """An activity exhausted its retries; triggers workflow compensation."""

    def __init__(self, activity_id, error):
        super().__init__(f"activity {activity_id!r} failed: {error}")
        self.activity_id = activity_id


class WorkflowCancelled(Exception):
    """Raised inside a replay when a cancel request is found in the log."""


class WorkflowTimedOut(Exception):
    """Raised inside a replay when the workflow deadline has passed."""


class WorkflowContext:
    """Deterministic, replayable handle passed to workflow functions."""

    def __init__(self, engine, workflow_id, deadline):
        self._engine = engine
        self.workflow_id = workflow_id
        self._deadline = deadline

    def _events(self):
        return self._engine.log.events(self.workflow_id)

    def _check_interrupt(self):
        events = self._events()
        if any(e["type"] == "cancel_requested" for e in events):
            raise WorkflowCancelled()
        if self._deadline is not None and time.time() > self._deadline:
            raise WorkflowTimedOut()

    def execute(self, activity_id, action, args=None, *, compensate=None,
                timeout=None, max_retries=0):
        """Run one activity exactly-once from the log's point of view.

        The remote side may see the call more than once (at-least-once);
        the stable idempotency key lets it deduplicate.
        """
        log = self._engine.log
        events = self._events()

        for event in events:
            if (event["type"] == "activity_completed"
                    and event["activity_id"] == activity_id):
                return event["result"]

        self._check_interrupt()

        idempotency_key = f"{self.workflow_id}:{activity_id}"
        if not any(e["type"] == "intent_recorded"
                   and e["activity_id"] == activity_id for e in events):
            log.append({
                "type": "intent_recorded",
                "workflow_id": self.workflow_id,
                "activity_id": activity_id,
                "action": action,
                "args": args,
                "idempotency_key": idempotency_key,
            })

        attempts = sum(1 for e in events
                       if e["type"] == "activity_failed"
                       and e["activity_id"] == activity_id)
        while True:
            try:
                result = self._engine.executor.call(
                    action, args,
                    idempotency_key=idempotency_key,
                    timeout=timeout,
                )
            except RemoteError as exc:
                attempts += 1
                log.append({
                    "type": "activity_failed",
                    "workflow_id": self.workflow_id,
                    "activity_id": activity_id,
                    "attempt": attempts,
                    "error": str(exc),
                })
                if attempts <= max_retries:
                    continue
                raise ActivityFailedError(activity_id, str(exc)) from exc
            log.append({
                "type": "activity_completed",
                "workflow_id": self.workflow_id,
                "activity_id": activity_id,
                "result": result,
            })
            if compensate is not None:
                comp_action, comp_args = compensate
                log.append({
                    "type": "compensation_registered",
                    "workflow_id": self.workflow_id,
                    "activity_id": activity_id,
                    "action": comp_action,
                    "args": comp_args,
                })
            return result

    def parallel(self, *activities):
        """Execute independent activities concurrently.

        Each entry is a dict with keys: activity_id, action, args (optional),
        plus the optional keyword arguments of execute(). Each branch gets
        its own intent/result events, so recovery replays only the branches
        that never committed a result.
        """
        results = [None] * len(activities)
        with ThreadPoolExecutor(max_workers=max(1, len(activities))) as pool:
            futures = {}
            for index, spec in enumerate(activities):
                future = pool.submit(
                    self.execute,
                    spec["activity_id"],
                    spec["action"],
                    spec.get("args"),
                    compensate=spec.get("compensate"),
                    timeout=spec.get("timeout"),
                    max_retries=spec.get("max_retries", 0),
                )
                futures[future] = index
            for future, index in futures.items():
                results[index] = future.result()
        return results


class WorkflowEngine:
    def __init__(self, log, executor, workflow_timeout=None):
        self.log = log
        self.executor = executor
        self.default_workflow_timeout = workflow_timeout
        self._workflows = {}

    def register(self, name=None):
        """Decorator: register a workflow function under a stable name."""
        def decorator(fn):
            self._workflows[name or fn.__name__] = fn
            return fn
        return decorator

    def run(self, name, workflow_id, input=None, workflow_timeout=None):
        """Run (or replay) a workflow to a terminal state; return its state."""
        fn = self._workflows[name]
        events = self.log.events(workflow_id)
        started = next((e for e in events if e["type"] == "workflow_started"), None)
        if started is None:
            timeout = (workflow_timeout if workflow_timeout is not None
                       else self.default_workflow_timeout)
            deadline = time.time() + timeout if timeout is not None else None
            self.log.append({
                "type": "workflow_started",
                "workflow_id": workflow_id,
                "workflow": name,
                "input": input,
                "deadline": deadline,
            })
        else:
            input = started.get("input")
            deadline = started.get("deadline")

        ctx = WorkflowContext(self, workflow_id, deadline)
        try:
            result = fn(ctx, input)
        except WorkflowCancelled:
            self.log.append({"type": "workflow_cancelled", "workflow_id": workflow_id})
            self._compensate(workflow_id)
        except WorkflowTimedOut:
            self.log.append({"type": "workflow_timed_out", "workflow_id": workflow_id})
            self._compensate(workflow_id)
        except ActivityFailedError as exc:
            self.log.append({
                "type": "workflow_failed",
                "workflow_id": workflow_id,
                "error": str(exc),
            })
            self._compensate(workflow_id)
        except Exception as exc:
            # A bug in workflow code fails the workflow; process crashes
            # (BaseException) propagate and are handled by recover().
            self.log.append({
                "type": "workflow_failed",
                "workflow_id": workflow_id,
                "error": f"{type(exc).__name__}: {exc}",
            })
            self._compensate(workflow_id)
        else:
            self.log.append({
                "type": "workflow_completed",
                "workflow_id": workflow_id,
                "result": result,
            })
        return self.get_state(workflow_id)

    def recover(self):
        """Resume every workflow whose committed log is not terminal.

        Re-runs the workflow function; already-committed activities are
        replayed from the log, only incomplete steps call the executor.
        Also resumes compensation chains interrupted by a crash.
        """
        resumed = []
        for workflow_id in self.log.workflow_ids():
            events = self.log.events(workflow_id)
            state = fold_state(events)
            if state["status"] == "running":
                started = next(e for e in events if e["type"] == "workflow_started")
                self.run(started["workflow"], workflow_id)
                resumed.append(workflow_id)
            elif state["status"] == "compensating":
                self._compensate(workflow_id)
                resumed.append(workflow_id)
        return resumed

    def request_cancel(self, workflow_id):
        """Commit a cancel request; honored at the next step boundary."""
        self.log.append({"type": "cancel_requested", "workflow_id": workflow_id})

    def get_state(self, workflow_id):
        """All visible state is folded from the committed log."""
        return fold_state(self.log.events(workflow_id))

    def _compensate(self, workflow_id):
        """Run registered compensations in reverse order, best effort.

        Each compensation is itself journaled, so a crash mid-chain resumes
        where it stopped. A failed compensation is committed and stops the
        chain: the workflow is left in `compensation_failed` for operators.
        """
        events = self.log.events(workflow_id)
        registered = [e for e in events if e["type"] == "compensation_registered"]
        done = {e["activity_id"] for e in events
                if e["type"] == "compensation_completed"}
        failed = {e["activity_id"] for e in events
                  if e["type"] == "compensation_failed"}
        for registration in reversed(registered):
            activity_id = registration["activity_id"]
            if activity_id in done or activity_id in failed:
                continue
            try:
                result = self.executor.call(
                    registration["action"],
                    registration.get("args"),
                    idempotency_key=f"{workflow_id}:{activity_id}:compensate",
                    timeout=None,
                )
            except RemoteError as exc:
                self.log.append({
                    "type": "compensation_failed",
                    "workflow_id": workflow_id,
                    "activity_id": activity_id,
                    "error": str(exc),
                })
                return
            self.log.append({
                "type": "compensation_completed",
                "workflow_id": workflow_id,
                "activity_id": activity_id,
                "result": result,
            })
