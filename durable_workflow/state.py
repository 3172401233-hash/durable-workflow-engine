"""Fold committed log events into externally visible workflow state."""

from __future__ import annotations

_TERMINAL = {
    "workflow_completed": "completed",
    "workflow_failed": "failed",
    "workflow_cancelled": "cancelled",
    "workflow_timed_out": "timed_out",
}


def fold_state(events):
    """Reduce an event stream (one workflow) into a state dict.

    Statuses:
      running              - started, no terminal event yet
      completed            - workflow function returned
      failed               - terminated, nothing to compensate
      cancelled/timed_out  - terminated, nothing to compensate
      compensating         - terminated, compensations still pending
      compensated          - terminated, all compensations completed
      compensation_failed  - a compensation failed; manual intervention
    """
    activities = {}
    compensations = []
    comp_by_id = {}
    terminal = None
    result = None

    for event in events:
        etype = event.get("type")
        if etype == "intent_recorded":
            activities[event["activity_id"]] = {
                "activity_id": event["activity_id"],
                "action": event["action"],
                "idempotency_key": event["idempotency_key"],
                "status": "intent",
                "attempts": 0,
                "result": None,
                "error": None,
            }
        elif etype == "activity_failed":
            activity = activities[event["activity_id"]]
            activity["attempts"] += 1
            activity["status"] = "failed"
            activity["error"] = event.get("error")
        elif etype == "activity_completed":
            activity = activities[event["activity_id"]]
            activity["status"] = "completed"
            activity["result"] = event.get("result")
        elif etype == "compensation_registered":
            comp = {
                "activity_id": event["activity_id"],
                "action": event["action"],
                "status": "registered",
                "error": None,
            }
            comp_by_id[event["activity_id"]] = comp
            compensations.append(comp)
        elif etype == "compensation_completed":
            comp_by_id[event["activity_id"]]["status"] = "completed"
        elif etype == "compensation_failed":
            comp = comp_by_id[event["activity_id"]]
            comp["status"] = "failed"
            comp["error"] = event.get("error")
        elif etype in _TERMINAL:
            terminal = _TERMINAL[etype]
            if etype == "workflow_completed":
                result = event.get("result")
            elif etype == "workflow_failed":
                result = None

    if terminal is None:
        status = "running" if events else "unknown"
        reason = None
    elif terminal == "completed":
        # Registered compensations are only relevant after a failure.
        status = "completed"
        reason = None
    elif any(c["status"] == "failed" for c in compensations):
        status = "compensation_failed"
        reason = terminal
    elif compensations and all(c["status"] == "completed" for c in compensations):
        status = "compensated"
        reason = terminal
    elif compensations:
        status = "compensating"
        reason = terminal
    else:
        status = terminal
        reason = terminal if terminal != "completed" else None

    return {
        "status": status,
        "reason": reason,
        "result": result,
        "activities": activities,
        "compensations": compensations,
    }
