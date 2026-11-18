"""Append-only, fsynced JSONL event log.

The log is the single source of truth. Any state the engine exposes is
folded from committed events; nothing meaningful lives only in memory.
"""

from __future__ import annotations

import json
import os
import threading
import time


class EventLog:
    def __init__(self, path):
        self.path = str(path)
        self._lock = threading.Lock()
        self._events = []
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        self._events.append(json.loads(line))

    def append(self, event):
        """Commit an event durably and return the stored record."""
        with self._lock:
            record = dict(event)
            record["seq"] = len(self._events)
            record["ts"] = time.time()
            self._events.append(record)
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, sort_keys=True) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            return record

    def events(self, workflow_id=None):
        with self._lock:
            snapshot = list(self._events)
        if workflow_id is None:
            return snapshot
        return [e for e in snapshot if e.get("workflow_id") == workflow_id]

    def workflow_ids(self):
        ids = []
        for event in self.events():
            if event.get("type") == "workflow_started":
                ids.append(event["workflow_id"])
        return ids
