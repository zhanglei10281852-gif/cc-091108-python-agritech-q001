"""JSONL 事件日志：所有状态变化追加落盘，重启后按序重放恢复同一计划。

事件携带实体快照，重放为确定性的状态重建，不依赖外部数据库。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Callable, Iterable


class EventStore:
    SCHEMA_VERSION = 1

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path is not None else None
        self._lock = threading.Lock()
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: dict) -> None:
        record = {"v": self.SCHEMA_VERSION, **event}
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self._lock:
            if self.path is not None:
                with self.path.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())

    def replay(self, handler: Callable[[dict], None]) -> int:
        if self.path is None or not self.path.exists():
            return 0
        n = 0
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                handler(json.loads(line))
                n += 1
        return n

    def events(self) -> Iterable[dict]:
        if self.path is None or not self.path.exists():
            return ()
        out: list[dict] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out
