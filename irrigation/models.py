"""领域模型：遥测读数、计划时段、阀门命令与执据。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from .clock import iso


class Quality(str, Enum):
    GOOD = "good"
    SUSPECT = "suspect"
    OFFLINE = "offline"


class SlotStatus(str, Enum):
    PROPOSED = "proposed"  # 预演中，可反复修订
    SCHEDULED = "scheduled"  # 已发布，等待执行
    LOCKED = "locked"  # 阀门命令已签发，水量已预占
    DONE = "done"  # 已关阀结算
    SKIPPED = "skipped"  # 决策为无需灌溉
    CANCELLED = "cancelled"  # 人工/管理员取消


class CommandState(str, Enum):
    ISSUED = "issued"  # 已签发，等待阀门 OPEN 回执
    OPEN = "open"  # 收到 OPEN 回执，阀门开启
    CLOSED = "closed"  # 收到 CLOSE 回执，已结算
    LOST = "lost"  # 超时无回执，判定失联
    CANCELLED = "cancelled"  # 发布前撤销
    MANUAL_STOP = "manual_stop"  # 人工停灌接管


# 终态：进入后命令不可再变，重复执据一律忽略
TERMINAL_STATES = frozenset(
    {CommandState.CLOSED, CommandState.LOST, CommandState.CANCELLED, CommandState.MANUAL_STOP}
)


@dataclass(frozen=True)
class Reading:
    """一条土壤遥测。业务序以 occurred_at 为准，received_at 仅用于迟到审计。"""

    event_id: str
    zone_id: str
    occurred_at: datetime
    received_at: datetime
    moisture: float | None
    quality: Quality

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "zone_id": str(self.zone_id),
            "occurred_at": iso(self.occurred_at),
            "received_at": iso(self.received_at),
            "moisture": self.moisture,
            "quality": self.quality.value,
        }


@dataclass
class Slot:
    """计划时段：一个分区在一天内的一次灌溉安排。

    时段是迟到读数影响的最小单位：只有 SCHEDULED/PROPOSED 状态可被重算，
    LOCKED/DONE 永不回改，保证已扣水量不被追溯。
    """

    slot_id: str
    plan_id: str
    zone_id: str
    start: datetime
    end: datetime
    status: SlotStatus
    planned_liters: float = 0.0
    actual_liters: float | None = None
    command_id: str | None = None
    reason: dict = field(default_factory=dict)
    # 该时段决策所依据的读数事件 id（suspect/offline 亦记录），供追溯
    based_on_event_id: str | None = None
    replaced_from: str | None = None  # 被迟到读数替换时，记录上一版决策摘要

    @property
    def locked(self) -> bool:
        return self.status in (SlotStatus.LOCKED, SlotStatus.DONE)

    @property
    def mutable(self) -> bool:
        return self.status in (SlotStatus.PROPOSED, SlotStatus.SCHEDULED)

    def to_dict(self) -> dict:
        return {
            "slot_id": self.slot_id,
            "plan_id": self.plan_id,
            "zone_id": self.zone_id,
            "start": iso(self.start),
            "end": iso(self.end),
            "status": self.status.value,
            "planned_liters": round(self.planned_liters, 3),
            "actual_liters": None if self.actual_liters is None else round(self.actual_liters, 3),
            "command_id": self.command_id,
            "reason": self.reason,
            "based_on_event_id": self.based_on_event_id,
            "replaced_from": self.replaced_from,
        }


@dataclass
class Command:
    """阀门命令。

    水量守恒约定：
    - 签发即按 intended_liters 预占当日配额；
    - 收到重复执据（同一 receipt_id）绝不产生第二次扣水；
    - 关阀按实测流量与历时结算，多预占部分释放回配额；
    - 失联命令的预占在超时时刻按额定流量封顶结算，不悬置。
    """

    command_id: str
    plan_id: str
    slot_id: str
    zone_id: str
    valve_id: str
    shared_line: str
    intended_liters: float
    issued_at: datetime
    state: CommandState = CommandState.ISSUED
    open_at: datetime | None = None
    close_at: datetime | None = None
    observed_lpm: float | None = None
    settled_liters: float | None = None  # 最终结算水量（None=尚未结算）
    receipt_ids: set[str] = field(default_factory=set)
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "command_id": self.command_id,
            "plan_id": self.plan_id,
            "slot_id": self.slot_id,
            "zone_id": self.zone_id,
            "valve_id": self.valve_id,
            "shared_line": self.shared_line,
            "intended_liters": round(self.intended_liters, 3),
            "issued_at": iso(self.issued_at),
            "state": self.state.value,
            "open_at": iso(self.open_at) if self.open_at else None,
            "close_at": iso(self.close_at) if self.close_at else None,
            "observed_lpm": self.observed_lpm,
            "settled_liters": None
            if self.settled_liters is None
            else round(self.settled_liters, 3),
            "receipt_ids": sorted(self.receipt_ids),
            "note": self.note,
        }
