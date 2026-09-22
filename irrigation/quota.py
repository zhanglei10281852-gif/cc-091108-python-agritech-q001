"""跨午夜配额台账。

守恒规则：
- 配额分桶按园区本地日期；命令“签发时刻”落在哪一天，就占用哪一天的额度。
  命令即使持续到午夜之后，结算仍回到开阀当日的桶（取水许可按开阀日授予）。
- 签发即按计划量预占（reserve），因此“已开/待确认”的命令也计入已用，
  绝不会因为回执未到而把同一额度发给第二条命令。
- 关阀/失联/人工停灌后按实结算（settle），预占多出的部分当日释放。
- reserve/settle 对同一 command_id 幂等：回执重复不会二次扣水。
"""

from __future__ import annotations

from datetime import date as date_cls
from zoneinfo import ZoneInfo

from .clock import local_date, now as clock_now
from .config import Domain


class QuotaExceededError(Exception):
    def __init__(self, day: date_cls, requested: float, remaining: float):
        self.day = day
        self.requested = requested
        self.remaining = remaining
        super().__init__(
            f"日配额不足: {day} 请求 {requested:.1f}L，剩余 {remaining:.1f}L"
        )


class _DayLedger:
    __slots__ = ("reservations", "settled")

    def __init__(self) -> None:
        self.reservations: dict[str, float] = {}
        self.settled: dict[str, float] = {}

    def reserve(self, command_id: str, liters: float) -> bool:
        """返回是否为新预占（False=该命令已预占或已结算，保持原值）。"""
        if command_id in self.reservations or command_id in self.settled:
            return False
        self.reservations[command_id] = float(liters)
        return True

    def settle(self, command_id: str, liters: float) -> float:
        """结算命令；返回对当日已用量的净变化（负=释放）。幂等。"""
        if command_id in self.settled:
            return 0.0
        prior = self.reservations.pop(command_id, 0.0)
        self.settled[command_id] = float(liters)
        return liters - prior

    def used(self) -> float:
        return sum(self.settled.values()) + sum(self.reservations.values())


class QuotaLedger:
    def __init__(self, domain: Domain):
        self.domain = domain
        self.tz = ZoneInfo(domain.timezone)
        self._days: dict[date_cls, _DayLedger] = {}
        # 命令 -> 归属日，保证跨午夜结算落回开阀当日
        self._cmd_day: dict[str, date_cls] = {}

    def _day(self, day: date_cls) -> _DayLedger:
        return self._days.setdefault(day, _DayLedger())

    def day_of(self, moment) -> date_cls:
        return local_date(moment, self.tz)

    def today(self) -> date_cls:
        return local_date(clock_now(self.tz), self.tz)

    def reserve(self, moment, command_id: str, liters: float) -> date_cls:
        day = self.day_of(moment)
        ledger = self._day(day)
        is_new = command_id not in self._cmd_day
        if is_new:
            if liters - self.remaining(day) > 1e-6:
                raise QuotaExceededError(day, liters, self.remaining(day))
            self._cmd_day[command_id] = day
        ledger.reserve(command_id, liters)
        return day

    def settle(self, command_id: str, actual_liters: float) -> float:
        day = self._cmd_day.get(command_id)
        if day is None:
            raise KeyError(f"命令尚未预占，无法结算: {command_id}")
        return self._day(day).settle(command_id, max(0.0, float(actual_liters)))

    def is_settled(self, command_id: str) -> bool:
        day = self._cmd_day.get(command_id)
        return day is not None and command_id in self._day(day).settled

    def used(self, day: date_cls | None = None) -> float:
        day = day or self.today()
        return self._days.get(day, _DayLedger()).used()

    def limit(self) -> float:
        return self.domain.daily_water_limit_liters

    def remaining(self, day: date_cls | None = None) -> float:
        day = day or self.today()
        return self.domain.daily_water_limit_liters - self.used(day)

    def summary(self, day: date_cls | None = None) -> dict:
        day = day or self.today()
        ledger = self._days.get(day, _DayLedger())
        settled = sum(ledger.settled.values())
        reserved = sum(ledger.reservations.values())
        used = settled + reserved
        return {
            "date": day.isoformat(),
            "limit_liters": self.domain.daily_water_limit_liters,
            "settled_liters": round(settled, 3),
            "reserved_liters": round(reserved, 3),
            "used_liters": round(used, 3),
            "remaining_liters": round(self.domain.daily_water_limit_liters - used, 3),
            "pending_command_ids": sorted(ledger.reservations),
            "settled_command_ids": sorted(ledger.settled),
        }
