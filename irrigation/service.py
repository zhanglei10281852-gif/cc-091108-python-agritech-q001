"""灌溉决策服务。

一条典型时间线：
    generate_plan(预演) -> update_thresholds(农艺师修订) -> preview/用水预演
    -> publish_plan(发布) -> (迟到读数) revise_plan 只改未锁定时段
    -> dispatch_due 签发命令（预占配额、管线串行门控）
    -> record_receipt 开/关阀回执（幂等去重，按实结算）
    -> manual_stop 人工立即压过自动；reconcile 处理失联
所有变化写 JSONL 事件日志，重启 replay 后从同一计划继续。
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import date as date_cls
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .clock import combine_local, local_date, minutes_between, now as clock_now, parse
from .config import Domain, ZoneThresholds
from .et import EtEstimator, ZoneContext
from .models import (
    TERMINAL_STATES,
    Command,
    CommandState,
    Quality,
    Reading,
    Slot,
    SlotStatus,
)
from .quota import QuotaExceededError, QuotaLedger
from .store import EventStore

_PLANNED_LIKE = (SlotStatus.PROPOSED, SlotStatus.SCHEDULED, SlotStatus.LOCKED)


@dataclass
class Receipt:
    """阀门回执。receipt_id 由网关/执据全局唯一，重复执据据此判重。"""

    receipt_id: str
    command_id: str
    kind: str  # open / close
    at: datetime
    observed_lpm: float | None = None


@dataclass
class Plan:
    plan_id: str
    day: date_cls
    status: str  # proposed / published
    created_at: datetime
    slot_ids: list[str] = field(default_factory=list)
    et0: dict[str, float] = field(default_factory=dict)


def _reading_from_dict(d: dict) -> Reading:
    return Reading(
        event_id=d["event_id"],
        zone_id=d["zone_id"],
        occurred_at=parse(d["occurred_at"]),
        received_at=parse(d["received_at"]),
        moisture=d.get("moisture"),
        quality=Quality(d["quality"]),
    )


class IrrigationService:
    def __init__(
        self,
        domain: Domain,
        store: EventStore | None = None,
        *,
        slot_minutes: int = 30,
        ack_timeout_minutes: int = 10,
        sensor_freshness_min: float = 240.0,
        candidate_starts: tuple[str, ...] = ("07:00", "11:00", "15:00"),
        window_end: str = "20:00",
        clock=None,
    ):
        self.domain = domain
        self.store = store or EventStore(None)
        self.tz = ZoneInfo(domain.timezone)
        self.slot_minutes = slot_minutes
        self.ack_timeout = timedelta(minutes=ack_timeout_minutes)
        self.freshness = timedelta(minutes=sensor_freshness_min)
        self.candidate_starts = candidate_starts
        self.window_end = window_end
        self._clock = clock or (lambda: clock_now(self.tz))

        self.ledger = QuotaLedger(domain)
        self.et_eng = EtEstimator(domain)

        self.plans: dict[str, Plan] = {}
        self.slots: dict[str, Slot] = {}
        self.commands: dict[str, Command] = {}
        self.readings: dict[str, list[Reading]] = {z.id: [] for z in domain.zones}
        self._reading_ids: set[str] = set()
        self._receipt_ids: set[str] = set()
        self._threshold_overrides: dict[str, dict[str, ZoneThresholds]] = {}
        self._et0: dict[tuple[str, date_cls], dict[str, float]] = {}
        self._valve_holds: set[str] = set()
        self._lock = threading.RLock()
        self._replayed = self._replay()

    # ------------------------------------------------------------------ clock

    def now(self) -> datetime:
        return self._clock()

    # ---------------------------------------------------------------- ingest

    def ingest_reading(self, data: dict) -> Reading:
        """收入一条遥测。乱序安全：内部按 occurred_at 排序。

        新读数只影响尚未锁定的时段——已 LOCKED/DONE 的时段不回改，
        调用方随后应 revise_plan() 让未执行时段吸收这条读数。
        """
        with self._lock:
            r = _reading_from_dict(data)
            return self._ingest(r)

    def ingest_readings(self, items: list[dict]) -> list[Reading]:
        with self._lock:
            return [self._ingest(_reading_from_dict(d)) for d in items]

    def _ingest(self, r: Reading) -> Reading:
        if r.event_id in self._reading_ids:
            return self._find_reading(r.zone_id, r.event_id)
        if r.zone_id not in self.readings:
            self.readings[r.zone_id] = []
        self.readings[r.zone_id].append(r)
        self.readings[r.zone_id].sort(key=lambda x: x.occurred_at)
        self._reading_ids.add(r.event_id)
        self._emit("reading_ingested", {"reading": r.to_dict()})
        return r

    def _find_reading(self, zone_id: str, event_id: str) -> Reading:
        for r in self.readings.get(zone_id, []):
            if r.event_id == event_id:
                return r
        raise KeyError(event_id)

    def latest_reading(self, zone_id: str, before: datetime) -> Reading | None:
        """取业务时间严格早于 before 的最新一条（不看接收顺序）。"""
        cand = [r for r in self.readings.get(zone_id, []) if r.occurred_at < before]
        return cand[-1] if cand else None

    def set_et0(self, day: date_cls, values: float | dict[str, float]) -> None:
        with self._lock:
            zmap = {z.id: float(values)} if isinstance(values, (int, float)) else values
            key = ("default", day)
            self._et0[key] = dict(zmap)
            self._emit("et0_set", {"date": day.isoformat(), "et0": zmap})

    def _et0_for(self, plan: Plan, zone_id: str) -> float | None:
        return plan.et0.get(zone_id) or self._et0.get(("default", plan.day), {}).get(zone_id)

    # ------------------------------------------------------------- thresholds

    def update_thresholds(
        self, plan_id: str, overrides: dict[str, dict]
    ) -> dict[str, ZoneThresholds]:
        """农艺师发布前/后修订阈值。只对尚未锁定的时段生效。"""
        with self._lock:
            plan = self._plan(plan_id)
            merged = dict(self._threshold_overrides.get(plan_id, {}))
            for zone_id, vals in overrides.items():
                base = self.domain.thresholds_for(zone_id)
                merged[zone_id] = ZoneThresholds(**{**base.__dict__, **vals})
            self._threshold_overrides[plan_id] = merged
            self._emit(
                "thresholds_updated",
                {"plan_id": plan_id, "overrides": {
                    z: t.__dict__ for z, t in merged.items()
                }},
            )
            if plan.status == "proposed":
                self._recompute_plan(plan)
            return merged

    def _thr(self, plan: Plan, zone_id: str) -> ZoneThresholds:
        return self._threshold_overrides.get(plan.plan_id, {}).get(
            zone_id, self.domain.thresholds_for(zone_id)
        )

    # --------------------------------------------------------- plan build/run

    def generate_plan(
        self,
        day: date_cls,
        et0: float | dict[str, float] | None = None,
        *,
        replace: bool = False,
    ) -> Plan:
        """为某本地日期生成 PROPOSED 计划（预演态，不触碰配额）。"""
        with self._lock:
            plan_id = f"PLAN-{day.isoformat()}"
            existing = self.plans.get(plan_id)
            if existing is not None:
                if existing.status == "published" or not replace:
                    raise ValueError(f"计划已存在: {plan_id}")
                for sid in existing.slot_ids:
                    s = self.slots[sid]
                    if not s.mutable:
                        raise ValueError("旧计划存在锁定时段，无法替换")
                removed = list(existing.slot_ids)
                for sid in removed:
                    self.slots.pop(sid, None)
                self._emit("plan_replaced", {"plan_id": plan_id, "removed_slot_ids": removed})
            plan = Plan(
                plan_id=plan_id,
                day=day,
                status="proposed",
                created_at=self.now(),
                et0=(
                    {z.id: float(et0) for z in self.domain.zones}
                    if isinstance(et0, (int, float))
                    else dict(et0 or {})
                ),
            )
            self.plans[plan_id] = plan
            self._emit("plan_created", {"plan": self._plan_dict(plan)})
            self._recompute_plan(plan)
            return plan

    def publish_plan(self, plan_id: str) -> Plan:
        with self._lock:
            plan = self._plan(plan_id)
            if plan.status != "proposed":
                raise ValueError(f"计划状态不可发布: {plan.status}")
            plan.status = "published"
            for sid in plan.slot_ids:
                slot = self.slots[sid]
                if slot.status == SlotStatus.PROPOSED:
                    slot.status = SlotStatus.SCHEDULED if slot.planned_liters > 0 else SlotStatus.SKIPPED
                    self._emit("slot_upserted", {"slot": slot.to_dict()})
            self._emit("plan_published", {"plan_id": plan_id, "at": self.now().isoformat()})
            return plan

    def revise_plan(self, plan_id: str) -> dict:
        """吸收新读数/新阈值，重算未锁定时段；锁定时段原样保留。"""
        with self._lock:
            plan = self._plan(plan_id)
            return self._recompute_plan(plan)

    def preview(
        self,
        day: date_cls,
        et0: float | dict[str, float] | None = None,
        overrides: dict[str, dict] | None = None,
    ) -> dict:
        """用水预演：不落任何状态，返回各时段与按日汇总的计划用水量。"""
        with self._lock:
            plan_id = f"PREVIEW-{day.isoformat()}-{uuid.uuid4().hex[:6]}"
            tmp = Plan(plan_id=plan_id, day=day, status="proposed",
                       created_at=self.now(),
                       et0=({z.id: float(et0) for z in self.domain.zones}
                            if isinstance(et0, (int, float)) else dict(et0 or {})))
            if overrides:
                self._threshold_overrides[plan_id] = {
                    z: ZoneThresholds(**{**self.domain.thresholds_for(z).__dict__, **v})
                    for z, v in overrides.items()
                }
            slots = self._build_slots(tmp)
            by_zone: dict[str, float] = {}
            for s in slots:
                by_zone[s.zone_id] = round(by_zone.get(s.zone_id, 0.0) + s.planned_liters, 3)
            total = round(sum(by_zone.values()), 3)
            self._threshold_overrides.pop(plan_id, None)
            return {
                "date": day.isoformat(),
                "slots": [s.to_dict() for s in slots],
                "liters_by_zone": by_zone,
                "total_liters": total,
                "daily_limit_liters": self.domain.daily_water_limit_liters,
                "within_quota": total <= self.domain.daily_water_limit_liters + 1e-6,
                "remaining_after_preview": round(
                    self.domain.daily_water_limit_liters - total, 3
                ),
            }

    # ----------------------------------------------------------- slot engine

    def _recompute_plan(self, plan: Plan) -> dict:
        """重算未锁定时段。时段 id 由“计划:分区:候选时刻”确定，按 id 对位替换。"""
        kept = [sid for sid in plan.slot_ids if not self.slots[sid].mutable]
        old_mut = {sid: self.slots[sid] for sid in plan.slot_ids if self.slots[sid].mutable}
        built = {s.slot_id: s for s in self._build_slots(plan, locked_ids=set(kept))}

        changed, cancelled = [], []
        for sid, old in old_mut.items():
            new = built.pop(sid, None)
            if new is None:
                old.status = SlotStatus.CANCELLED
                old.reason = {**old.reason, "revision": "迟到读数/阈值修订后该时段不再需要灌溉"}
                cancelled.append(old.to_dict())
                self._emit("slot_upserted", {"slot": old.to_dict()})
                continue
            moved = new.start != old.start or new.end != old.end
            old_planned = old.planned_liters
            revised = abs(new.planned_liters - old_planned) > 1e-6 or moved
            if revised:
                old.replaced_from = (
                    f"{old_planned:.1f}L -> {new.planned_liters:.1f}L"
                    if not moved
                    else f"rescheduled {old.start.isoformat()} -> {new.start.isoformat()}"
                )
                changed.append(old.to_dict())
            old.start, old.end = new.start, new.end
            old.planned_liters = new.planned_liters
            old.based_on_event_id = new.based_on_event_id
            old.reason = new.reason
            if plan.status == "published":
                old.status = (
                    SlotStatus.SCHEDULED if old.planned_liters > 0 else SlotStatus.SKIPPED
                )
            self._emit("slot_upserted", {"slot": old.to_dict()})

        for new in built.values():
            new.plan_id = plan.plan_id
            new.status = (
                SlotStatus.SCHEDULED
                if plan.status == "published" and new.planned_liters > 0
                else SlotStatus.SKIPPED if plan.status == "published"
                else SlotStatus.PROPOSED
            )
            self.slots[new.slot_id] = new
            plan.slot_ids.append(new.slot_id)
            changed.append(new.to_dict())
            self._emit("slot_upserted", {"slot": new.to_dict()})

        return {
            "plan_id": plan.plan_id,
            "locked_preserved": len(kept),
            "changed": changed,
            "cancelled": cancelled,
        }

    def _build_slots(self, plan: Plan, locked_ids: set[str] | None = None) -> list[Slot]:
        """按候选时刻生成时段；同管线串行排产，异管线允许时间重叠（并发）。"""
        locked_ids = locked_ids or set()
        locked = [self.slots[sid] for sid in locked_ids]
        # 管线游标：已锁定的同管线时段必须避让
        line_cursor: dict[str, datetime] = {}
        for s in locked:
            valve = self.domain.valve_of(s.zone_id)
            prev = line_cursor.get(valve.shared_line)
            if prev is None or s.end > prev:
                line_cursor[valve.shared_line] = s.end

        requests: list[tuple[datetime, str]] = []
        for hhmm in self.candidate_starts:
            for z in self.domain.zones:
                requests.append((combine_local(plan.day, hhmm, self.tz), z.id))
        requests.sort(key=lambda x: (x[0], x[1]))

        window_end = combine_local(plan.day, self.window_end, self.tz)
        out: list[Slot] = []
        allocated: dict[str, float] = {z.id: 0.0 for z in self.domain.zones}
        for s in locked:  # 锁定水量先计入当日已分配
            v = s.actual_liters if s.actual_liters is not None else s.planned_liters
            allocated[s.zone_id] += v / self.domain.area_of(s.zone_id)

        for desired, zone_id in requests:
            zone = self.domain.zone(zone_id)
            valve = self.domain.valve_of(zone_id)
            start = max(desired, line_cursor.get(valve.shared_line, desired))
            reading = self.latest_reading(zone_id, start)
            ctx = ZoneContext(
                latest_reading=reading,
                et0_mm=self._et0_for(plan, zone_id),
                already_allocated_mm=allocated[zone_id],
            )
            dec = self.et_eng.decide(
                zone, ctx, at=start, lpm=valve.rated_lpm,
                thresholds=self._thr(plan, zone_id),
                freshness_min=self.freshness.total_seconds() / 60.0,
            )
            if not dec.irrigate:
                continue
            duration = timedelta(minutes=dec.duration_min)
            end = start + duration
            if end > window_end:
                continue
            sid = f"{plan.plan_id}:{zone_id}:{desired.strftime('%H%M')}"
            slot = Slot(
                slot_id=sid, plan_id=plan.plan_id, zone_id=zone_id,
                start=start, end=end, status=SlotStatus.PROPOSED,
                planned_liters=dec.liters, reason=dec.reason,
                based_on_event_id=reading.event_id if reading else None,
            )
            out.append(slot)
            allocated[zone_id] += dec.liters / self.domain.area_of(zone_id)
            line_cursor[valve.shared_line] = end
        return out

    # ------------------------------------------------------------- execution

    def dispatch_due(self, at: datetime | None = None) -> dict:
        """控制器节拍：把到时的 SCHEDULED 时段签发为阀门命令。

        受管线并发门控与配额约束；暂时开不了的时段留待下一节拍，
        错过结束时刻仍无配额/管线的时段标记 SKIPPED。
        """
        with self._lock:
            at = at or self.now()
            issued, blocked, skipped = [], [], []
            due = sorted(
                (s for s in self.slots.values()
                 if s.status == SlotStatus.SCHEDULED and s.start <= at),
                key=lambda s: (s.start, s.zone_id),
            )
            for slot in due:
                # 排队时段是否仍可执行：以“此刻签发、按计划历时能否在当日窗口结束前完成”
                # 判定。同线伙伴超时占用只让伙伴顺延，不应据计划 end 把排队时段误跳过。
                duration = slot.end - slot.start
                window_end = combine_local(
                    local_date(slot.start, self.tz), self.window_end, self.tz
                )
                if at + duration > window_end:
                    slot.status = SlotStatus.SKIPPED
                    slot.reason = {**slot.reason, "skip_reason": "窗口剩余时间不足以完成灌溉"}
                    self._emit("slot_upserted", {"slot": slot.to_dict()})
                    skipped.append(slot.slot_id)
                    continue
                why = self._dispatch_blocked(slot)
                if why:
                    blocked.append({"slot_id": slot.slot_id, "reason": why})
                    continue
                try:
                    cmd = self._issue(slot, at)
                    issued.append(cmd.command_id)
                except QuotaExceededError as exc:
                    blocked.append({"slot_id": slot.slot_id, "reason": str(exc)})
            return {"at": at.isoformat(), "issued": issued, "blocked": blocked, "skipped": skipped}

    def _dispatch_blocked(self, slot: Slot) -> str | None:
        valve = self.domain.valve_of(slot.zone_id)
        if valve.id in self._valve_holds:
            return "valve_manual_hold"
        for c in self.commands.values():
            if c.shared_line == valve.shared_line and c.state in (CommandState.ISSUED, CommandState.OPEN):
                return f"shared_line_busy:{c.command_id}"
        return None

    def _issue(self, slot: Slot, at: datetime) -> Command:
        valve = self.domain.valve_of(slot.zone_id)
        # 因排队/等配额而晚于计划开始时，把实际窗口顺延（历时与计划量不变）
        if at > slot.start:
            slot.end = at + (slot.end - slot.start)
            slot.start = at
        command_id = f"cmd:{slot.slot_id}:{uuid.uuid4().hex[:8]}"
        cmd = Command(
            command_id=command_id, plan_id=slot.plan_id, slot_id=slot.slot_id,
            zone_id=slot.zone_id, valve_id=valve.id, shared_line=valve.shared_line,
            intended_liters=slot.planned_liters, issued_at=at,
        )
        day = self.ledger.reserve(at, command_id, slot.planned_liters)
        slot.status = SlotStatus.LOCKED
        slot.command_id = command_id
        self.commands[command_id] = cmd
        self._emit("command_issued", {"command": cmd.to_dict(), "quota_day": day.isoformat()})
        self._emit("slot_upserted", {"slot": slot.to_dict()})
        return cmd

    def record_receipt(self, receipt: Receipt | dict) -> dict:
        """登记阀门回执。重复 receipt_id / 已终态命令一律忽略，不二次扣水。"""
        with self._lock:
            rc = self._coerce_receipt(receipt)
            if rc.receipt_id in self._receipt_ids:
                return {"status": "duplicate_receipt", "receipt_id": rc.receipt_id}
            cmd = self.commands.get(rc.command_id)
            if cmd is None:
                return {"status": "unknown_command", "command_id": rc.command_id}
            if cmd.state in TERMINAL_STATES:
                return {"status": "ignored_terminal", "command_id": rc.command_id,
                        "state": cmd.state.value}
            self._receipt_ids.add(rc.receipt_id)
            cmd.receipt_ids.add(rc.receipt_id)
            self._emit("receipt_received", {
                "receipt_id": rc.receipt_id, "command_id": rc.command_id,
                "kind": rc.kind, "at": rc.at.isoformat(),
                "observed_lpm": rc.observed_lpm,
            })
            if rc.kind == "open":
                if cmd.state == CommandState.OPEN:
                    return {"status": "ignored_already_open", "command_id": cmd.command_id}
                cmd.state = CommandState.OPEN
                cmd.open_at = rc.at
                self._emit("command_transition", {"command": cmd.to_dict()})
                return {"status": "opened", "command_id": cmd.command_id}
            if rc.kind == "close":
                if cmd.state == CommandState.ISSUED:
                    # 未收到 OPEN 就来 CLOSE：无法确认出过水，按 0 结算并标注异常。
                    cmd.open_at = None
                    note = "close_without_open_receipt"
                    liters = 0.0
                else:
                    lpm = rc.observed_lpm or cmd.observed_lpm or self.domain.valve_by_id(
                        cmd.valve_id
                    ).rated_lpm
                    cmd.observed_lpm = lpm
                    liters = round(minutes_between(cmd.open_at, rc.at) * lpm, 3)
                    note = ""
                self._settle(cmd, rc.at, liters, CommandState.CLOSED, note)
                return {"status": "closed", "command_id": cmd.command_id,
                        "settled_liters": liters}
            return {"status": "unknown_kind", "kind": rc.kind}

    def _settle(self, cmd: Command, at: datetime, liters: float,
                state: CommandState, note: str = "") -> None:
        cmd.state = state
        cmd.close_at = at
        cmd.settled_liters = max(0.0, liters)
        if note:
            cmd.note = note
        delta = self.ledger.settle(cmd.command_id, cmd.settled_liters)
        slot = self.slots.get(cmd.slot_id)
        if slot is not None:
            slot.actual_liters = cmd.settled_liters
            if state == CommandState.CLOSED:
                slot.status = SlotStatus.DONE
            self._emit("slot_upserted", {"slot": slot.to_dict()})
        self._emit("command_transition",
                   {"command": cmd.to_dict(), "quota_delta_liters": round(delta, 3)})

    def reconcile(self, at: datetime | None = None) -> dict:
        """超时巡检：ISSUED 超过回执超时判失联，释放其全部预占（不出水不扣水）。"""
        with self._lock:
            at = at or self.now()
            lost = []
            for cmd in self.commands.values():
                if cmd.state == CommandState.ISSUED and at - cmd.issued_at >= self.ack_timeout:
                    self._settle(cmd, at, 0.0, CommandState.LOST, "ack_timeout_no_flow_confirmed")
                    slot = self.slots.get(cmd.slot_id)
                    if slot is not None:
                        slot.status = SlotStatus.CANCELLED
                        self._emit("slot_upserted", {"slot": slot.to_dict()})
                    lost.append(cmd.command_id)
            return {"at": at.isoformat(), "lost": lost}

    # ---------------------------------------------------------- manual override

    def manual_stop(self, *, valve_id: str | None = None, zone_id: str | None = None,
                    at: datetime | None = None, reason: str = "operator_stop") -> dict:
        """人工停灌：立即压过自动指令，并对该阀门置人工接管直到 resume。"""
        with self._lock:
            at = at or self.now()
            if valve_id is None:
                valve_id = self.domain.valve_of(zone_id).id
            active = [
                c for c in self.commands.values()
                if c.valve_id == valve_id and c.state in (CommandState.ISSUED, CommandState.OPEN)
            ]
            results = []
            for cmd in sorted(active, key=lambda c: c.issued_at, reverse=True):
                if cmd.state == CommandState.OPEN and cmd.open_at is not None:
                    lpm = cmd.observed_lpm or self.domain.valve_by_id(cmd.valve_id).rated_lpm
                    liters = round(minutes_between(cmd.open_at, at) * lpm, 3)
                else:
                    liters = 0.0  # 从未确认开阀：全额释放预占
                self._settle(cmd, at, liters, CommandState.MANUAL_STOP, reason)
                slot = self.slots.get(cmd.slot_id)
                if slot is not None and slot.status != SlotStatus.DONE:
                    slot.status = SlotStatus.CANCELLED
                    slot.reason = {**slot.reason, "manual_stop": reason}
                    self._emit("slot_upserted", {"slot": slot.to_dict()})
                results.append({"command_id": cmd.command_id, "settled_liters": liters})
            self._valve_holds.add(valve_id)
            self._emit("manual_hold_set",
                       {"valve_id": valve_id, "at": at.isoformat(), "reason": reason})
            return {"valve_id": valve_id, "stopped": results, "hold": True}

    def resume_auto(self, valve_id: str) -> None:
        with self._lock:
            self._valve_holds.discard(valve_id)
            self._emit("manual_hold_resumed", {"valve_id": valve_id})

    # --------------------------------------------------------------- views

    def plan_view(self, plan_id: str) -> dict:
        with self._lock:
            plan = self._plan(plan_id)
            slots = [self.slots[s] for s in plan.slot_ids]
            return {
                **self._plan_dict(plan),
                "slots": [s.to_dict() for s in sorted(slots, key=lambda s: (s.start, s.zone_id))],
            }

    def pending_commands(self) -> list[dict]:
        with self._lock:
            return [
                c.to_dict() for c in self.commands.values()
                if c.state in (CommandState.ISSUED, CommandState.OPEN)
            ]

    def quota_view(self, day: date_cls | None = None) -> dict:
        with self._lock:
            return self.ledger.summary(day)

    def zone_status(self, at: datetime | None = None) -> list[dict]:
        """主管视图：每区为何浇/当前读数/当日剩余安排。"""
        with self._lock:
            at = at or self.now()
            today = local_date(at, self.tz)
            plan = self.plans.get(f"PLAN-{today.isoformat()}")
            out = []
            for z in self.domain.zones:
                reading = None
                rs = self.readings.get(z.id, [])
                if rs:
                    last = rs[-1]
                    reading = {**last.to_dict(),
                               "age_min": round((at - last.occurred_at).total_seconds() / 60, 1)}
                zone_slots = []
                if plan:
                    zone_slots = [
                        self.slots[s].to_dict() for s in plan.slot_ids
                        if self.slots[s].zone_id == z.id
                    ]
                out.append({
                    "zone_id": z.id,
                    "crop": z.crop,
                    "stage": z.stage,
                    "valve_id": z.valve_id,
                    "latest_reading": reading,
                    "slots_today": sorted(zone_slots, key=lambda s: s["start"]),
                })
            return out

    def status(self, at: datetime | None = None) -> dict:
        with self._lock:
            at = at or self.now()
            today = local_date(at, self.tz)
            tomorrow = today + timedelta(days=1)
            return {
                "at": at.isoformat(),
                "zones": self.zone_status(at),
                "quota": {
                    "today": self.ledger.summary(today),
                    "tomorrow": self.ledger.summary(tomorrow),
                },
                "pending_commands": self.pending_commands(),
                "plans": {p.plan_id: self._plan_dict(p) for p in self.plans.values()},
                "valve_holds": sorted(self._valve_holds),
            }

    # -------------------------------------------------------------- internals

    def _plan(self, plan_id: str) -> Plan:
        if plan_id not in self.plans:
            raise KeyError(f"未知计划: {plan_id}")
        return self.plans[plan_id]

    def _plan_dict(self, plan: Plan) -> dict:
        return {
            "plan_id": plan.plan_id,
            "date": plan.day.isoformat(),
            "status": plan.status,
            "created_at": plan.created_at.isoformat(),
            "slots": plan.slot_ids,
            "et0": plan.et0,
        }

    @staticmethod
    def _coerce_receipt(r: Receipt | dict) -> Receipt:
        if isinstance(r, Receipt):
            return r
        return Receipt(
            receipt_id=r["receipt_id"],
            command_id=r["command_id"],
            kind=r["kind"],
            at=parse(r["at"]),
            observed_lpm=r.get("observed_lpm"),
        )

    def _emit(self, etype: str, payload: dict) -> None:
        self.store.append({"type": etype, "at": self.now().isoformat(), **payload})

    # ---------------------------------------------------------------- replay

    def _replay(self) -> int:
        n = 0

        def handle(ev: dict) -> None:
            nonlocal n
            n += 1
            self._apply(ev)

        self.store.replay(handle)
        return n

    def _apply(self, ev: dict) -> None:
        t = ev["type"]
        if t == "reading_ingested":
            r = _reading_from_dict(ev["reading"])
            if r.event_id not in self._reading_ids:
                self.readings.setdefault(r.zone_id, []).append(r)
                self.readings[r.zone_id].sort(key=lambda x: x.occurred_at)
                self._reading_ids.add(r.event_id)
        elif t == "et0_set":
            self._et0[("default", date_cls.fromisoformat(ev["date"]))] = dict(ev["et0"])
        elif t == "plan_created":
            pd = ev["plan"]
            plan = Plan(
                plan_id=pd["plan_id"], day=date_cls.fromisoformat(pd["date"]),
                status=pd["status"], created_at=parse(pd["created_at"]),
                slot_ids=[], et0=dict(pd.get("et0", {})),
            )
            self.plans[plan.plan_id] = plan
        elif t == "plan_replaced":
            for sid in ev["removed_slot_ids"]:
                self.slots.pop(sid, None)
            p = self.plans.get(ev["plan_id"])
            if p is not None:
                p.slot_ids = []
        elif t == "plan_published":
            p = self.plans.get(ev["plan_id"])
            if p is not None:
                p.status = "published"
        elif t == "thresholds_updated":
            self._threshold_overrides[ev["plan_id"]] = {
                z: ZoneThresholds(**vals) for z, vals in ev["overrides"].items()
            }
        elif t == "slot_upserted":
            d = ev["slot"]
            slot = Slot(
                slot_id=d["slot_id"], plan_id=d["plan_id"], zone_id=d["zone_id"],
                start=parse(d["start"]), end=parse(d["end"]),
                status=SlotStatus(d["status"]),
                planned_liters=d["planned_liters"], actual_liters=d.get("actual_liters"),
                command_id=d.get("command_id"), reason=d.get("reason", {}),
                based_on_event_id=d.get("based_on_event_id"),
                replaced_from=d.get("replaced_from"),
            )
            self.slots[slot.slot_id] = slot
            plan = self.plans.get(slot.plan_id)
            if plan is not None and slot.slot_id not in plan.slot_ids:
                plan.slot_ids.append(slot.slot_id)
        elif t == "command_issued":
            cd = ev["command"]
            cmd = self._command_from_dict(cd)
            self.commands[cmd.command_id] = cmd
            self.ledger.reserve(cmd.issued_at, cmd.command_id, cmd.intended_liters)
        elif t == "receipt_received":
            self._receipt_ids.add(ev["receipt_id"])
        elif t == "command_transition":
            cmd = self._command_from_dict(ev["command"])
            self.commands[cmd.command_id] = cmd
            slot = self.slots.get(cmd.slot_id)
            if slot is not None and cmd.settled_liters is not None:
                slot.actual_liters = cmd.settled_liters
                if cmd.state == CommandState.CLOSED:
                    slot.status = SlotStatus.DONE
            if cmd.settled_liters is not None and cmd.state in (
                CommandState.CLOSED, CommandState.LOST, CommandState.MANUAL_STOP
            ):
                # 幂等：整段日志重放时只结算一次
                if not self.ledger.is_settled(cmd.command_id):
                    self.ledger.settle(cmd.command_id, cmd.settled_liters)
        elif t == "manual_hold_set":
            self._valve_holds.add(ev["valve_id"])
        elif t == "manual_hold_resumed":
            self._valve_holds.discard(ev["valve_id"])

    @staticmethod
    def _command_from_dict(cd: dict) -> Command:
        return Command(
            command_id=cd["command_id"], plan_id=cd["plan_id"], slot_id=cd["slot_id"],
            zone_id=cd["zone_id"], valve_id=cd["valve_id"], shared_line=cd["shared_line"],
            intended_liters=cd["intended_liters"], issued_at=parse(cd["issued_at"]),
            state=CommandState(cd["state"]),
            open_at=parse(cd["open_at"]) if cd.get("open_at") else None,
            close_at=parse(cd["close_at"]) if cd.get("close_at") else None,
            observed_lpm=cd.get("observed_lpm"),
            settled_liters=cd.get("settled_liters"),
            receipt_ids=set(cd.get("receipt_ids", [])),
            note=cd.get("note", ""),
        )
