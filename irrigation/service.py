"""灌溉决策服务门面：计划草案/预演/发布、调度、回执幂等、人工干预、看板。

水量守恒约定（按园区本地日分账，单位升）：
  ledger 同日净额 = 预留(reserve, +) − 释放(release, −) + 实耗(consume, +)
  - reserve / release / consume 均以 (entry_id) 为幂等键，回执重复不会二次扣水；
  - 时段结算时原子写入 release(−计划量) 与 consume(+实际量)，净额=实际量；
  - 取消未执行时段只写 release，退回预留；
  - 日已占用 = SUM(ledger)，剩余额度 = 日限额 − 已占用。
"""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timedelta

from . import engine, timeutil
from .config import DomainConfig
from .store import Store

DEFAULT_WINDOWS = [{"start": "07:30", "end": "10:30"}, {"start": "15:30", "end": "18:30"}]
DEFAULT_ET0_MM = 5.0
RESEND_INTERVAL = timedelta(seconds=30)

# 已开始执行、仍占线占额的时段状态
ACTIVE_STATUSES = ("dispatched", "closing", "stopping")
# 已开始执行的全部非待执行状态（修订计划时需并入新版）
STARTED_STATUSES = ACTIVE_STATUSES + ("confirmed", "blocked", "close_failed")


class Gateway:
    """阀门网关接口。真实部署替换为 PLC/物联网平台实现。

    必须以 command_uid 做去重：同一 uid 的重复投递不得让硬件动作两次。
    """

    def send(self, command: dict) -> None:  # pragma: no cover - 接口
        raise NotImplementedError


class LoggingGateway(Gateway):
    def __init__(self, sink=print):
        self.sink = sink

    def send(self, command: dict) -> None:
        self.sink(f"[gateway] -> {command['command_uid']} {command['action']} "
                  f"valve={command['valve_id']}")


class IrrigationService:
    def __init__(self, store: Store, config: DomainConfig, gateway: Gateway | None = None,
                 clock=datetime.now):
        self.store = store
        self.cfg = config
        self.gateway = gateway or LoggingGateway()
        self.clock = clock
        self._lock = threading.RLock()
        self._last_sent: dict[str, datetime] = {}

    # ================= 遥测接入 =================
    def ingest_telemetry(self, events: list[dict], now: datetime | None = None) -> dict:
        """接入遥测（可乱序、可重复）。重复 event_id 忽略，新读数触发在线重排。"""
        now = now or self._now()
        accepted = 0
        affected: set[str] = set()
        with self._lock:
            for ev in events:
                ok = self.store.insert_telemetry(
                    {
                        "event_id": ev["event_id"],
                        "zone_id": ev["zone_id"],
                        "occurred_at": timeutil.iso(timeutil.parse(ev["occurred_at"], self.cfg.timezone)),
                        "received_at": timeutil.iso(
                            timeutil.parse(ev["received_at"], self.cfg.timezone)
                            if ev.get("received_at") else now
                        ),
                        "moisture": ev["moisture"],
                        "quality": ev["quality"],
                    }
                )
                if ok:
                    accepted += 1
                    affected.add(ev["zone_id"])
            self.store.mark_incorporated(
                [ev["event_id"] for ev in events], timeutil.iso(now)
            )
            adjustments = []
            for zone_id in affected:
                adj = self._apply_late_reading(zone_id, now)
                if adj:
                    adjustments.append(adj)
            self.store.audit(timeutil.iso(now), "sensor", "telemetry_ingest",
                             {"accepted": accepted, "events": len(events)})
            self.store.commit()
        return {"accepted": accepted, "received": len(events), "adjustments": adjustments}

    # ================= 计划构建 / 预演 / 发布 =================
    def build_plan(self, day_str: str, *, et0: float | dict | None = None,
                   windows: list[dict] | None = None,
                   thresholds: dict[str, float] | None = None,
                   targets: dict[str, float] | None = None,
                   persist: bool = False, note: str = "",
                   now: datetime | None = None) -> dict:
        """构建计划草案。persist=False 即“预演”，不写任何额度。"""
        now = now or self._now()
        day = timeutil.parse_day(day_str)
        windows = windows or DEFAULT_WINDOWS
        thresholds = thresholds or {}
        targets = targets or {}
        with self._lock:
            plan = self._compose_plan(day, et0, windows, thresholds, targets, now)
            plan["note"] = note
            if persist:
                old_draft = self.store.get_draft(day_str)
                if old_draft is not None:
                    self._discard_draft(old_draft["id"])
                plan["id"] = f"P-{day_str}-{uuid.uuid4().hex[:8]}"
                plan["status"] = "draft"
                self._persist_plan(plan)
                self.store.audit(timeutil.iso(now), "agronomist", "plan_draft",
                                 {"plan_id": plan["id"], "day": day_str})
                self.store.commit()
            else:
                plan["id"] = f"preview-{day_str}-{uuid.uuid4().hex[:8]}"
                plan["status"] = "preview"
                # 预演不落库：丢弃推演过程中对 zone_state 的临时写入
                self.store.rollback()
            return self._plan_view(plan)

    def publish_plan(self, plan_id: str | None = None, day_str: str | None = None,
                     now: datetime | None = None) -> dict:
        now = now or self._now()
        with self._lock:
            row = None
            if plan_id:
                row = self.store.get_plan(plan_id)
            elif day_str:
                row = self.store.get_draft(day_str)
            if row is None:
                raise KeyError("没有可发布的草案")
            if row["status"] != "draft":
                raise ValueError(f"计划状态为 {row['status']}，不可发布")
            plan_id = row["id"]
            day = row["plan_date"]

            # 1) 旧版已发布计划：未执行的取消并退额；已开始执行/已结算的时段并入新计划
            old = self.store.get_active_plan(day)
            if old is not None:
                for e in self.store.entries_of_plan(old["id"]):
                    if e["status"] == "pending":
                        self._cancel_entry(e, now, "superseded")
                    elif e["status"] in STARTED_STATUSES:
                        self.store.conn.execute(
                            "UPDATE entries SET plan_id=? WHERE id=?", (plan_id, e["id"])
                        )
                self.store.update_plan_status(old["id"], "superseded", None)

            # 2) 发布并预留新计划全部时段
            self.store.update_plan_status(plan_id, "published", timeutil.iso(now))
            for e in self.store.entries_of_plan(plan_id):
                self._reserve_entry(e)
            self.store.audit(timeutil.iso(now), "agronomist", "plan_publish",
                             {"plan_id": plan_id, "day": day})
            self.store.commit()
            return self.get_plan(plan_id)

    def revise_plan(self, day_str: str, *, et0: float | dict | None = None,
                    windows: list[dict] | None = None,
                    thresholds: dict[str, float] | None = None,
                    targets: dict[str, float] | None = None,
                    note: str = "农艺师修订", publish: bool = True,
                    now: datetime | None = None) -> dict:
        """修订阈值后重建当日计划；已执行时段保留，水量不重算。"""
        now = now or self._now()
        with self._lock:
            # 直接重新发布时：先退掉旧版未执行时段的预留，使新计划在真实剩余
            # 额度上重排；已开始执行的时段保留并在发布时并入新版。
            # 仅存草案（publish=False）时不动旧计划，调度不中断（草案口径偏保守）。
            old = self.store.get_active_plan(day_str)
            if publish and old is not None:
                for e in self.store.entries_of_plan(old["id"]):
                    if e["status"] == "pending":
                        self._cancel_entry(e, now, "revision_rebuild")
            view = self.build_plan(
                day_str, et0=et0, windows=windows, thresholds=thresholds,
                targets=targets, persist=True, note=note, now=now,
            )
            if publish:
                view = self.publish_plan(view["id"], now=now)
            return view

    def _compose_plan(self, day, et0, windows, thresholds, targets, now) -> dict:
        et0 = et0 if et0 is not None else DEFAULT_ET0_MM
        tz_name = self.cfg.timezone

        # 已开始执行（来自当日旧版计划）的时段：占线 + 占额，不重复安排
        carried = self._carried_intervals(day)
        busy_by_line: dict[str, list] = {}
        inflight_l: dict[str, float] = {}
        for e in carried:
            valve = self.cfg.valve_by_id[e["valve_id"]]
            if e["status"] in ACTIVE_STATUSES:
                busy_by_line.setdefault(valve.shared_line, []).append(
                    (timeutil.parse(e["start_at"]), timeutil.parse(e["end_at"]))
                )
            # 在浇的按计划量、已结算的按实际量，从今日总需求中扣减，避免重复安排；
            # blocked（未出水）不扣
            if e["status"] in ACTIVE_STATUSES or e["status"] == "close_failed":
                done_l = float(e["planned_l"])
            elif e["status"] == "confirmed":
                done_l = float(e["actual_l"])
            else:
                done_l = 0.0
            inflight_l[e["zone_id"]] = inflight_l.get(e["zone_id"], 0.0) + done_l
        next_seq = {}
        for e in carried:
            next_seq[e["zone_id"]] = max(next_seq.get(e["zone_id"], 0), e["seq"] + 1)

        remaining_quota = self._remaining_quota_table(day, windows, tz_name)
        zones_out = []
        entries_out = []
        for zone in self.cfg.zones:
            zone_et0 = et0.get(zone.id, DEFAULT_ET0_MM) if isinstance(et0, dict) else float(et0)
            fused = self._fuse(zone.id, now)
            depletion = self._water_balance(zone, now)
            assessment = engine.assess_demand(
                zone,
                et0_mm=zone_et0,
                fused=fused,
                stored_depletion_mm=depletion,
                threshold_override=thresholds.get(zone.id),
                target_override=targets.get(zone.id),
            )
            demand = assessment["demand_l"] - inflight_l.get(zone.id, 0.0)
            demand = max(0.0, round(demand, 3))
            valve = self.cfg.valve_by_id[zone.valve_id]
            scheduled = engine.schedule_zone(
                zone=zone,
                rated_lpm=valve.rated_lpm,
                demand_l=demand,
                day=day,
                windows=windows,
                tz_name=tz_name,
                busy=busy_by_line.setdefault(valve.shared_line, []),
                remaining_quota=remaining_quota,
                seq_start=next_seq.get(zone.id, 1),
            )
            sched_l = round(sum(x["planned_l"] for x in scheduled), 3)
            unscheduled = round(max(0.0, demand - sched_l), 3)
            if unscheduled > 0:
                assessment["reasons"].append(
                    f"受窗口/日额度限制，{unscheduled:.0f}L 未能排入，已列入缺口待人工处理"
                )
            zones_out.append(
                {
                    "zone_id": zone.id,
                    "moisture_threshold": assessment["threshold"],
                    "moisture_target": assessment["target"],
                    "moisture": assessment["moisture"],
                    "quality": assessment["quality"],
                    "et0_mm": zone_et0,
                    "kc": assessment["kc"],
                    "et_need_l": assessment["wb_need_l"],
                    "deficit_l": assessment["sensor_deficit_l"],
                    "demand_l": round(assessment["demand_l"], 3),
                    "inflight_l": round(inflight_l.get(zone.id, 0.0), 3),
                    "scheduled_l": sched_l,
                    "unscheduled_l": unscheduled,
                    "reason": {
                        "basis": assessment["basis"],
                        "reasons": assessment["reasons"],
                        "etc_mm": assessment["etc_mm"],
                        "mad_mm": assessment["mad_mm"],
                        "projected_depletion_mm": assessment["projected_depletion_mm"],
                        "sensor_sources": assessment["sensor_sources"],
                        "sensor_excluded": assessment["sensor_excluded"],
                        "sensor_detail": assessment["sensor_detail"],
                    },
                    "recipe": zone.recipe,
                }
            )
            for s in scheduled:
                entries_out.append(
                    {
                        "zone_id": zone.id,
                        "valve_id": zone.valve_id,
                        "seq": s["seq"],
                        "start_at": timeutil.iso(s["start"]),
                        "end_at": timeutil.iso(s["end"]),
                        "planned_l": s["planned_l"],
                        "day_split": s["day_split"],
                    }
                )

        return {
            "plan_date": day.isoformat(),
            "revision": 1,
            "revision_of": None,
            "et0": et0 if isinstance(et0, dict) else {"_default": float(et0)},
            "windows": windows,
            "created_at": timeutil.iso(now),
            "published_at": None,
            "zones": zones_out,
            "entries": entries_out,
        }

    # ================= 调度（tick / 重启对账） =================
    def tick(self, now: datetime | None = None) -> dict:
        """推进调度：到期开阀、到点关阀、过期退额、失联重发。可在重启后任意时刻调用。"""
        now = now or self._now()
        actions = []
        with self._lock:
            for e in self.store.entries_by_status(["pending", "dispatched"]):
                start = timeutil.parse(e["start_at"])
                end = timeutil.parse(e["end_at"])
                if e["status"] == "pending":
                    if now >= end:
                        self._cancel_entry(e, now, "window_expired")
                        actions.append({"entry_id": e["id"], "action": "expired"})
                    elif now >= start:
                        if self.store.is_paused(e["zone_id"]):
                            continue
                        self._dispatch_open(e, now, actions)
                elif e["status"] == "dispatched" and now >= end:
                    self._dispatch_close(e, now, actions, cause="schedule")
            self._resend_pending(now, actions)
            self.store.commit()
        return {"now": timeutil.iso(now), "actions": actions}

    def reconcile(self, now: datetime | None = None) -> dict:
        """服务重启后对账：命令以 SQLite 为准，待确认命令按同一 uid 重投，不重复扣水。"""
        now = now or self._now()
        with self._lock:
            actions = []
            self._resend_pending(now, actions, force=True)
            self.store.audit(timeutil.iso(now), "system", "reconcile",
                             {"pending_commands": len(self.store.pending_commands())})
            self.store.commit()
        return {"now": timeutil.iso(now), "actions": actions}

    # ================= 阀门回执（幂等） =================
    def ack_command(self, uid: str, ok: bool, result: dict | None = None,
                    now: datetime | None = None) -> dict:
        now = now or self._now()
        result = result or {}
        with self._lock:
            cmd = self.store.get_command(uid)
            if cmd is None:
                raise KeyError(f"未知命令 {uid}")
            if cmd["status"] == "acked":
                # 回执重复：只计数，绝不再结算、不再扣水
                self.store.bump_dup(uid)
                self.store.commit()
                return {"command_uid": uid, "duplicate": True}
            self.store.ack_command(uid, timeutil.iso(now), bool(ok), result)
            entry = self.store.get_entry(cmd["entry_id"])
            outcome = {"command_uid": uid, "duplicate": False, "action": cmd["action"]}
            if cmd["action"] == "open":
                if not ok:
                    self._fail_entry(entry, now, "open_rejected")
                    outcome["entry_status"] = "blocked"
                else:
                    # 不回冲 closing/stopping/blocked 等后续状态
                    if entry["status"] == "pending":
                        self.store.update_entry(entry["id"], status="dispatched")
                        entry = self.store.get_entry(entry["id"])
                    outcome["entry_status"] = entry["status"]
            else:  # close
                if not ok:
                    if entry["status"] in ("blocked", "cancelled", "confirmed"):
                        outcome["entry_status"] = entry["status"]
                    else:
                        self.store.update_entry(entry["id"], status="close_failed")
                        self.store.audit(timeutil.iso(now), "valve", "close_failed",
                                         {"entry_id": entry["id"]})
                        outcome["entry_status"] = "close_failed"
                else:
                    if entry["status"] in ("blocked", "cancelled", "confirmed"):
                        # 人工已处置/已结算：迟到回执不得重新扣水
                        outcome["entry_status"] = entry["status"]
                    else:
                        self._settle_entry(entry, result, now)
                        outcome["entry_status"] = "confirmed"
            self.store.commit()
            return outcome

    def resolve_entry(self, entry_id: str, decision: str, measured_l: float | None = None,
                      operator: str = "operator", now: datetime | None = None) -> dict:
        """人工处置 close_failed：measured=按实测结算，abort=确认未出水并退额。"""
        now = now or self._now()
        with self._lock:
            e = self.store.get_entry(entry_id)
            if e is None:
                raise KeyError(entry_id)
            if decision == "abort":
                self._fail_entry(e, now, "manual_abort")
            elif decision == "measured":
                if measured_l is None:
                    raise ValueError("measured 决策需要 measured_l")
                self._settle_entry(e, {"liters": float(measured_l)}, now)
            else:
                raise ValueError(decision)
            self.store.audit(timeutil.iso(now), operator, "entry_resolved",
                             {"entry_id": entry_id, "decision": decision})
            self.store.commit()
            return self._entry_view(self.store.get_entry(entry_id))

    # ================= 人工停灌 / 恢复 =================
    def pause(self, scope: str = "global", zone_id: str | None = None,
              reason: str = "", operator: str = "operator", now: datetime | None = None) -> dict:
        now = now or self._now()
        if scope == "zone" and not zone_id:
            raise ValueError("zone 级停灌需要 zone_id")
        with self._lock:
            oid = self.store.insert_override(scope, zone_id, "pause",
                                             timeutil.iso(now), reason, operator)
            actions = []
            # 立即压过自动指令：已开阀的立刻发关阀令
            for e in self.store.entries_by_status(["dispatched"]):
                if scope == "global" or e["zone_id"] == zone_id:
                    self._dispatch_close(e, now, actions, cause="manual_pause")
            self.store.audit(timeutil.iso(now), operator, "pause",
                             {"scope": scope, "zone_id": zone_id, "reason": reason})
            self.store.commit()
            return {"override_id": oid, "scope": scope, "zone_id": zone_id, "actions": actions}

    def resume(self, scope: str = "global", zone_id: str | None = None,
               operator: str = "operator", now: datetime | None = None) -> dict:
        now = now or self._now()
        with self._lock:
            self.store.deactivate_overrides(scope, zone_id)
            self.store.audit(timeutil.iso(now), operator, "resume",
                             {"scope": scope, "zone_id": zone_id})
            self.store.commit()
        return {"scope": scope, "zone_id": zone_id, "resumed": True}

    # ================= 看板 =================
    def dashboard(self, day_str: str | None = None, now: datetime | None = None) -> dict:
        now = now or self._now()
        day_str = day_str or timeutil.day_key(now, self.cfg.timezone)
        with self._lock:
            plan_row = self.store.get_active_plan(day_str)
            zones_view, entries_view = [], []
            if plan_row:
                for pz in self.store.plan_zone_rows(plan_row["id"]):
                    z = self.cfg.zone_by_id[pz["zone_id"]]
                    reasons = json.loads(pz["reason"] or "{}")
                    zones_view.append(
                        {
                            "zone_id": z.id,
                            "crop": z.crop,
                            "stage": z.stage,
                            "valve_id": z.valve_id,
                            "moisture": pz["moisture"],
                            "quality": pz["quality"],
                            "threshold": pz["moisture_threshold"],
                            "demand_l": pz["demand_l"],
                            "scheduled_l": pz["scheduled_l"],
                            "unscheduled_l": pz["unscheduled_l"],
                            "why": reasons,
                            "recipe": json.loads(pz["recipe_json"]) if pz["recipe_json"] else None,
                        }
                    )
                entries_view = [self._entry_view(e)
                                for e in self.store.entries_of_plan(plan_row["id"])]

            days = sorted({sp["day"] for e in entries_view for sp in e["day_split"]})
            if not days:
                days = [day_str]
            daily = {}
            for d in days:
                rows = self.store.ledger_rows(d)
                reserved = sum(r["liters"] for r in rows if r["ref_kind"] == "reserve")
                released = -sum(r["liters"] for r in rows if r["ref_kind"] == "release")
                consumed = sum(r["liters"] for r in rows if r["ref_kind"] == "consume")
                committed = self.store.committed(d)
                daily[d] = {
                    "limit_l": self.cfg.daily_water_limit_liters,
                    "reserved_l": round(reserved, 3),
                    "released_l": round(released, 3),
                    "consumed_l": round(consumed, 3),
                    "committed_l": round(committed, 3),
                    "remaining_l": round(self.cfg.daily_water_limit_liters - committed, 3),
                }
            pending_cmds = [
                {
                    "command_uid": c["command_uid"],
                    "entry_id": c["entry_id"],
                    "valve_id": c["valve_id"],
                    "action": c["action"],
                    "at": c["at"],
                    "dup_count": c["dup_count"],
                }
                for c in self.store.pending_commands()
            ]
            alarms = [
                {"entry_id": e["id"], "zone_id": e["zone_id"], "status": e["status"]}
                for e in self.store.entries_by_status(
                    ["blocked", "close_failed", "dispatched", "closing", "stopping"]
                )
            ]
            return {
                "now": timeutil.iso(now),
                "day": day_str,
                "active_plan_id": plan_row["id"] if plan_row else None,
                "zones": zones_view,
                "entries": entries_view,
                "daily_quota": daily,
                "pending_commands": pending_cmds,
                "pauses": [
                    {"scope": r["scope"], "zone_id": r["zone_id"], "reason": r["reason"],
                     "at": r["at"]}
                    for r in self.store.active_pauses()
                ],
                "alarms": alarms,
            }

    def ledger(self, day_str: str | None = None) -> list[dict]:
        with self._lock:
            return [dict(r) for r in self.store.ledger_rows(day_str)]

    def get_plan(self, plan_id: str) -> dict:
        with self._lock:
            row = self.store.get_plan(plan_id)
            if row is None:
                raise KeyError(plan_id)
            plan = {
                "id": row["id"],
                "plan_date": row["plan_date"],
                "revision": row["revision"],
                "status": row["status"],
                "et0": json.loads(row["et0_json"]),
                "windows": json.loads(row["windows_json"]),
                "created_at": row["created_at"],
                "published_at": row["published_at"],
                "note": row["note"],
                "zones": [dict(r) for r in self.store.plan_zone_rows(plan_id)],
                "entries": [self._entry_view(e) for e in self.store.entries_of_plan(plan_id)],
            }
            for pz in plan["zones"]:
                pz["reason"] = json.loads(pz["reason"] or "{}")
                pz["recipe"] = json.loads(pz["recipe_json"]) if pz["recipe_json"] else None
                del pz["recipe_json"]
            return plan

    # ================= 内部：状态推导 =================
    def _now(self) -> datetime:
        v = self.clock()
        return v if v.tzinfo else v.replace(tzinfo=timeutil.ZoneInfo(self.cfg.timezone))

    def _fuse(self, zone_id: str, as_of: datetime) -> engine.FusedMoisture:
        rows = self.store.telemetry_for_zone(zone_id)
        return engine.fuse_readings([dict(r) for r in rows], as_of=as_of,
                                    wet_until=self._wet_until(zone_id))

    def _wet_until(self, zone_id: str) -> datetime | None:
        """最近灌溉的“湿润栅栏”时刻：进行中取计划结束，已结算取关阀回执时刻。"""
        candidates: list[datetime] = []
        for e in self.store.conn.execute(
                "SELECT id,status,start_at,end_at FROM entries WHERE zone_id=? "
                "AND status IN ('dispatched','closing','stopping','confirmed')",
                (zone_id,)).fetchall():
            if e["status"] in ACTIVE_STATUSES:
                candidates.append(timeutil.parse(e["end_at"]))
            else:
                close = self.store.get_command(f"cmd:{e['id']}:close")
                if close is not None and close["ack_at"]:
                    candidates.append(timeutil.parse(close["ack_at"]))
                else:
                    candidates.append(timeutil.parse(e["end_at"]))
        return max(candidates) if candidates else None

    def _water_balance(self, zone, now: datetime) -> float:
        """蒸散水量平衡：自最近一次 good 读数（或锚点）起，累计 ETc − 有效灌溉深度。"""
        area = engine.effective_area_m2(zone)
        taw = engine.taw_mm(zone)

        # 选起点：优先最近 good 读数（用墒情校准模型）
        anchor_dt = None
        depletion = 0.0
        rows = self.store.telemetry_for_zone(zone.id)
        for r in sorted(rows, key=lambda x: x["occurred_at"], reverse=True):
            occ = timeutil.parse(r["occurred_at"])
            if occ <= now and r["quality"] == "good":
                m = float(r["moisture"])
                depletion = (zone.soil["field_capacity"] - m) * zone.soil["root_depth_mm"]
                anchor_dt = occ
                break
        st = self.store.get_zone_state(zone.id)
        if anchor_dt is None and st and st["anchored_at"]:
            anchor_dt = timeutil.parse(st["anchored_at"])
            depletion = float(st["depletion_mm"])
        if anchor_dt is None:
            anchor_dt = now
        self.store.upsert_zone_state(
            zone.id, anchored_at=timeutil.iso(anchor_dt),
            depletion_mm=round(depletion, 3),
        )

        # 累计 ETc：只计锚点→当前时刻实际覆盖的时间，按本地日边界比例拆分
        tz = self.cfg.timezone
        zone_info = timeutil.ZoneInfo(tz)
        d0 = anchor_dt.astimezone(zone_info).date()
        d1 = now.astimezone(zone_info).date()
        cur_day = d0
        from datetime import timedelta as _td
        while cur_day <= d1:
            et0 = self._et0_of_day(cur_day.isoformat(), zone.id)
            if et0 is not None:
                day_start = datetime.combine(cur_day, datetime.min.time(), zone_info)
                day_end = day_start + _td(days=1)
                # 锚点 good 读数本身已校准 depletion，当日 ET 从锚点后起算
                seg_start = anchor_dt if cur_day == d0 else day_start
                seg_end = min(now, day_end)
                overlap = (seg_end - seg_start).total_seconds()
                if overlap > 0:
                    depletion += zone.profile["kc"] * et0 * overlap / 86400.0
            cur_day += _td(days=1)

        # 扣减起点之后已确认的灌溉（按实际过流升数折算水深 mm）
        row = self.store.conn.execute(
            "SELECT COALESCE(SUM(actual_l),0) AS s FROM entries "
            "WHERE zone_id=? AND status='confirmed' AND start_at>=?",
            (zone.id, timeutil.iso(anchor_dt)),
        ).fetchone()
        depletion -= float(row["s"]) * 1000.0 / (area * 1_000_000.0) * 1000.0
        return round(min(max(depletion, 0.0), taw), 3)

    def _et0_of_day(self, day_str: str, zone_id: str) -> float | None:
        row = self.store.conn.execute(
            "SELECT p.id FROM plans p WHERE p.plan_date=? ORDER BY "
            "CASE p.status WHEN 'published' THEN 0 ELSE 1 END, p.revision DESC LIMIT 1",
            (day_str,),
        ).fetchone()
        if not row:
            return None
        pz = self.store.plan_zone_row(row["id"], zone_id)
        return float(pz["et0_mm"]) if pz and pz["et0_mm"] is not None else None

    def _carried_intervals(self, day) -> list:
        """当日已发布旧计划中已开始执行（未取消）的时段。"""
        active = self.store.get_active_plan(day.isoformat())
        if not active:
            return []
        out = []
        for e in self.store.entries_of_plan(active["id"]):
            if e["status"] in STARTED_STATUSES:
                out.append(e)
        return out

    def _remaining_quota_table(self, day, windows, tz_name) -> dict[str, float]:
        """排程用的各本地日剩余额度（含已发布计划的预留，草案不占额）。"""
        touched = {day.isoformat()}
        for w in windows:
            hh, mm = (int(x) for x in w["end"].split(":"))
            if (hh, mm) <= (int(w["start"].split(":")[0]), int(w["start"].split(":")[1])):
                from datetime import timedelta as _td
                touched.add((day + _td(days=1)).isoformat())
        table = {}
        for d in touched:
            table[d] = self.cfg.daily_water_limit_liters - self.store.committed(d)
        return table

    # ================= 内部：持久化映射 =================
    def _persist_plan(self, plan: dict) -> None:
        self.store.insert_plan(plan)
        for pz in plan["zones"]:
            self.store.upsert_plan_zone({"plan_id": plan["id"], **pz})
        for i, e in enumerate(plan["entries"], start=1):
            entry = {
                "id": f"E-{plan['id']}-{i:03d}-{uuid.uuid4().hex[:6]}",
                "plan_id": plan["id"],
                **e,
            }
            e["id"] = entry["id"]
            self.store.insert_entry(entry)

    def _discard_draft(self, plan_id: str) -> None:
        self.store.conn.execute("DELETE FROM entries WHERE plan_id=?", (plan_id,))
        self.store.conn.execute("DELETE FROM plan_zones WHERE plan_id=?", (plan_id,))
        self.store.conn.execute("DELETE FROM plans WHERE id=?", (plan_id,))

    def _plan_view(self, plan: dict) -> dict:
        return {
            "id": plan.get("id"),
            "plan_date": plan["plan_date"],
            "status": plan["status"],
            "et0": plan["et0"],
            "windows": plan["windows"],
            "note": plan.get("note", ""),
            "zones": plan["zones"],
            "entries": [
                {
                    "zone_id": e["zone_id"],
                    "valve_id": e["valve_id"],
                    "seq": e["seq"],
                    "start_at": e["start_at"],
                    "end_at": e["end_at"],
                    "planned_l": e["planned_l"],
                    "day_split": e["day_split"],
                    **({"id": e["id"]} if "id" in e else {}),
                }
                for e in plan["entries"]
            ],
        }

    def _entry_view(self, e) -> dict:
        return {
            "id": e["id"],
            "plan_id": e["plan_id"],
            "zone_id": e["zone_id"],
            "valve_id": e["valve_id"],
            "seq": e["seq"],
            "start_at": e["start_at"],
            "end_at": e["end_at"],
            "planned_l": e["planned_l"],
            "day_split": json.loads(e["day_split_json"]),
            "status": e["status"],
            "actual_l": e["actual_l"],
            "actual_split": json.loads(e["actual_split_json"])
            if e["actual_split_json"] else None,
            "settle_source": e["settle_source"],
        }

    # ================= 内部：台账与命令 =================
    def _reserve_entry(self, e) -> None:
        for sp in json.loads(e["day_split_json"]):
            self.store.ledger_insert_ignore(
                sp["day"], "reserve", e["id"], e["zone_id"], sp["liters"],
                e["start_at"], "计划预留",
            )

    def _release_reserve(self, e) -> None:
        for sp in json.loads(e["day_split_json"]):
            if self.store.ledger_insert_ignore(
                    sp["day"], "release", e["id"], e["zone_id"], -sp["liters"],
                    timeutil.iso(self._now()), "取消/结算释放"):
                pass

    def _cancel_entry(self, e, now, cause: str) -> None:
        self.store.update_entry(e["id"], status="cancelled")
        self._release_reserve(e)
        self.store.audit(timeutil.iso(now), "system", "entry_cancelled",
                         {"entry_id": e["id"], "cause": cause})

    def _fail_entry(self, e, now, cause: str) -> None:
        self.store.update_entry(e["id"], status="blocked")
        self._release_reserve(e)
        self.store.audit(timeutil.iso(now), "system", "entry_blocked",
                         {"entry_id": e["id"], "cause": cause})

    def _dispatch_open(self, e, now, actions) -> None:
        uid = f"cmd:{e['id']}:open"
        if self.store.get_command(uid) is None:
            self.store.insert_command(
                {"command_uid": uid, "entry_id": e["id"], "valve_id": e["valve_id"],
                 "action": "open", "at": timeutil.iso(now),
                 "payload": {"planned_l": e["planned_l"], "end_at": e["end_at"]}}
            )
        self._send(uid, now, actions)
        self.store.update_entry(e["id"], status="dispatched")

    def _dispatch_close(self, e, now, actions, *, cause: str) -> None:
        uid = f"cmd:{e['id']}:close"
        if self.store.get_command(uid) is None:
            self.store.insert_command(
                {"command_uid": uid, "entry_id": e["id"], "valve_id": e["valve_id"],
                 "action": "close", "at": timeutil.iso(now),
                 "payload": {"cause": cause}}
            )
        self._send(uid, now, actions)
        self.store.update_entry(
            e["id"], status="stopping" if cause == "manual_pause" else "closing"
        )

    def _send(self, uid: str, now: datetime, actions: list, force: bool = False) -> None:
        last = self._last_sent.get(uid)
        if not force and last is not None and now - last < RESEND_INTERVAL:
            return
        cmd = self.store.get_command(uid)
        if cmd is None or cmd["status"] == "acked":
            return
        try:
            self.gateway.send(dict(cmd))
            self._last_sent[uid] = now
            actions.append({"command_uid": uid, "action": cmd["action"], "sent": True})
        except Exception as exc:  # 网关故障：留待下次 tick/reconcile 重投
            actions.append({"command_uid": uid, "sent": False, "error": str(exc)})

    def _resend_pending(self, now, actions, *, force: bool = False) -> None:
        for cmd in self.store.pending_commands():
            self._send(cmd["command_uid"], now, actions, force=force)

    def _settle_entry(self, e, result: dict, now: datetime) -> None:
        """以实际过流结算：释放计划预留 + 记实际消耗，跨午夜按时间比例拆分。"""
        split_plan = json.loads(e["day_split_json"])
        open_cmd = self.store.get_command(f"cmd:{e['id']}:open")
        opened_at = timeutil.parse(e["start_at"])
        if open_cmd is not None and open_cmd["ack_at"]:
            opened_at = timeutil.parse(open_cmd["ack_at"])
        closed_at = now
        actual_total = result.get("liters")
        source = "measured" if actual_total is not None else "rated"
        if actual_total is None:
            elapsed = max((closed_at - opened_at).total_seconds(), 0.0)
            rated_lpm = self.cfg.valve_by_id[e["valve_id"]].rated_lpm
            actual_total = round(rated_lpm * elapsed / 60.0, 3)
        actual_total = max(0.0, float(actual_total))

        segs = timeutil.day_segments(opened_at, closed_at, self.cfg.timezone)
        total_s = sum(s for _, s in segs) or 1.0
        actual_split = [
            {"day": d.isoformat(), "seconds": round(s, 3),
             "liters": round(actual_total * s / total_s, 3)}
            for d, s in segs
        ]
        # 实测总量与时间拆分的尾差并入第一天，保证分天合计 == 总量（水量守恒）
        if actual_split:
            tail = round(actual_total - sum(x["liters"] for x in actual_split), 3)
            actual_split[0]["liters"] = round(actual_split[0]["liters"] + tail, 3)

        # 先释放计划预留（幂等），再记实际消耗（幂等）；重复 close 回执整体为空操作
        for sp in split_plan:
            self.store.ledger_insert_ignore(
                sp["day"], "release", e["id"], e["zone_id"], -sp["liters"],
                timeutil.iso(now), "结算释放",
            )
        for sp in actual_split:
            self.store.ledger_insert_ignore(
                sp["day"], "consume", e["id"], e["zone_id"], sp["liters"],
                timeutil.iso(now), f"实际过流/{source}",
            )
        self.store.update_entry(
            e["id"], status="confirmed", actual_l=actual_total,
            actual_split_json=actual_split, settle_source=source,
        )
        self.store.upsert_zone_state(
            e["zone_id"], last_irrigation_at=timeutil.iso(opened_at),
            last_irrigation_l=actual_total,
        )
        self.store.audit(timeutil.iso(now), "valve", "entry_confirmed",
                         {"entry_id": e["id"], "actual_l": actual_total, "source": source})

    # ================= 迟到读数在线重排 =================
    def _apply_late_reading(self, zone_id: str, now: datetime) -> dict | None:
        """新读数只影响当日计划中尚未开始的时段；已执行时段不动、不重扣。"""
        day_str = timeutil.day_key(now, self.cfg.timezone)
        plan_row = self.store.get_active_plan(day_str)
        if plan_row is None:
            return None
        zone = self.cfg.zone_by_id.get(zone_id)
        if zone is None:
            return None

        pending = [
            e for e in self.store.entries_of_plan(plan_row["id"])
            if e["zone_id"] == zone_id and e["status"] == "pending"
        ]
        future = [e for e in pending if timeutil.parse(e["start_at"]) > now]
        if not future:
            return None  # 没有可调整的未来时段（含正在浇的，绝不动）

        fused = self._fuse(zone_id, now)
        et0 = self._et0_of_day(day_str, zone_id) or DEFAULT_ET0_MM
        assessment = engine.assess_demand(
            zone, et0_mm=et0, fused=fused,
            stored_depletion_mm=self._water_balance(zone, now),
        )
        delivered = self._delivered_today(zone_id, day_str)
        demand_left = max(0.0, round(assessment["demand_l"] - delivered, 3))

        # 取消该分区全部未开始时段并退额
        for e in future:
            self._cancel_entry(e, now, "late_telemetry_replan")

        windows = json.loads(plan_row["windows_json"])
        busy = self._line_busy(plan_row["id"], zone.valve_id, now)
        remaining_quota = {}
        for d in {day_str, (timeutil.parse_day(day_str) + timedelta(days=1)).isoformat()}:
            remaining_quota[d] = self.cfg.daily_water_limit_liters - self.store.committed(d)
        next_seq = max(
            (e["seq"] for e in self.store.entries_of_plan(plan_row["id"])
             if e["zone_id"] == zone.id),
            default=0,
        ) + 1
        scheduled = engine.schedule_zone(
            zone=zone, rated_lpm=self.cfg.valve_by_id[zone.valve_id].rated_lpm,
            demand_l=demand_left, day=timeutil.parse_day(day_str), windows=windows,
            tz_name=self.cfg.timezone, busy=busy, remaining_quota=remaining_quota,
            seq_start=next_seq,
        )
        new_entries = []
        for s in scheduled:
            entry = {
                "id": f"E-{plan_row['id']}-{uuid.uuid4().hex[:9]}",
                "plan_id": plan_row["id"],
                "zone_id": zone.id,
                "valve_id": zone.valve_id,
                "seq": s["seq"],
                "start_at": timeutil.iso(s["start"]),
                "end_at": timeutil.iso(s["end"]),
                "planned_l": s["planned_l"],
                "day_split": s["day_split"],
            }
            self.store.insert_entry(entry)
            self._reserve_entry(self.store.get_entry(entry["id"]))
            new_entries.append(entry["id"])

        sched_l = round(sum(x["planned_l"] for x in scheduled), 3)
        self.store.upsert_plan_zone(
            {
                "plan_id": plan_row["id"],
                "zone_id": zone_id,
                "moisture_threshold": assessment["threshold"],
                "moisture_target": assessment["target"],
                "moisture": assessment["moisture"],
                "quality": assessment["quality"],
                "et0_mm": et0,
                "kc": assessment["kc"],
                "et_need_l": assessment["wb_need_l"],
                "deficit_l": assessment["sensor_deficit_l"],
                "demand_l": round(assessment["demand_l"], 3),
                "scheduled_l": sched_l,
                "unscheduled_l": round(max(0.0, demand_left - sched_l), 3),
                "reason": {
                    "basis": assessment["basis"],
                    "reasons": assessment["reasons"]
                    + [f"迟到读数于 {timeutil.iso(now)} 触发重排，仅影响未执行时段"],
                    "sensor_sources": assessment["sensor_sources"],
                    "sensor_excluded": assessment["sensor_excluded"],
                },
                "recipe": zone.recipe,
            }
        )
        self.store.audit(timeutil.iso(now), "sensor", "late_telemetry_replan",
                         {"zone_id": zone_id, "cancelled": [e["id"] for e in future],
                          "new_entries": new_entries})
        return {"zone_id": zone_id, "cancelled": [e["id"] for e in future],
                "new_entries": new_entries, "demand_left_l": demand_left}

    def _delivered_today(self, zone_id: str, day_str: str) -> float:
        """当日已实耗 + 在浇（未含待执行，待执行正是重排对象）。"""
        consumed = self.store.conn.execute(
            "SELECT COALESCE(SUM(liters),0) s FROM ledger WHERE day=? AND zone_id=? "
            "AND ref_kind='consume'", (day_str, zone_id)
        ).fetchone()["s"]
        active = self.store.conn.execute(
            "SELECT COALESCE(SUM(planned_l),0) s FROM entries e JOIN plans p ON e.plan_id=p.id "
            "WHERE p.plan_date=? AND e.zone_id=? AND e.status IN (?,?,?)",
            (day_str, zone_id, *ACTIVE_STATUSES),
        ).fetchone()["s"]
        return float(consumed) + float(active)

    def _line_busy(self, plan_id: str, valve_id: str, now: datetime):
        line = self.cfg.valve_by_id[valve_id].shared_line
        valve_ids = {v.id for v in self.cfg.valves if v.shared_line == line}
        busy = []
        q = ",".join("?" * len(valve_ids))
        for e in self.store.conn.execute(
                f"SELECT start_at,end_at FROM entries WHERE valve_id IN ({q}) "
                "AND status IN ('pending','dispatched','closing','stopping')",
                list(valve_ids)).fetchall():
            busy.append((timeutil.parse(e["start_at"]), timeutil.parse(e["end_at"])))
        return busy
