"""决策服务核对测试。

重点核对需求点名的场景：
- 跨午夜配额归属与水量守恒；
- 共用管线并发门控（同线串行、异线并行、超时占用排队）；
- 传感器降级（offline / 陈旧 / suspect）下不重复浇已浇畦面；
- 迟到读数只改未锁定时段；执据重复不二次扣水；人工停灌压过自动；
- 事件日志重放后从同一计划继续。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from irrigation.config import Domain, NutrientFormula, StageCoefficients, Valve, Zone, domain_from_dict, load_domain
from irrigation.et import EtEstimator, ZoneContext
from irrigation.models import CommandState, Quality, SlotStatus
from irrigation.service import IrrigationService, Receipt
from irrigation.store import EventStore
from irrigation.clock import combine_local

SH = date(2026, 9, 11)
TZ = "Asia/Shanghai"


def reading(event_id, zone, occ, recv, moisture, quality):
    return {
        "event_id": event_id,
        "zone_id": zone,
        "occurred_at": occ,
        "received_at": recv,
        "moisture": moisture,
        "quality": quality,
    }


class Clock:
    """可控时钟。"""

    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def advance(self, minutes: float):
        self.t += timedelta(minutes=minutes)
        return self.t


class ServiceTestBase(unittest.TestCase):
    def make_service(self, store=None, domain=None, **kw):
        from zoneinfo import ZoneInfo

        domain = domain or load_domain(Path(__file__).parents[1] / "reference" / "domain.json")
        start = kw.pop("clock_start", "06:30")
        clock = Clock(combine_local(SH, start, ZoneInfo(domain.timezone)))
        return IrrigationService(domain, store or EventStore(None), clock=clock, **kw), clock


class OutOfOrderTelemetryTest(unittest.TestCase):
    """资料包约定：接收顺序不代表发生顺序，一律以 occurred_at 定序。"""

    def test_ingest_in_received_order_uses_occurred_order(self):
        svc, _ = ServiceTestBase().make_service()
        raw = json.loads((Path(__file__).parents[1] / "reference" / "domain.json").read_text("utf-8"))
        # 按 received_at 顺序入库：M-102 先到（06:04:30），M-101 后到（06:05:10）
        svc.ingest_readings(raw["telemetry"])
        at = combine_local(SH, "07:00", svc.tz)
        latest = svc.latest_reading("GH-A-01", at)
        self.assertEqual(latest.event_id, "M-102")  # 业务时间 05:58 晚于 05:55
        self.assertEqual(latest.quality, Quality.GOOD)

    def test_duplicate_event_id_ingested_once(self):
        svc, _ = ServiceTestBase().make_service()
        r = reading("M-1", "GH-A-01", combine_local(SH, "05:00", svc.tz),
                    combine_local(SH, "05:01", svc.tz), 0.2, "good")
        svc.ingest_reading(r)
        svc.ingest_reading(dict(r))
        self.assertEqual(len(svc.readings["GH-A-01"]), 1)


class PlanLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = ServiceTestBase().make_service()
        self.svc.ingest_reading(reading(
            "M-101", "GH-A-01", combine_local(SH, "05:55", self.svc.tz),
            combine_local(SH, "06:05", self.svc.tz), 0.23, "suspect"))

    def test_preview_does_not_mutate_state(self):
        out = self.svc.preview(SH, et0=4.0)
        self.assertGreater(out["total_liters"], 0)
        self.assertTrue(out["within_quota"])
        self.assertEqual(self.svc.plans, {})  # 预演不留计划
        self.assertEqual(self.svc.quota_view(SH)["used_liters"], 0)

    def test_propose_revise_thresholds_publish_execute_close(self):
        plan = self.svc.generate_plan(SH, et0=0.0)
        view = self.svc.plan_view(plan.plan_id)
        slot = next(s for s in view["slots"] if s["zone_id"] == "GH-A-01")
        # 0.23 < 下限 0.25，触发；亏缺 36mm 先被坐果期日封顶 10mm 截断
        self.assertIn("moisture_below_min", slot["reason"]["triggers"])
        self.assertIn("stage_daily_cap", slot["reason"]["capped_by"])
        self.assertEqual(slot["status"], "proposed")
        self.assertEqual(slot["planned_liters"], 10.0 * 200)

        # 农艺师把下限调到 0.20、目标 0.24：0.23 仅差 0.01 → 3mm = 600L
        self.svc.update_thresholds(plan.plan_id, {"GH-A-01": {"min_moisture": 0.20, "target_moisture": 0.24}})
        view = self.svc.plan_view(plan.plan_id)
        a01 = [s for s in view["slots"] if s["zone_id"] == "GH-A-01"]
        self.assertEqual(sum(s["planned_liters"] for s in a01), 3.0 * 200)

        self.svc.publish_plan(plan.plan_id)
        self.clock.t = combine_local(SH, "07:00", self.svc.tz)
        res = self.svc.dispatch_due()
        self.assertEqual(len(res["issued"]), 1)
        cmd_id = res["issued"][0]
        # 签发即预占
        q = self.svc.quota_view(SH)
        self.assertEqual(q["reserved_liters"], 600.0)
        self.assertEqual(q["used_liters"], 600.0)

        self.svc.record_receipt(Receipt("R-1", cmd_id, "open", self.clock.t))
        self.clock.advance(5)  # 600L/120lpm 恰为 5 分钟
        close = self.svc.record_receipt(Receipt("R-2", cmd_id, "close", self.clock.t, observed_lpm=120))
        self.assertEqual(close["settled_liters"], 600.0)
        q = self.svc.quota_view(SH)
        self.assertEqual(q["settled_liters"], 600.0)
        self.assertEqual(q["reserved_liters"], 0.0)
        slot = self.svc.slots[slot["slot_id"]]
        self.assertEqual(slot.status, SlotStatus.DONE)
        self.assertEqual(slot.actual_liters, 600.0)


class LateReadingTest(unittest.TestCase):
    """迟到的好读数：只取消尚未执行的时段，已浇时段绝不回补/倒扣。"""

    def setUp(self):
        # 候选时刻都在新鲜度窗口内，使初始计划基于同一条 suspect 读数
        # 在 07:00 与 09:00 各排出一个 12mm 封顶时段（亏缺 36mm）。
        raw = json.loads((Path(__file__).parents[1] / "reference" / "domain.json").read_text("utf-8"))
        raw["stages"] = {"坐果期": {"kc": 1.1, "max_daily_liters_per_m2": 40.0}}
        domain = domain_from_dict(raw)
        self.svc, self.clock = ServiceTestBase().make_service(
            domain=domain, candidate_starts=("07:00", "09:00"))
        self.svc.ingest_reading(reading(
            "M-101", "GH-A-01", combine_local(SH, "05:55", self.svc.tz),
            combine_local(SH, "06:05", self.svc.tz), 0.23, "suspect"))
        self.plan = self.svc.generate_plan(SH, et0=0.0)
        self.svc.publish_plan(self.plan.plan_id)

    def test_late_good_reading_only_affects_unlocked_slots(self):
        plan_id = self.plan.plan_id
        view = self.svc.plan_view(plan_id)
        a01 = sorted((s for s in view["slots"] if s["zone_id"] == "GH-A-01"),
                     key=lambda s: s["start"])
        self.assertEqual(len(a01), 2)

        # 07:00 时段执行完毕（12mm * 200m2 = 2400L，120lpm 共 20 分钟）
        self.clock.t = combine_local(SH, "07:00", self.svc.tz)
        cmd = self.svc.dispatch_due()["issued"][0]
        self.svc.record_receipt(Receipt("R-1", cmd, "open", self.clock.t))
        self.clock.t = combine_local(SH, "07:20", self.svc.tz)
        self.svc.record_receipt(Receipt("R-2", cmd, "close", self.clock.t, observed_lpm=120))
        used_after_water = self.svc.quota_view(SH)["used_liters"]
        self.assertEqual(used_after_water, 20 * 120)

        # 07:30 才收到 05:58 的好读数：含水率 0.36，已高于目标，畦面其实已湿
        self.svc.ingest_reading(reading(
            "M-102", "GH-A-01", combine_local(SH, "05:58", self.svc.tz),
            combine_local(SH, "07:30", self.svc.tz), 0.36, "good"))
        rev = self.svc.revise_plan(plan_id)
        self.assertGreaterEqual(len(rev["cancelled"]), 1)
        self.assertEqual(rev["locked_preserved"], 1)

        view = self.svc.plan_view(plan_id)
        for s in view["slots"]:
            if s["zone_id"] != "GH-A-01":
                continue
            if s["status"] == "done":
                self.assertEqual(s["actual_liters"], 20 * 120)  # 已浇水量不动
            else:
                self.assertIn(s["status"], ("cancelled", "skipped"))
        # 没有产生第二次扣水
        self.assertEqual(self.svc.quota_view(SH)["used_liters"], used_after_water)
        self.assertEqual(self.svc.pending_commands(), [])


class QuotaConservationTest(unittest.TestCase):
    def test_reserve_settle_idempotent_and_releases(self):
        from irrigation.quota import QuotaLedger
        from irrigation.config import load_domain
        ledger = QuotaLedger(load_domain(Path(__file__).parents[1] / "reference" / "domain.json"))
        tz = ledger.tz
        t1 = datetime(2026, 9, 11, 23, 56, tzinfo=tz)
        ledger.reserve(t1, "c1", 320.0)
        ledger.reserve(t1, "c1", 320.0)  # 重复预占不翻倍
        self.assertEqual(ledger.used(SH), 320.0)
        # 跨午夜结算：归属仍是开阀当日
        t2 = datetime(2026, 9, 12, 0, 3, tzinfo=tz)
        delta = ledger.settle("c1", 9 * 90.0)  # 实流 9 分钟
        self.assertEqual(delta, 810.0 - 320.0)
        self.assertEqual(ledger.used(SH), 810.0)
        self.assertEqual(ledger.used(date(2026, 9, 12)), 0.0)
        # 重复结算净变化为 0
        self.assertEqual(ledger.settle("c1", 9999.0), 0.0)
        self.assertEqual(ledger.used(SH), 810.0)

    def test_cross_midnight_command_billed_to_opening_day(self):
        svc, clock = ServiceTestBase().make_service(
            candidate_starts=("23:54",), window_end="23:59")
        # 让 GH-A-01 湿润不参与，只保留 GH-A-02 的跨午夜命令
        svc.ingest_reading(reading(
            "M-wet", "GH-A-01", combine_local(SH, "23:30", svc.tz),
            combine_local(SH, "23:31", svc.tz), 0.40, "good"))
        svc.ingest_reading(reading(
            "M-x", "GH-A-02", combine_local(SH, "23:30", svc.tz),
            combine_local(SH, "23:31", svc.tz), 0.20, "good"))
        # 把单次灌量压到 2mm => 320L，约 3.6 分钟，23:54 开、窗口内结束
        plan = svc.generate_plan(SH, et0=0.0)
        svc.update_thresholds(plan.plan_id, {"GH-A-02": {"max_single_irrigation_mm": 2.0}})
        svc.publish_plan(plan.plan_id)
        clock.t = combine_local(SH, "23:54", svc.tz)
        cmd_id = svc.dispatch_due()["issued"][0]
        self.assertEqual(svc.quota_view(SH)["reserved_liters"], 320.0)
        svc.record_receipt(Receipt("o", cmd_id, "open", clock.t))
        # 关阀回执次日 00:03 才到（阀门跑过了午夜）
        clock.t = datetime(2026, 9, 12, 0, 3, tzinfo=svc.tz)
        out = svc.record_receipt(Receipt("c", cmd_id, "close", clock.t, observed_lpm=90))
        self.assertEqual(out["settled_liters"], 9 * 90.0)
        self.assertEqual(svc.quota_view(SH)["used_liters"], 810.0)
        self.assertEqual(svc.quota_view(date(2026, 9, 12))["used_liters"], 0.0)
        self.assertEqual(svc.quota_view(date(2026, 9, 12))["remaining_liters"], 18000.0)

    def test_daily_limit_never_exceeded(self):
        raw = json.loads((Path(__file__).parents[1] / "reference" / "domain.json").read_text("utf-8"))
        raw["daily_water_limit_liters"] = 1000.0
        svc, clock = ServiceTestBase().make_service(
            domain=domain_from_dict(raw), window_end="08:00")
        svc.ingest_reading(reading(
            "M", "GH-A-01", combine_local(SH, "05:00", svc.tz),
            combine_local(SH, "05:01", svc.tz), 0.20, "good"))
        plan = svc.generate_plan(SH, et0=0.0)
        svc.publish_plan(plan.plan_id)
        clock.t = combine_local(SH, "07:00", svc.tz)
        res = svc.dispatch_due()
        self.assertEqual(res["issued"], [])
        self.assertEqual(len(res["blocked"]), 1)  # 10mm*200=2000L > 1000L，被配额挡住
        self.assertEqual(svc.quota_view(SH)["used_liters"], 0.0)
        # 到当日窗口末尾仍无配额：剩余时间不足以完成，跳过而非超额放行
        clock.t = combine_local(SH, "08:01", svc.tz)
        res = svc.dispatch_due()
        self.assertIn(svc.slots[plan.slot_ids[0]].slot_id, res["skipped"])
        self.assertEqual(svc.quota_view(SH)["used_liters"], 0.0)


class ConcurrencyTest(unittest.TestCase):
    def _three_zone_domain(self) -> Domain:
        return Domain(
            timezone=TZ,
            daily_water_limit_liters=18000,
            zones=(
                Zone("Z-A", "番茄", "坐果期", "V-A"),
                Zone("Z-B", "彩椒", "开花期", "V-B"),
                Zone("Z-C", "黄瓜", "坐果期", "V-C"),
            ),
            valves=(
                Valve("V-A", 100, "L-A"),
                Valve("V-B", 90, "L-A"),
                Valve("V-C", 100, "L-B"),
            ),
            area_m2={"Z-A": 100.0, "Z-B": 160.0, "Z-C": 100.0},
            stages={
                "坐果期": StageCoefficients(kc=1.1, max_daily_liters_per_m2=None),
                "开花期": StageCoefficients(kc=0.95, max_daily_liters_per_m2=None),
            },
            formulas={"f": NutrientFormula("f", {"N": 100.0})},
            zone_formula={"Z-A": "f"},
        )

    def test_same_line_serial_other_line_parallel(self):
        svc, clock = ServiceTestBase().make_service(
            domain=self._three_zone_domain(), candidate_starts=("07:00",))
        t0 = combine_local(SH, "06:00", svc.tz)
        for z in ("Z-A", "Z-B", "Z-C"):
            svc.ingest_reading(reading(f"M-{z}", z, t0, t0 + timedelta(minutes=1), 0.20, "good"))
        plan = svc.generate_plan(SH, et0=0.0)
        slots = {s["zone_id"]: s for s in svc.plan_view(plan.plan_id)["slots"]}
        # Z-A 与 Z-C 异管线：07:00 同时开
        self.assertEqual(slots["Z-A"]["start"], slots["Z-C"]["start"])
        # Z-B 与 Z-A 共线：排在 Z-A 结束（15mm*100m2/100lpm=15min）之后
        self.assertEqual(slots["Z-B"]["start"], slots["Z-A"]["end"])

    def test_overrun_blocks_next_valve_then_releases(self):
        svc, clock = ServiceTestBase().make_service(
            domain=self._three_zone_domain(), candidate_starts=("07:00",))
        t0 = combine_local(SH, "06:00", svc.tz)
        for z in ("Z-A", "Z-B", "Z-C"):
            svc.ingest_reading(reading(f"M-{z}", z, t0, t0 + timedelta(minutes=1), 0.20, "good"))
        plan = svc.generate_plan(SH, et0=0.0)
        svc.publish_plan(plan.plan_id)

        clock.t = combine_local(SH, "07:00", svc.tz)
        issued = svc.dispatch_due()["issued"]
        self.assertEqual(len(issued), 2)  # L-A 的 Z-A 与 L-B 的 Z-C 并发
        cmd_a = next(c for c in issued if svc.commands[c].valve_id == "V-A")
        svc.record_receipt(Receipt("oa", cmd_a, "open", clock.t))

        # 计划 07:15 结束，但阀门一直开到 07:20：Z-B 到时必须排队
        clock.t = combine_local(SH, "07:15", svc.tz)
        res = svc.dispatch_due()
        self.assertEqual(res["issued"], [])
        self.assertTrue(any("shared_line_busy" in b["reason"] for b in res["blocked"]))

        # 关阀后下一节拍 Z-B 才签发；A 按实结算（超时多流的 5 分钟如实入账）
        clock.t = combine_local(SH, "07:20", svc.tz)
        svc.record_receipt(Receipt("ca", cmd_a, "close", clock.t, observed_lpm=100))
        res = svc.dispatch_due()
        self.assertEqual(len(res["issued"]), 1)
        self.assertEqual(svc.commands[res["issued"][0]].valve_id, "V-B")
        # A 实浇 2000L（07:00–07:20 @100lpm）；B 预占 15mm*160=2400L；
        # C 在 L-B 上始终 OPEN，仍占 15mm*100=1500L。
        self.assertEqual(svc.quota_view(SH)["used_liters"], 2000.0 + 2400.0 + 1500.0)


class SensorDegradationTest(unittest.TestCase):
    def test_offline_uses_fallback_but_respects_already_watered(self):
        svc, clock = ServiceTestBase().make_service(candidate_starts=("07:00", "11:00"))
        svc.ingest_reading(reading(
            "M1", "GH-A-01", combine_local(SH, "05:00", svc.tz),
            combine_local(SH, "05:01", svc.tz), None, "offline"))
        plan = svc.generate_plan(SH, et0=0.0)
        svc.publish_plan(plan.plan_id)
        view = svc.plan_view(plan.plan_id)
        slots = [s for s in view["slots"] if s["zone_id"] == "GH-A-01"]
        # 离线：fallback 3mm * 200m2 = 600L；11:00 时段因当日已浇 3mm 不再补水
        self.assertEqual(len(slots), 1)
        self.assertIn("sensor_offline", slots[0]["reason"]["triggers"])
        self.assertEqual(slots[0]["planned_liters"], 600.0)

    def test_stale_good_reading_is_treated_as_degraded(self):
        svc, clock = ServiceTestBase().make_service(
            candidate_starts=("07:00",), sensor_freshness_min=120)
        # 04:00 的“好”读数在 07:00 已陈旧（>120 分钟）
        svc.ingest_reading(reading(
            "M1", "GH-A-01", combine_local(SH, "04:00", svc.tz),
            combine_local(SH, "04:01", svc.tz), 0.10, "good"))
        plan = svc.generate_plan(SH, et0=0.0)
        slot = self.slot_of(svc, plan, "GH-A-01")
        self.assertIn("sensor_stale", slot["reason"]["triggers"])
        self.assertEqual(slot["planned_liters"], 3.0 * 200)  # fallback，而非 0.10 对应的大水量

    def test_suspect_reading_still_usable(self):
        svc, _ = ServiceTestBase().make_service(candidate_starts=("07:00",))
        svc.ingest_reading(reading(
            "M1", "GH-A-01", combine_local(SH, "06:30", svc.tz),
            combine_local(SH, "06:31", svc.tz), 0.21, "suspect"))
        plan = svc.generate_plan(SH, et0=0.0)
        slot = self.slot_of(svc, plan, "GH-A-01")
        self.assertIn("moisture_below_min", slot["reason"]["triggers"])
        self.assertEqual(slot["reason"]["sensor"]["quality"], "suspect")

    @staticmethod
    def slot_of(svc, plan, zone):
        return next(s for s in svc.plan_view(plan.plan_id)["slots"] if s["zone_id"] == zone)


class ReceiptTest(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = ServiceTestBase().make_service(candidate_starts=("07:00",))
        self.svc.ingest_reading(reading(
            "M1", "GH-A-01", combine_local(SH, "06:00", self.svc.tz),
            combine_local(SH, "06:01", self.svc.tz), 0.20, "good"))
        plan = self.svc.generate_plan(SH, et0=0.0)
        self.svc.publish_plan(plan.plan_id)
        self.clock.t = combine_local(SH, "07:00", self.svc.tz)
        self.cmd_id = self.svc.dispatch_due()["issued"][0]

    def test_duplicate_close_receipt_does_not_double_settle(self):
        self.svc.record_receipt(Receipt("R1", self.cmd_id, "open", self.clock.t))
        self.clock.advance(5)
        first = self.svc.record_receipt(
            Receipt("R2", self.cmd_id, "close", self.clock.t, observed_lpm=120))
        used = self.svc.quota_view(SH)["used_liters"]
        self.assertEqual(first["settled_liters"], 600.0)
        # 同一执据重发
        dup = self.svc.record_receipt(
            Receipt("R2", self.cmd_id, "close", self.clock.t + timedelta(hours=1), 120))
        self.assertEqual(dup["status"], "duplicate_receipt")
        # 又一条不同 id 的关阀执据：命令已终态，忽略
        again = self.svc.record_receipt(
            Receipt("R3", self.cmd_id, "close", self.clock.t + timedelta(hours=1), 120))
        self.assertEqual(again["status"], "ignored_terminal")
        self.assertEqual(self.svc.quota_view(SH)["used_liters"], used)
        cmd = self.svc.commands[self.cmd_id]
        self.assertEqual(cmd.state, CommandState.CLOSED)
        self.assertEqual(cmd.settled_liters, 600.0)

    def test_close_without_open_settles_zero(self):
        out = self.svc.record_receipt(Receipt("Rx", self.cmd_id, "close", self.clock.t, 120))
        self.assertEqual(out["settled_liters"], 0.0)
        self.assertEqual(self.svc.quota_view(SH)["used_liters"], 0.0)  # 预占全额释放

    def test_lost_command_releases_reservation(self):
        # 从无回执：超过 ack 超时（默认 10 分钟）判失联
        self.clock.advance(11)
        res = self.svc.reconcile()
        self.assertEqual(res["lost"], [self.cmd_id])
        self.assertEqual(self.svc.quota_view(SH)["used_liters"], 0.0)
        slot = self.svc.slots[self.svc.commands[self.cmd_id].slot_id]
        self.assertEqual(slot.status, SlotStatus.CANCELLED)
        # 迟到的关阀执据不得再扣水
        out = self.svc.record_receipt(Receipt("late", self.cmd_id, "close", self.clock.t, 120))
        self.assertEqual(out["status"], "ignored_terminal")
        self.assertEqual(self.svc.quota_view(SH)["used_liters"], 0.0)


class ManualStopTest(unittest.TestCase):
    def test_manual_stop_preempts_and_hold_blocks_redispatch(self):
        raw = json.loads((Path(__file__).parents[1] / "reference" / "domain.json").read_text("utf-8"))
        raw["stages"] = {"坐果期": {"kc": 1.1, "max_daily_liters_per_m2": 40.0}}
        svc, clock = ServiceTestBase().make_service(
            domain=domain_from_dict(raw),
            candidate_starts=("07:00", "11:00"), window_end="12:00")
        svc.ingest_reading(reading(
            "M1", "GH-A-01", combine_local(SH, "06:00", svc.tz),
            combine_local(SH, "06:01", svc.tz), None, "offline"))
        # 另一区湿润且读数新鲜，不参与当日灌溉，避免占用共线 L-A
        svc.ingest_reading(reading(
            "M2", "GH-A-02", combine_local(SH, "06:00", svc.tz),
            combine_local(SH, "06:01", svc.tz), 0.40, "good"))
        plan = svc.generate_plan(SH, et0=0.0)
        # 离线维持量 14mm：07:00 排 12mm（单次封顶），11:00 再排 2mm
        svc.update_thresholds(
            plan.plan_id, {"GH-A-01": {"offline_fallback_et_mm": 14.0}})
        svc.publish_plan(plan.plan_id)
        clock.t = combine_local(SH, "07:00", svc.tz)
        cmd_id = svc.dispatch_due()["issued"][0]
        self.assertEqual(svc.commands[cmd_id].intended_liters, 12 * 200)
        svc.record_receipt(Receipt("o", cmd_id, "open", clock.t))
        clock.advance(2)  # 已浇 2*120=240L

        out = svc.manual_stop(valve_id="V-01", reason="畦面积水")
        self.assertEqual(out["stopped"][0]["settled_liters"], 240.0)
        cmd = svc.commands[cmd_id]
        self.assertEqual(cmd.state, CommandState.MANUAL_STOP)
        # 人工接管期间，后续时段到时也不得自动开该阀
        clock.t = combine_local(SH, "11:00", svc.tz)
        res = svc.dispatch_due()
        v01_blocked = [b for b in res["blocked"]
                       if svc.slots[b["slot_id"]].zone_id == "GH-A-01"]
        self.assertEqual(len(v01_blocked), 1)
        self.assertEqual(v01_blocked[0]["reason"], "valve_manual_hold")
        # 恢复自动后才允许签发
        svc.resume_auto("V-01")
        res = svc.dispatch_due()
        self.assertTrue(any(svc.commands[i].zone_id == "GH-A-01" for i in res["issued"]))


class RecoveryTest(unittest.TestCase):
    def test_replay_continues_same_plan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            store = EventStore(path)
            svc, clock = ServiceTestBase().make_service(store=store, candidate_starts=("07:00",))
            svc.ingest_reading(reading(
                "M1", "GH-A-01", combine_local(SH, "06:00", svc.tz),
                combine_local(SH, "06:01", svc.tz), 0.20, "good"))
            plan = svc.generate_plan(SH, et0=0.0)
            svc.publish_plan(plan.plan_id)
            clock.t = combine_local(SH, "07:00", svc.tz)
            cmd_id = svc.dispatch_due()["issued"][0]
            svc.record_receipt(Receipt("o1", cmd_id, "open", clock.t))
            before = svc.status()

            # 重启：新服务实例重放同一日志（注入同一固定时钟，保证“今天”一致）
            domain = load_domain(Path(__file__).parents[1] / "reference" / "domain.json")
            svc2 = IrrigationService(
                domain, EventStore(path), clock=clock, candidate_starts=("07:00",))
            after = svc2.status()
            self.assertEqual(
                json.dumps(before["quota"], sort_keys=True),
                json.dumps(after["quota"], sort_keys=True))
            self.assertEqual(
                json.dumps(before["plans"], sort_keys=True, ensure_ascii=False),
                json.dumps(after["plans"], sort_keys=True, ensure_ascii=False))
            self.assertEqual(len(after["pending_commands"]), 1)
            # 执据去重集合也恢复：同一执据不会因重启二次结算
            dup = svc2.record_receipt(Receipt("o1", cmd_id, "open", clock.t))
            self.assertEqual(dup["status"], "duplicate_receipt")
            # 可继续完成该命令
            out = svc2.record_receipt(Receipt("c1", cmd_id, "close", clock.t + timedelta(minutes=5), 120))
            self.assertEqual(out["settled_liters"], 600.0)


class DecisionTraceTest(unittest.TestCase):
    def test_reason_carries_full_trace(self):
        domain = load_domain(Path(__file__).parents[1] / "reference" / "domain.json")
        eng = EtEstimator(domain)
        from irrigation.models import Reading
        from datetime import datetime
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(TZ)
        reading = Reading("M1", "GH-A-01", datetime(2026, 9, 11, 6, 0, tzinfo=tz),
                          datetime(2026, 9, 11, 6, 1, tzinfo=tz), 0.23, Quality.SUSPECT)
        dec = eng.decide(
            domain.zone("GH-A-01"), ZoneContext(latest_reading=reading, et0_mm=4.0),
            at=datetime(2026, 9, 11, 7, 0, tzinfo=tz))
        self.assertTrue(dec.irrigate)
        self.assertEqual(dec.reason["et"]["kc"], 1.10)
        self.assertEqual(dec.reason["formula"], "tomato-fruiting")
        self.assertIn("N", dec.reason["nutrients_grams"])
        self.assertEqual(dec.reason["sensor"]["event_id"], "M1")


if __name__ == "__main__":
    unittest.main()


class ConcurrencyRaceTest(unittest.TestCase):
    def test_parallel_close_receipts_settle_once(self):
        import threading
        svc, clock = ServiceTestBase().make_service(candidate_starts=("07:00",))
        svc.ingest_reading(reading(
            "M1", "GH-A-01", combine_local(SH, "06:00", svc.tz),
            combine_local(SH, "06:01", svc.tz), 0.20, "good"))
        plan = svc.generate_plan(SH, et0=0.0)
        svc.publish_plan(plan.plan_id)
        clock.t = combine_local(SH, "07:00", svc.tz)
        cmd_id = svc.dispatch_due()["issued"][0]
        svc.record_receipt(Receipt("open", cmd_id, "open", clock.t))
        clock.advance(5)

        results = []

        def close_k(i):
            # 每个线程执据 id 不同，但命令只能结算一次
            out = svc.record_receipt(Receipt(f"c-{i}", cmd_id, "close", clock.t, 120))
            results.append(out)

        threads = [threading.Thread(target=close_k, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        closed = [r for r in results if r.get("status") == "closed"]
        ignored = [r for r in results if r.get("status") == "ignored_terminal"]
        self.assertEqual(len(closed), 1)
        self.assertEqual(len(ignored), 19)
        self.assertEqual(svc.quota_view(SH)["used_liters"], 600.0)

    def test_parallel_reservations_never_exceed_limit(self):
        import threading
        from irrigation.quota import QuotaLedger
        domain = load_domain(Path(__file__).parents[1] / "reference" / "domain.json")
        ledger = QuotaLedger(domain)
        t0 = combine_local(SH, "08:00", ledger.tz)
        errors = []

        def reserve(i):
            try:
                ledger.reserve(t0, f"c-{i}", 1000.0)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=reserve, args=(i,)) for i in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 18000 限额下恰好 18 条成功，其余配额不足；已用绝不超限
        self.assertLessEqual(ledger.used(SH), 18000.0 + 1e-9)
        self.assertEqual(ledger.used(SH), 18000.0)
        self.assertEqual(len(errors), 12)


class QueuedSlotOverrunTest(unittest.TestCase):
    def test_queued_slot_dispatches_even_after_scheduled_end(self):
        """同线伙伴占用超过排队时段的计划 end，但当日窗口仍够完成：顺延签发而非跳过。"""
        t = ConcurrencyTest()
        svc, clock = ServiceTestBase().make_service(
            domain=t._three_zone_domain(), candidate_starts=("07:00",), window_end="20:00")
        t0 = combine_local(SH, "06:00", svc.tz)
        for z in ("Z-A", "Z-B"):
            svc.ingest_reading(reading(f"M-{z}", z, t0, t0 + timedelta(minutes=1), 0.20, "good"))
        plan = svc.generate_plan(SH, et0=0.0)
        slots = {s["zone_id"]: s for s in svc.plan_view(plan.plan_id)["slots"]}
        svc.publish_plan(plan.plan_id)

        clock.t = combine_local(SH, "07:00", svc.tz)
        cmd_a = svc.dispatch_due()["issued"][0]
        svc.record_receipt(Receipt("oa", cmd_a, "open", clock.t))

        # Z-B 计划 07:15 起、约 26.7 分钟（end≈07:41:40）。A 一直开到 07:50（超过该 end）。
        clock.t = combine_local(SH, "07:15", svc.tz)
        self.assertEqual(svc.dispatch_due()["issued"], [])
        clock.t = combine_local(SH, "07:50", svc.tz)
        svc.record_receipt(Receipt("ca", cmd_a, "close", clock.t, observed_lpm=100))
        res = svc.dispatch_due()
        self.assertEqual(len(res["issued"]), 1)
        b_cmd = svc.commands[res["issued"][0]]
        self.assertEqual(b_cmd.valve_id, "V-B")
        b_slot = svc.slots[b_cmd.slot_id]
        # 实际窗口顺延到 07:50，历时不变
        self.assertEqual(b_slot.start, clock.t)
        self.assertGreater(b_slot.end, combine_local(SH, "07:50", svc.tz))
