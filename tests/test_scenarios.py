import json
import os
import unittest
from datetime import datetime
from pathlib import Path

from irrigation.config import DomainConfig
from irrigation.store import Store
from irrigation.service import IrrigationService

REF = Path(__file__).parents[1] / "reference" / "domain.json"
TZ = "+08:00"
DAY = "2026-09-11"


class Gateway:
    """记录全部投递；网关侧以 command_uid 去重，模拟真实 PLC。"""

    def __init__(self):
        self.sent = []
        self.seen = set()
        self.fail = False

    def send(self, command):
        if command["command_uid"] in self.seen:
            return
        self.seen.add(command["command_uid"])
        if self.fail:
            raise ConnectionError("gateway offline")
        self.sent.append(command["command_uid"])


def make_service(db=":memory:", clock="2026-09-11T07:00:00+08:00"):
    cfg = DomainConfig.load(REF)
    store = Store(db)
    gw = Gateway()
    svc = IrrigationService(store, cfg, gw,
                            clock=lambda: datetime.fromisoformat(clock))
    return svc, store, cfg, gw


def at(s):
    return datetime.fromisoformat(s)


def telemetry(eid, zone, occ, recv, m, q="good"):
    return {"event_id": eid, "zone_id": zone, "occurred_at": occ + TZ,
            "received_at": recv + TZ, "moisture": m, "quality": q}


def dry_both(svc, m=0.20):
    svc.ingest_telemetry([
        telemetry("M-1", "GH-A-01", f"{DAY}T05:30", f"{DAY}T05:31", m),
        telemetry("M-2", "GH-A-02", f"{DAY}T05:30", f"{DAY}T05:31", m),
    ])


def publish_day(svc, day=DAY, et0=2.0, windows=None, thresholds=None, dry=True,
                moisture=0.20):
    if dry:
        dry_both(svc, m=moisture)
    svc.build_plan(day, et0=et0, windows=windows, thresholds=thresholds,
                   persist=True)
    return svc.publish_plan(day_str=day)


class TelemetryFusionTest(unittest.TestCase):
    def test_out_of_order_old_reading_never_overrides_newer(self):
        svc, store, cfg, gw = make_service()
        svc.ingest_telemetry([
            telemetry("M-2", "GH-A-01", f"{DAY}T05:58", f"{DAY}T06:00", 0.31),
            telemetry("M-1", "GH-A-01", f"{DAY}T05:30", f"{DAY}T06:05", 0.20, "suspect"),
        ])
        plan = publish_day(svc, et0=1.0, dry=False)
        z = next(z for z in plan["zones"] if z["zone_id"] == "GH-A-01")
        self.assertEqual(z["quality"], "good")
        self.assertAlmostEqual(z["moisture"], 0.31)
        self.assertEqual(z["demand_l"], 0.0)

    def test_suspect_blended_with_prior_good_and_never_overstates(self):
        svc, store, cfg, gw = make_service()
        svc.ingest_telemetry([
            telemetry("M-1", "GH-A-01", f"{DAY}T05:30", f"{DAY}T05:31", 0.30),
            telemetry("M-2", "GH-A-01", f"{DAY}T05:58", f"{DAY}T06:05", 0.20, "suspect"),
        ])
        plan = publish_day(svc, et0=1.0, dry=False)
        z = next(z for z in plan["zones"] if z["zone_id"] == "GH-A-01")
        self.assertAlmostEqual(z["moisture"], (0.3 * 0.20 + 0.30) / 1.3, places=6)
        self.assertEqual(z["quality"], "suspect")

    def test_offline_zone_falls_back_to_water_balance(self):
        svc, store, cfg, gw = make_service()
        # 19 小时前的 good 旱读数已超期 → 传感器降级 offline；
        # 水量平衡以该读数锚定的亏缺仍应报需水
        svc.ingest_telemetry([
            telemetry("M-OLD", "GH-A-02", "2026-09-10T12:00", "2026-09-10T12:05", 0.25),
        ])
        plan = publish_day(svc, et0=9.0, dry=False)
        z = next(z for z in plan["zones"] if z["zone_id"] == "GH-A-02")
        self.assertEqual(z["quality"], "offline")
        self.assertGreater(z["demand_l"], 0)
        self.assertIn("水量平衡", " ".join(z["reason"]["reasons"]))


class QuotaAndConcurrencyTest(unittest.TestCase):
    def test_daily_quota_never_exceeded(self):
        svc, store, cfg, gw = make_service()
        plan = publish_day(svc, et0=9.0)
        planned_total = sum(
            sum(s["liters"] for s in e["day_split"] if s["day"] == DAY)
            for e in plan["entries"]
        )
        self.assertLessEqual(planned_total, cfg.daily_water_limit_liters + 1e-6)
        self.assertAlmostEqual(store.committed(DAY), planned_total, places=3)
        self.assertTrue(any(z["unscheduled_l"] > 0 for z in plan["zones"]))

    def test_shared_line_valves_never_overlap(self):
        svc, store, cfg, gw = make_service()
        plan = publish_day(svc, et0=2.0, moisture=0.27)
        intervals = sorted(
            (datetime.fromisoformat(e["start_at"]), datetime.fromisoformat(e["end_at"]))
            for e in plan["entries"]
        )
        self.assertGreater(len(intervals), 1)
        for (s1, e1), (s2, e2) in zip(intervals, intervals[1:]):
            self.assertLessEqual(e1, s2, f"共线阀门时段重叠: {s1,e1} vs {s2,e2}")

    def test_preview_does_not_consume_quota(self):
        svc, store, cfg, gw = make_service()
        svc.build_plan(DAY, et0=9.0, persist=False)
        self.assertEqual(store.committed(DAY), 0.0)
        self.assertIsNone(store.get_active_plan(DAY))


class CommandIdempotencyTest(unittest.TestCase):
    def _run_one_entry(self, svc):
        plan = publish_day(svc, et0=2.0)
        entry = plan["entries"][0]
        svc.tick(at(f"{DAY}T07:30:00+08:00"))
        return entry

    def test_duplicate_close_ack_settles_once(self):
        svc, store, cfg, gw = make_service()
        entry = self._run_one_entry(svc)
        eid = entry["id"]
        svc.ack_command(f"cmd:{eid}:open", True, {}, at(f"{DAY}T07:30:05+08:00"))
        out = svc.tick(at(f"{DAY}T10:30:00+08:00"))
        self.assertTrue(any(a["action"] == "close" for a in out["actions"]))
        measured = entry["planned_l"]
        r1 = svc.ack_command(f"cmd:{eid}:close", True, {"liters": measured},
                             at(f"{DAY}T10:30:06+08:00"))
        r2 = svc.ack_command(f"cmd:{eid}:close", True, {"liters": measured * 2},
                             at(f"{DAY}T10:30:09+08:00"))
        r3 = svc.ack_command(f"cmd:{eid}:close", True, {"liters": measured * 3},
                             at(f"{DAY}T10:30:12+08:00"))
        self.assertFalse(r1["duplicate"])
        self.assertTrue(r2["duplicate"])
        self.assertTrue(r3["duplicate"])
        self.assertAlmostEqual(store.committed(DAY), measured, places=3)
        e = store.get_entry(eid)
        self.assertEqual(e["status"], "confirmed")
        self.assertAlmostEqual(e["actual_l"], measured, places=3)

    def test_gateway_loss_then_reconcile_resends_same_uid(self):
        svc, store, cfg, gw = make_service()
        plan = publish_day(svc, et0=2.0)
        entry = plan["entries"][0]
        gw.fail = True
        out = svc.tick(at(f"{DAY}T07:30:00+08:00"))
        self.assertTrue(any(not a.get("sent", True) for a in out["actions"]))
        n_reserve = len([r for r in store.ledger_rows(DAY)
                         if r["ref_kind"] == "reserve"])
        gw.fail = False
        rec = svc.reconcile(at(f"{DAY}T07:30:10+08:00"))
        resent = [a["command_uid"] for a in rec["actions"] if a.get("sent")]
        self.assertIn(f"cmd:{entry['id']}:open", resent)
        self.assertEqual(
            len([r for r in store.ledger_rows(DAY) if r["ref_kind"] == "reserve"]),
            n_reserve,
        )


class ManualPauseTest(unittest.TestCase):
    def test_pause_closes_open_valves_and_blocks_auto(self):
        windows = [{"start": "07:30", "end": "08:30"},
                   {"start": "15:30", "end": "18:30"}]
        svc, store, cfg, gw = make_service()
        plan = publish_day(svc, et0=2.0, windows=windows)
        first = plan["entries"][0]
        svc.tick(at(f"{DAY}T07:30:00+08:00"))
        svc.ack_command(f"cmd:{first['id']}:open", True, {},
                        at(f"{DAY}T07:30:02+08:00"))
        out = svc.pause(reason="爆管", now=at(f"{DAY}T07:35:00+08:00"))
        close_actions = [a for a in out["actions"] if a["action"] == "close"]
        self.assertEqual(len(close_actions), 1)
        svc.ack_command(f"cmd:{first['id']}:close", True, {"liters": 600.0},
                        at(f"{DAY}T07:35:05+08:00"))
        # 全局停灌期间任何 tick 不得自动开阀；未执行时段到窗口结束自动退额
        for t in ("08:00:00", "09:00:00", "16:00:00", "18:31:00"):
            later = svc.tick(at(f"{DAY}T{t}+08:00"))
            self.assertFalse(any(a["action"] == "open" for a in later["actions"]),
                             f"停灌期间于 {t} 出现自动开阀")
        self.assertTrue(store.is_paused(first["zone_id"]))
        # 已结算净额只等于人工停止时的实测水量（其余时段全部退额）
        self.assertAlmostEqual(store.committed(DAY), 600.0, places=3)
        svc.resume(now=at(f"{DAY}T18:35:00+08:00"))
        again = svc.tick(at(f"{DAY}T18:35:30+08:00"))
        self.assertFalse(any(a["action"] == "open" for a in again["actions"]))


class CrossMidnightTest(unittest.TestCase):
    def test_entry_crossing_midnight_splits_and_totals_match(self):
        windows = [{"start": "23:00", "end": "01:00"}]
        svc, store, cfg, gw = make_service()
        svc.ingest_telemetry([
            telemetry("M-1", "GH-A-01", f"{DAY}T05:30", f"{DAY}T05:31", 0.20),
        ])
        svc.build_plan(DAY, et0=1.0, windows=windows, persist=True)
        plan = svc.publish_plan(day_str=DAY)
        cross = [e for e in plan["entries"]
                 if len({s["day"] for s in e["day_split"]}) == 2]
        self.assertTrue(cross, "应当排出跨午夜时段")
        e = cross[0]
        self.assertAlmostEqual(
            sum(s["liters"] for s in e["day_split"]), e["planned_l"], places=3
        )
        svc.tick(at(f"{DAY}T23:00:00+08:00"))
        svc.ack_command(f"cmd:{e['id']}:open", True, {},
                        at(f"{DAY}T23:00:10+08:00"))
        # 到点（次日 01:00）关阀并按实测结算
        svc.tick(at("2026-09-12T01:00:00+08:00"))
        actual = e["planned_l"]
        svc.ack_command(f"cmd:{e['id']}:close", True, {"liters": actual},
                        at("2026-09-12T01:00:08+08:00"))
        row = store.get_entry(e["id"])
        split = json.loads(row["actual_split_json"])
        self.assertAlmostEqual(sum(s["liters"] for s in split), actual, places=3)
        d11 = sum(s["liters"] for s in split if s["day"] == DAY)
        d12 = sum(s["liters"] for s in split if s["day"] == "2026-09-12")
        self.assertAlmostEqual(store.committed(DAY), d11, places=3)
        self.assertAlmostEqual(store.committed("2026-09-12"), d12, places=3)
        self.assertLessEqual(store.committed(DAY), cfg.daily_water_limit_liters + 1e-6)
        self.assertLessEqual(store.committed("2026-09-12"),
                             cfg.daily_water_limit_liters + 1e-6)


class LateReadingTest(unittest.TestCase):
    def test_late_reading_only_affects_future_entries(self):
        windows = [{"start": "07:30", "end": "08:30"},
                   {"start": "15:30", "end": "18:30"}]
        svc, store, cfg, gw = make_service()
        svc.ingest_telemetry([
            telemetry("M-1", "GH-A-01", f"{DAY}T05:30", f"{DAY}T05:31", 0.27),
            telemetry("M-2", "GH-A-02", f"{DAY}T05:30", f"{DAY}T05:31", 0.27),
        ])
        svc.build_plan(DAY, et0=2.0, windows=windows, persist=True)
        plan = svc.publish_plan(day_str=DAY)
        first = next(e for e in plan["entries"] if e["zone_id"] == "GH-A-01")
        afternoon_before = [
            e for e in plan["entries"]
            if e["zone_id"] == "GH-A-01" and e["start_at"].endswith("07:30:00+00:00")
        ]
        # 早晨时段执行并结算
        svc.tick(at(f"{DAY}T07:30:00+08:00"))
        svc.ack_command(f"cmd:{first['id']}:open", True, {},
                        at(f"{DAY}T07:30:02+08:00"))
        svc.tick(at(f"{DAY}T09:00:00+08:00"))  # 早已过结束点，触发关阀
        svc.ack_command(f"cmd:{first['id']}:close", True,
                        {"liters": first["planned_l"]},
                        at(f"{DAY}T09:00:05+08:00"))
        committed_before = store.committed(DAY)
        confirmed_l = store.get_entry(first["id"])["actual_l"]
        # 该分区尚有下午待执行时段
        pending_afternoon = [
            e for e in store.entries_of_plan(plan["id"])
            if e["zone_id"] == "GH-A-01" and e["status"] == "pending"
        ]
        self.assertTrue(pending_afternoon)

        # 迟到的“已浇透”读数（业务时间在灌溉后，接收更晚）
        result = svc.ingest_telemetry([
            telemetry("M-9", "GH-A-01", f"{DAY}T10:35", f"{DAY}T11:30", 0.35),
        ], now=at(f"{DAY}T11:30:00+08:00"))
        adj = result["adjustments"][0]
        self.assertNotIn(first["id"], adj["cancelled"])
        self.assertGreaterEqual(
            {x["id"] if isinstance(x, dict) else x for x in adj["cancelled"]}
            if adj["cancelled"] and isinstance(adj["cancelled"][0], dict)
            else set(adj["cancelled"]),
            {e["id"] for e in pending_afternoon},
        )
        # 已结算水量不被重算/重扣
        self.assertAlmostEqual(store.get_entry(first["id"])["actual_l"],
                               confirmed_l, places=6)
        self.assertLessEqual(store.committed(DAY), committed_before + 1e-6)
        # 墒情已高于回补目标 → 重排后不再安排
        pz = store.plan_zone_row(plan["id"], "GH-A-01")
        self.assertEqual(pz["scheduled_l"], 0.0)

    def test_pre_irrigation_reading_excluded_after_watering(self):
        svc, store, cfg, gw = make_service()
        plan = publish_day(svc, et0=2.0)
        first = plan["entries"][0]
        svc.tick(at(f"{DAY}T07:30:00+08:00"))
        svc.ack_command(f"cmd:{first['id']}:open", True, {},
                        at(f"{DAY}T07:30:02+08:00"))
        svc.tick(at(f"{DAY}T10:30:00+08:00"))
        svc.ack_command(f"cmd:{first['id']}:close", True,
                        {"liters": first["planned_l"]},
                        at(f"{DAY}T10:30:05+08:00"))
        svc.ingest_telemetry([
            telemetry("M-OLD", first["zone_id"], f"{DAY}T06:00", f"{DAY}T11:00", 0.18),
        ], now=at(f"{DAY}T11:00:00+08:00"))
        view = svc.build_plan(DAY, et0=2.0, persist=True)
        z = next(z for z in view["zones"] if z["zone_id"] == first["zone_id"])
        excluded_ids = [x["event_id"] for x in z["reason"].get("sensor_excluded", [])]
        self.assertIn("M-OLD", excluded_ids)


class DegradationAndRevisionTest(unittest.TestCase):
    def test_sensor_goes_offline_midday_uses_balance_without_extra_water(self):
        svc, store, cfg, gw = make_service()
        svc.ingest_telemetry([
            telemetry("M-1", "GH-A-01", f"{DAY}T05:30", f"{DAY}T05:31", 0.22),
        ])
        plan = publish_day(svc, et0=2.0, dry=False)
        first = plan["entries"][0]
        svc.tick(at(f"{DAY}T07:30:00+08:00"))
        svc.ack_command(f"cmd:{first['id']}:open", True, {},
                        at(f"{DAY}T07:30:02+08:00"))
        svc.tick(at(f"{DAY}T10:30:00+08:00"))
        svc.ack_command(f"cmd:{first['id']}:close", True,
                        {"liters": first["planned_l"]},
                        at(f"{DAY}T10:30:05+08:00"))
        committed = store.committed(DAY)
        svc.ingest_telemetry([
            telemetry("M-OFF", "GH-A-01", f"{DAY}T11:00", f"{DAY}T11:00", 0.0,
                      "offline"),
        ], now=at(f"{DAY}T11:00:00+08:00"))
        self.assertLessEqual(store.committed(DAY), committed + 1e-6)

    def test_revision_keeps_started_entries_and_releases_cancelled(self):
        svc, store, cfg, gw = make_service()
        plan = publish_day(svc, et0=2.0)
        first = plan["entries"][0]
        svc.tick(at(f"{DAY}T07:30:00+08:00"))
        svc.ack_command(f"cmd:{first['id']}:open", True, {},
                        at(f"{DAY}T07:30:02+08:00"))
        rev = svc.revise_plan(DAY, et0=2.0,
                              thresholds={"GH-A-01": 0.35, "GH-A-02": 0.35},
                              now=at(f"{DAY}T07:31:00+08:00"))
        moved = store.get_entry(first["id"])
        self.assertEqual(moved["plan_id"], rev["id"])
        self.assertEqual(moved["status"], "dispatched")
        self.assertEqual(store.get_plan(plan["id"])["status"], "superseded")
        # 修订过程水量守恒：已在执行时段的预留仍在，无重复预留行
        reserves = [r for r in store.ledger_rows(DAY) if r["ref_kind"] == "reserve"
                    and r["ref_key"] == first["id"]]
        self.assertEqual(len(reserves), 1)


class PersistenceTest(unittest.TestCase):
    def test_restart_resumes_same_plan_and_settles(self):
        path = "/tmp/irr_test_restart.db"
        if os.path.exists(path):
            os.remove(path)
        svc, store, cfg, gw = make_service(db=path)
        plan = publish_day(svc, et0=2.0)
        eid = plan["entries"][0]["id"]
        svc.tick(at(f"{DAY}T07:30:00+08:00"))
        store.commit()
        store.close()

        svc2, store2, cfg2, gw2 = make_service(db=path, clock=f"{DAY}T07:31:00+08:00")
        rec = svc2.reconcile(at(f"{DAY}T07:31:00+08:00"))
        self.assertTrue(any(a["command_uid"] == f"cmd:{eid}:open"
                            for a in rec["actions"]))
        self.assertIsNotNone(store2.get_active_plan(DAY))
        svc2.ack_command(f"cmd:{eid}:open", True, {},
                         at(f"{DAY}T07:31:05+08:00"))
        svc2.tick(at(f"{DAY}T10:30:00+08:00"))
        svc2.ack_command(f"cmd:{eid}:close", True, {"liters": 600.0},
                         at(f"{DAY}T10:30:06+08:00"))
        self.assertAlmostEqual(store2.committed(DAY), 600.0, places=3)
        store2.close()
        os.remove(path)


if __name__ == "__main__":
    unittest.main()
