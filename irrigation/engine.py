"""纯函数农艺引擎：墒情融合、蒸散水量平衡、剂量与排程。

本模块不接触数据库，便于单测与“预演”：同样的输入必然得到同样的计划。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from . import timeutil
from .config import QUALITY_POLICY, Zone

GOOD_FRESHNESS = timedelta(hours=12)
SUSPECT_FRESHNESS = timedelta(hours=6)
# 单条时段最小整分钟数，低于此值不再开阀
MIN_RUN_MINUTES = 1


@dataclass
class FusedMoisture:
    moisture: float | None
    quality: str  # good / suspect / offline
    sources: list[str]
    excluded: list[dict]
    detail: dict


def fuse_readings(
    rows: list,
    *,
    as_of: datetime,
    wet_until: datetime | None = None,
) -> FusedMoisture:
    """按“业务发生时间”融合墒情。

    关键规则：
    - 只看 occurred_at，不看 received_at：晚到的旧读数不能覆盖新读数；
    - wet_until 为最近一次灌溉的结束/结算时刻（灌溉进行中取计划结束时刻）：
      occurred_at 早于该时刻的读数描述的是灌溉前或入渗中的状态，一律排除，
      这是“已浇畦面被再次灌水”的主要防线；
    - good 直接采用；suspect 与最近一条 good 做收缩加权，无 good 佐证时降级标注；
    - offline / 超期 / 无有效读数 → offline，转入蒸散水量平衡后备方案。
    """
    usable: list = []
    excluded: list[dict] = []
    for r in rows:
        occ = timeutil.parse(r["occurred_at"])
        reason = None
        if occ > as_of:
            reason = "future"
        elif wet_until is not None and occ < wet_until:
            reason = "pre_or_during_irrigation"
        else:
            policy = QUALITY_POLICY.get(r["quality"], QUALITY_POLICY["offline"])
            if not policy["usable"]:
                reason = "quality_offline"
        if reason:
            excluded.append({"event_id": r["event_id"], "reason": reason})
        else:
            usable.append(r)

    usable.sort(key=lambda r: timeutil.parse(r["occurred_at"]), reverse=True)
    newest_good = next(
        (r for r in usable
         if r["quality"] == "good"
         and as_of - timeutil.parse(r["occurred_at"]) <= GOOD_FRESHNESS),
        None,
    )
    newest_suspect = next(
        (r for r in usable
         if r["quality"] == "suspect"
         and as_of - timeutil.parse(r["occurred_at"]) <= SUSPECT_FRESHNESS),
        None,
    )

    if newest_suspect and (newest_good is None
                           or timeutil.parse(newest_suspect["occurred_at"])
                           > timeutil.parse(newest_good["occurred_at"])):
        s = newest_suspect
        ws = QUALITY_POLICY["suspect"]["weight"]
        if newest_good is not None:
            wg = QUALITY_POLICY["good"]["weight"]
            moisture = (ws * float(s["moisture"]) + wg * float(newest_good["moisture"])) / (ws + wg)
            sources = [s["event_id"], newest_good["event_id"]]
            detail = {"suspect": s["moisture"], "prior_good": newest_good["moisture"]}
        else:
            # 无 good 佐证的 suspect：采用但明确标注，计划只敢走水量平衡口径
            moisture = float(s["moisture"])
            sources = [s["event_id"]]
            detail = {"suspect": s["moisture"], "prior_good": None}
        return FusedMoisture(moisture, "suspect", sources, excluded, detail)

    if newest_good is not None:
        return FusedMoisture(
            float(newest_good["moisture"]), "good", [newest_good["event_id"]], excluded,
            {"based_on": "latest_good"},
        )

    # 有读数但全部超期
    stale_ids = [r["event_id"] for r in usable]
    return FusedMoisture(
        None, "offline", [], excluded,
        {"stale": stale_ids, "fallback": "water_balance"},
    )


def effective_area_m2(zone: Zone) -> float:
    return zone.soil["crop_area_m2"] * zone.soil["wetting_ratio"]


def taw_mm(zone: Zone) -> float:
    """有效根区持水量 TAW = (田持 − 萎蔫点) × 根深。"""
    return (zone.soil["field_capacity"] - zone.soil["wilting_point"]) * zone.soil["root_depth_mm"]


def assess_demand(
    zone: Zone,
    *,
    et0_mm: float,
    fused: FusedMoisture,
    stored_depletion_mm: float,
    threshold_override: float | None = None,
    target_override: float | None = None,
) -> dict:
    """计算单日净需水量（升），返回判定明细，保证可追溯。"""
    area = effective_area_m2(zone)
    kc = zone.profile["kc"]
    etc_mm = kc * float(et0_mm)
    projected_depletion = max(0.0, stored_depletion_mm + etc_mm)
    mad = zone.profile["dep_max"] * taw_mm(zone)

    # 水量平衡口径：预计耗水越过允许亏缺线（MAD）则回补预计亏缺量
    wb_need_l = projected_depletion * area if projected_depletion >= mad else 0.0

    threshold = (
        float(threshold_override)
        if threshold_override is not None
        else float(zone.soil["moisture_threshold"])
    )
    target = (
        float(target_override)
        if target_override is not None
        else float(zone.soil["moisture_target"])
    )

    sensor_deficit_l = 0.0
    if fused.quality in ("good", "suspect") and fused.moisture is not None:
        if fused.moisture < threshold:
            sensor_deficit_l = (
                (target - fused.moisture) * zone.soil["root_depth_mm"] * area
            )

    if fused.quality == "good":
        demand_l = max(sensor_deficit_l, wb_need_l)
        basis = "moisture_and_balance"
    elif fused.quality == "suspect":
        # 疑似读数不可单方加大剂量：以水量平衡为下限，墒情口径需显著更高才采纳
        demand_l = max(wb_need_l, sensor_deficit_l if sensor_deficit_l > wb_need_l * 1.10 else 0.0)
        basis = "suspect_moisture_balanced"
    else:
        demand_l = wb_need_l
        basis = "water_balance_fallback"

    reasons = []
    if fused.quality == "offline":
        reasons.append("传感器不可用，按蒸散水量平衡估算")
    elif fused.quality == "suspect":
        reasons.append("最新墒情为 suspect，已与最近 good 读数收缩融合")
    if sensor_deficit_l > 0:
        reasons.append(
            f"墒情 {fused.moisture:.3f} 低于阈值 {threshold:.3f}，"
            f"回补目标 {target:.3f}"
        )
    if wb_need_l > 0:
        reasons.append(
            f"预计耗水 {projected_depletion:.1f}mm 已达允许亏缺线 {mad:.1f}mm"
            f"（kc={kc}，ET0={et0_mm:.1f}mm）"
        )
    if not reasons:
        reasons.append("墒情充足且预计耗水未越线，无需灌溉")

    return {
        "demand_l": round(max(0.0, demand_l), 3),
        "basis": basis,
        "threshold": threshold,
        "target": target,
        "moisture": fused.moisture,
        "quality": fused.quality,
        "sensor_sources": fused.sources,
        "sensor_excluded": fused.excluded,
        "sensor_detail": fused.detail,
        "kc": kc,
        "et0_mm": float(et0_mm),
        "etc_mm": round(etc_mm, 3),
        "taw_mm": round(taw_mm(zone), 3),
        "mad_mm": round(mad, 3),
        "stored_depletion_mm": round(stored_depletion_mm, 3),
        "projected_depletion_mm": round(projected_depletion, 3),
        "sensor_deficit_l": round(sensor_deficit_l, 3),
        "wb_need_l": round(wb_need_l, 3),
        "reasons": reasons,
        "projected_depletion_raw": projected_depletion,
    }


def _window_bounds(day, window: dict, tz_name: str):
    start = timeutil.at_time(day, window["start"], tz_name)
    end = timeutil.at_time(day, window["end"], tz_name)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def _free_gaps(wstart: datetime, wend: datetime,
               busy: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """窗口内切掉共线忙段后的空闲区间（阀门不可并发于同一 shared_line）。"""
    gaps = []
    cur = wstart
    for bstart, bend in sorted(busy):
        if bend <= cur:
            continue
        if bstart >= wend:
            break
        if bstart > cur:
            gaps.append((cur, min(bstart, wend)))
        cur = max(cur, bend)
        if cur >= wend:
            break
    if cur < wend:
        gaps.append((cur, wend))
    return gaps


def _quota_cap_seconds(
    start: datetime,
    hard_end: datetime,
    rated_lpm: float,
    remaining_quota: dict[str, float],
    tz_name: str,
) -> float:
    """从 start 起，在 hard_end 与各本地日剩余配额双重约束下最多可开多少秒。"""
    tz = timeutil.ZoneInfo(tz_name)
    total = 0.0
    cur = start
    while cur < hard_end:
        cur_local = cur.astimezone(tz)
        midnight = datetime.combine(cur_local.date() + timedelta(days=1),
                                    datetime.min.time(), tz)
        seg_end = min(midnight, hard_end)
        seg_avail = (seg_end - cur).total_seconds()
        day = cur_local.date().isoformat()
        quota_l = max(0.0, remaining_quota.get(day, 0.0))
        allow_s = min(seg_avail, quota_l * 60.0 / rated_lpm)
        total += max(0.0, allow_s)
        if allow_s < seg_avail - 1e-6:
            break  # 当日配额用尽，后续日期不再顺延（避免把今天的水挪到明天超排）
        cur = seg_end
    return total


def schedule_zone(
    *,
    zone: Zone,
    rated_lpm: float,
    demand_l: float,
    day,
    windows: list[dict],
    tz_name: str,
    busy: list[tuple[datetime, datetime]],
    remaining_quota: dict[str, float],
    seq_start: int = 1,
) -> list[dict]:
    """把一个分区的日需水切成时段，占用共线忙闲表并就地扣减内存配额。

    按时间顺序消化：优先选能完整容纳剩余水量的最早空档；装不下就截取空档
    前段（整分钟），再继续找后面的窗口/空档。排不下的水量由上层记 unscheduled。
    """
    entries: list[dict] = []
    remaining_l = demand_l
    seq = seq_start
    while remaining_l > 0.5:
        want_minutes = max(MIN_RUN_MINUTES, int(-(-remaining_l // rated_lpm)))  # ceil
        candidate_gaps = []
        for win in windows:
            wstart, wend = _window_bounds(day, win, tz_name)
            for gstart, gend in _free_gaps(wstart, wend, busy):
                candidate_gaps.append((gstart, gend))
        candidate_gaps.sort()

        placed = False
        for gstart, gend in candidate_gaps:
            cap_s = _quota_cap_seconds(gstart, gend, rated_lpm, remaining_quota, tz_name)
            minutes = int(cap_s // 60)
            if minutes < MIN_RUN_MINUTES:
                continue
            minutes = min(minutes, want_minutes)
            seconds = minutes * 60
            start, end = gstart, gstart + timedelta(seconds=seconds)
            planned_l = round(rated_lpm * minutes, 3)
            split = []
            for d, secs in timeutil.day_segments(start, end, tz_name):
                liters = round(rated_lpm * secs / 60.0, 6)
                split.append({"day": d.isoformat(), "seconds": round(secs, 3),
                              "liters": liters})
                remaining_quota[d.isoformat()] = (
                    remaining_quota.get(d.isoformat(), 0.0) - liters
                )
            entries.append(
                {"seq": seq, "start": start, "end": end,
                 "planned_l": planned_l, "day_split": split}
            )
            busy.append((start, end))
            busy.sort()
            remaining_l = round(remaining_l - planned_l, 3)
            seq += 1
            placed = True
            break
        if not placed:
            break
        if len(entries) > 64:  # 防御性上限
            break
    return entries
