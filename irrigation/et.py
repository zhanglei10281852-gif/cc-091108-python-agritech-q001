"""蒸散估算与单时段决策。

决策输出同时携带结构化 reason：触发因子、所用读数、质量、ET0、作物系数、
亏缺换算、配额/单次封顶与肥方投加量，供主管界面与审计追溯。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime

from .config import Domain, Zone, ZoneThresholds
from .models import Quality, Reading

# 1 mm 水深均匀铺在 1 m² 上恰好是 1 L，故 mm 与 L/m² 数值相等。
MM_TO_L_PER_M2 = 1.0


@dataclass
class Decision:
    zone_id: str
    irrigate: bool
    liters: float
    duration_min: float
    reason: dict = field(default_factory=dict)


@dataclass
class ZoneContext:
    """一次决策时该分区的已知信息。"""

    latest_reading: Reading | None = None
    et0_mm: float | None = None  # 当日参考蒸散（气象来源）
    already_allocated_mm: float = 0.0  # 当日更早时段已安排/已浇的水量（mm）


class EtEstimator:
    def __init__(self, domain: Domain):
        self.domain = domain

    def decide(
        self,
        zone: Zone,
        ctx: ZoneContext,
        *,
        at: datetime,
        lpm: float | None = None,
        thresholds: ZoneThresholds | None = None,
        freshness_min: float | None = None,
    ) -> Decision:
        d = self.domain
        thr = thresholds or d.thresholds_for(zone.id)
        coef = d.stage_coef(zone.stage)
        area = d.area_of(zone.id)
        valve = d.valve_of(zone.id)
        lpm = lpm if lpm is not None else valve.rated_lpm
        formula = d.formula_for(zone.id)

        et0 = ctx.et0_mm
        etc_mm: float | None = None
        if et0 is not None:
            etc_mm = et0 * coef.kc

        r = ctx.latest_reading
        age_min = (
            round((at - r.occurred_at).total_seconds() / 60.0, 1) if r is not None else None
        )
        stale = (
            r is not None
            and freshness_min is not None
            and age_min is not None
            and age_min > freshness_min
        )
        usable = (
            r is not None
            and r.quality in (Quality.GOOD, Quality.SUSPECT)
            and not stale
        )
        degraded = not usable

        moisture_deficit_mm = 0.0
        if usable:
            gap = max(0.0, thr.target_moisture - (r.moisture or 0.0))
            moisture_deficit_mm = gap * thr.effective_root_depth_m * 1000.0

        # 当日剩余的蒸散补充需求（mm），已安排时段不再重复补水。
        remaining_et_mm = max(0.0, (etc_mm or 0.0) - ctx.already_allocated_mm)
        # 含水率亏缺同样要扣除当日已浇水量：浇过的水正在抬升含水率，
        # 同一条读数不得在后续时段重复计亏缺。
        remaining_moisture_mm = max(0.0, moisture_deficit_mm - ctx.already_allocated_mm)

        triggers: list[str] = []
        if degraded:
            # 传感器降级：不使用任何可能迟到/陈旧的含水率，改用保守蒸散维持量。
            # 已浇过的水量必须扣除，避免“读数迟到 → 再次开阀”重复灌溉。
            gross_need = max(thr.offline_fallback_et_mm, etc_mm or 0.0)
            need_mm = max(0.0, gross_need - ctx.already_allocated_mm)
            triggers.append("sensor_degraded")
            if r is not None and r.quality == Quality.OFFLINE:
                triggers.append("sensor_offline")
            elif stale:
                triggers.append("sensor_stale")
            if etc_mm is not None:
                triggers.append("et_replenishment")
        else:
            need_mm = max(remaining_moisture_mm, remaining_et_mm)
            if r.moisture is not None and r.moisture < thr.min_moisture:
                triggers.append("moisture_below_min")
            elif moisture_deficit_mm > 1e-9:
                triggers.append("moisture_below_target")
            if remaining_et_mm > 0:
                triggers.append("et_replenishment")

        # 单次封顶 + 当日作物阶段总量封顶。
        capped_by: list[str] = []
        stage_cap_mm = coef.max_daily_liters_per_m2
        stage_remaining_mm = math.inf
        if stage_cap_mm is not None:
            stage_remaining_mm = max(0.0, stage_cap_mm - ctx.already_allocated_mm)
            if need_mm > stage_remaining_mm:
                capped_by.append("stage_daily_cap")
            need_mm = min(need_mm, stage_remaining_mm)
        if need_mm > thr.max_single_irrigation_mm:
            capped_by.append("max_single_irrigation")
            need_mm = min(need_mm, thr.max_single_irrigation_mm)

        irrigate = need_mm > 1e-9 and (bool(triggers))
        if not usable and not irrigate:
            irrigate = False
        liters = round(need_mm * MM_TO_L_PER_M2 * area, 3) if irrigate else 0.0
        duration_min = round(liters / lpm, 2) if lpm > 0 and liters else 0.0

        nutrients: dict[str, float] = {}
        if irrigate and formula is not None:
            nutrients = {
                k: round(liters / 1000.0 * grams, 2) for k, grams in formula.grams_per_m3.items()
            }

        reason = {
            "at": at.isoformat(),
            "zone_id": zone.id,
            "crop": zone.crop,
            "stage": zone.stage,
            "area_m2": area,
            "valve_id": valve.id,
            "triggers": triggers,
            "sensor": (
                {
                    "event_id": r.event_id,
                    "quality": r.quality.value,
                    "moisture": r.moisture,
                    "occurred_at": r.occurred_at.isoformat(),
                    "age_min": age_min,
                    "stale": stale,
                    "usable": usable,
                }
                if r is not None
                else None
            ),
            "et": {
                "et0_mm": et0,
                "kc": coef.kc,
                "etc_mm": round(etc_mm, 3) if etc_mm is not None else None,
                "fallback_et_mm": thr.offline_fallback_et_mm if degraded else None,
                "already_allocated_mm": round(ctx.already_allocated_mm, 3),
            },
            "moisture_deficit_mm": round(moisture_deficit_mm, 3),
            "need_mm": round(need_mm, 3),
            "capped_by": capped_by,
            "thresholds": {
                "min_moisture": thr.min_moisture,
                "target_moisture": thr.target_moisture,
                "root_depth_m": thr.effective_root_depth_m,
            },
            "rated_lpm": lpm,
            "formula": formula.name if formula else None,
            "nutrients_grams": nutrients,
        }
        return Decision(
            zone_id=zone.id,
            irrigate=irrigate,
            liters=liters,
            duration_min=duration_min,
            reason=reason,
        )
