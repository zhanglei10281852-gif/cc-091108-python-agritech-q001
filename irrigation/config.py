"""静态领域配置：分区、阀门、阈值、水肥配方与服务参数。

配置可从 reference/domain.json 加载（资料包是领域约定的事实来源），
也可由调用方直接构造，便于测试自定义场景。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .clock import DEFAULT_TZ


@dataclass(frozen=True)
class Valve:
    id: str
    rated_lpm: float
    shared_line: str


@dataclass(frozen=True)
class Zone:
    id: str
    crop: str
    stage: str
    valve_id: str


@dataclass(frozen=True)
class StageCoefficients:
    """作物系数与安全窗口，按生育期取值。

    kc 用于把参考蒸散折算成作物蒸散：etc = et0 * kc。
    """

    kc: float = 1.0
    max_daily_liters_per_m2: float | None = None


@dataclass(frozen=True)
class NutrientFormula:
    """水肥配方：每立方米灌溉水投加的肥料克数。"""

    name: str
    grams_per_m3: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ZoneThresholds:
    """单区可由农艺师修订的阈值。"""

    min_moisture: float  # 体积含水率触发下限
    target_moisture: float  # 补灌目标含水率
    effective_root_depth_m: float  # 有效根深，决定每 1% 含水率的补水量
    max_single_irrigation_mm: float = 15.0
    # 质量降级时使用的默认蒸散量（L/m²/日），离线时据此维持保守灌溉
    offline_fallback_et_mm: float = 3.0


@dataclass(frozen=True)
class Domain:
    timezone: str
    daily_water_limit_liters: float
    zones: tuple[Zone, ...]
    valves: tuple[Valve, ...]
    area_m2: dict[str, float] = field(default_factory=dict)
    thresholds: dict[str, ZoneThresholds] = field(default_factory=dict)
    stages: dict[str, StageCoefficients] = field(default_factory=dict)
    formulas: dict[str, NutrientFormula] = field(default_factory=dict)
    zone_formula: dict[str, str] = field(default_factory=dict)

    def zone(self, zone_id: str) -> Zone:
        for z in self.zones:
            if z.id == zone_id:
                return z
        raise KeyError(f"未知分区: {zone_id}")

    def valve_of(self, zone_id: str) -> Valve:
        return self.valve_by_id(self.zone(zone_id).valve_id)

    def valve_by_id(self, valve_id: str) -> Valve:
        for v in self.valves:
            if v.id == valve_id:
                return v
        raise KeyError(f"未知阀门: {valve_id}")

    def thresholds_for(self, zone_id: str) -> ZoneThresholds:
        if zone_id in self.thresholds:
            return self.thresholds[zone_id]
        return ZoneThresholds(
            min_moisture=0.25,
            target_moisture=0.35,
            effective_root_depth_m=0.3,
        )

    def area_of(self, zone_id: str) -> float:
        return float(self.area_m2.get(zone_id, 100.0))

    def stage_coef(self, stage: str) -> StageCoefficients:
        return self.stages.get(stage, StageCoefficients())

    def formula_for(self, zone_id: str) -> NutrientFormula | None:
        name = self.zone_formula.get(zone_id)
        return self.formulas.get(name) if name else None


# 参考资料包默认值：资料包未给出面积/阈值，这里给出可工作的内置默认，
# 真实部署应由农艺配置文件覆盖。
_DEFAULT_thresholds = {
    "GH-A-01": ZoneThresholds(
        min_moisture=0.25,
        target_moisture=0.35,
        effective_root_depth_m=0.30,
        max_single_irrigation_mm=12.0,
        offline_fallback_et_mm=3.0,
    ),
    "GH-A-02": ZoneThresholds(
        min_moisture=0.27,
        target_moisture=0.37,
        effective_root_depth_m=0.25,
        max_single_irrigation_mm=10.0,
        offline_fallback_et_mm=2.5,
    ),
}

_DEFAULT_AREA = {"GH-A-01": 200.0, "GH-A-02": 160.0}

_DEFAULT_STAGES = {
    "开花期": StageCoefficients(kc=0.95, max_daily_liters_per_m2=8.0),
    "坐果期": StageCoefficients(kc=1.10, max_daily_liters_per_m2=10.0),
}

_DEFAULT_FORMULAS = {
    "tomato-fruiting": NutrientFormula(
        name="tomato-fruiting", grams_per_m3={"N": 180.0, "P2O5": 80.0, "K2O": 250.0}
    ),
    "pepper-flower": NutrientFormula(
        name="pepper-flower", grams_per_m3={"N": 150.0, "P2O5": 100.0, "K2O": 180.0}
    ),
}

_DEFAULT_ZONE_FORMULA = {"GH-A-01": "tomato-fruiting", "GH-A-02": "pepper-flower"}


@dataclass(frozen=True)
class ServiceConfig:
    """运行期参数（非领域事实）。"""

    timezone: str = DEFAULT_TZ
    # 时段时长：计划按等宽时段组织（分钟）
    slot_minutes: int = 30
    # 阀门回执超时：超过此时长未收到回执的 OPEN 命令可被判定失联
    ack_timeout_minutes: int = 10
    # 遥测迟到视为“仍可影响计划”的宽限；超过此年龄的好读数只进审计，不改未锁定时段
    late_reading_max_age_hours: float = 24.0


def load_domain(path: str | Path) -> Domain:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return domain_from_dict(raw)


def domain_from_dict(raw: dict) -> Domain:
    zones = tuple(
        Zone(id=z["id"], crop=z["crop"], stage=z["stage"], valve_id=z["valve_id"])
        for z in raw.get("zones", [])
    )
    valves = tuple(
        Valve(id=v["id"], rated_lpm=float(v["rated_lpm"]), shared_line=v["shared_line"])
        for v in raw.get("valves", [])
    )
    area = {k: float(v) for k, v in raw.get("area_m2", _DEFAULT_AREA).items()}
    thresholds = _parse_thresholds(raw.get("thresholds"))
    stages = _DEFAULT_STAGES | {
        k: StageCoefficients(**v) for k, v in raw.get("stages", {}).items()
    }
    formulas = _DEFAULT_FORMULAS | {
        k: NutrientFormula(name=k, **({"grams_per_m3": v} if isinstance(v, dict) else v))
        for k, v in raw.get("formulas", {}).items()
    }
    zone_formula = dict(_DEFAULT_ZONE_FORMULA, **raw.get("zone_formula", {}))

    zone_ids = {z.id for z in zones}
    valve_ids = {v.id for v in valves}
    if {z.valve_id for z in zones} != valve_ids:
        raise ValueError("分区阀门与阀门清单不一致")
    unknown = {z for z in zone_formula if z not in zone_ids}
    if unknown:
        raise ValueError(f"配方绑定了未知分区: {sorted(unknown)}")

    return Domain(
        timezone=raw.get("timezone", DEFAULT_TZ),
        daily_water_limit_liters=float(raw["daily_water_limit_liters"]),
        zones=zones,
        valves=valves,
        area_m2=area,
        thresholds=thresholds,
        stages=stages,
        formulas=formulas,
        zone_formula=zone_formula,
    )


def _parse_thresholds(raw: dict | None) -> dict[str, ZoneThresholds]:
    if not raw:
        return dict(_DEFAULT_thresholds)
    out = dict(_DEFAULT_thresholds)
    for zone_id, vals in raw.items():
        out[zone_id] = ZoneThresholds(**vals)
    return out


def load_config(path: str | Path | None = None) -> ServiceConfig:
    if path is None:
        return ServiceConfig()
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return ServiceConfig(**raw)
