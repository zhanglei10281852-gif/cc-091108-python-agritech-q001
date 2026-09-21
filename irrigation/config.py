from __future__ import annotations

import json
import copy
from dataclasses import dataclass, field
from pathlib import Path

# 各生育期默认农艺参数：
#   kc        作物系数（用于作物蒸散 ETc = kc * ET0）
#   dep_max   允许耗水占比（相对于有效根区持水量 TAW）
#   wb_minutes 墒情不足时的目标回补比例（补到田持的比例）
#   refill_to  墒情驱动灌溉的目标体积含水率（田持近似）
STAGE_PROFILES: dict[str, dict] = {
    "苗期": {"kc": 0.65, "dep_max": 0.30, "refill_to": 0.32},
    "开花期": {"kc": 0.95, "dep_max": 0.40, "refill_to": 0.33},
    "坐果期": {"kc": 1.10, "dep_max": 0.45, "refill_to": 0.34},
    "结果期": {"kc": 1.05, "dep_max": 0.40, "refill_to": 0.33},
    "采收期": {"kc": 0.85, "dep_max": 0.50, "refill_to": 0.30},
}

# 土壤参数默认值（可被 zone 覆盖）；体积含水率阈值 + 水量平衡所需田持/萎蔫点/根深
DEFAULT_SOIL = {
    "moisture_threshold": 0.28,  # 低于此值判定需要灌溉（传感器直接判定）
    "moisture_target": 0.34,     # 回补目标体积含水率
    "field_capacity": 0.36,
    "wilting_point": 0.14,
    "root_depth_mm": 400.0,
    "crop_area_m2": 600.0,       # 分区灌溉面积
    "wetting_ratio": 0.85,       # 滴灌湿润比
}

# 水肥配方（每立方米灌溉水的肥量 kg/m^3，仅作计划标注，不参与水量守恒）
DEFAULT_RECIPES = {
    "番茄": {"坐果期": {"n": 0.45, "p": 0.18, "k": 0.55, "ec_ms_cm": 2.2}},
    "彩椒": {"开花期": {"n": 0.35, "p": 0.20, "k": 0.40, "ec_ms_cm": 1.8}},
}

# 传感器质量策略：suspect 读数参与但收缩权重，offline 完全不用
QUALITY_POLICY = {
    "good": {"weight": 1.0, "usable": True},
    "suspect": {"weight": 0.3, "usable": True},
    "offline": {"weight": 0.0, "usable": False},
}


@dataclass
class Valve:
    id: str
    rated_lpm: float
    shared_line: str


@dataclass
class Zone:
    id: str
    crop: str
    stage: str
    valve_id: str
    soil: dict = field(default_factory=lambda: copy.deepcopy(DEFAULT_SOIL))

    @property
    def profile(self) -> dict:
        return STAGE_PROFILES.get(self.stage, STAGE_PROFILES["结果期"])

    @property
    def recipe(self) -> dict | None:
        return DEFAULT_RECIPES.get(self.crop, {}).get(self.stage)


@dataclass
class DomainConfig:
    timezone: str
    daily_water_limit_liters: float
    zones: list[Zone]
    valves: list[Valve]
    valve_by_id: dict[str, Valve]
    zone_by_id: dict[str, Zone]

    @classmethod
    def load(cls, path: str | Path) -> "DomainConfig":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        valves = [
            Valve(id=v["id"], rated_lpm=float(v["rated_lpm"]), shared_line=v["shared_line"])
            for v in raw["valves"]
        ]
        zones = [
            Zone(
                id=z["id"],
                crop=z["crop"],
                stage=z["stage"],
                valve_id=z["valve_id"],
                soil={**copy.deepcopy(DEFAULT_SOIL), **z.get("soil", {})},
            )
            for z in raw["zones"]
        ]
        cfg = cls(
            timezone=raw.get("timezone", "Asia/Shanghai"),
            daily_water_limit_liters=float(raw["daily_water_limit_liters"]),
            zones=zones,
            valves=valves,
            valve_by_id={v.id: v for v in valves},
            zone_by_id={z.id: z for z in zones},
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.daily_water_limit_liters <= 0:
            raise ValueError("daily_water_limit_liters 必须为正")
        zone_valves = {z.valve_id for z in self.zones}
        if zone_valves != set(self.valve_by_id):
            raise ValueError("zones 与 valves 的阀门集合不一致")
        if len({z.id for z in self.zones}) != len(self.zones):
            raise ValueError("zone id 重复")
        for z in self.zones:
            if z.valve_id not in self.valve_by_id:
                raise ValueError(f"分区 {z.id} 引用未知阀门 {z.valve_id}")

    def zones_of_line(self, line: str) -> list[Zone]:
        return [z for z in self.zones if self.valve_by_id[z.valve_id].shared_line == line]
