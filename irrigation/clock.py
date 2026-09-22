"""时间工具：统一在园区时区（Asia/Shanghai）下解释业务时间。"""

from __future__ import annotations

from datetime import date as date_cls
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

DEFAULT_TZ = "Asia/Shanghai"


def get_tz(name: str = DEFAULT_TZ) -> ZoneInfo:
    return ZoneInfo(name)


def parse(value: str | datetime) -> datetime:
    """解析 ISO 8601；要求结果带时区偏移，避免朴素时间歧义。"""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"时间必须带时区偏移: {value!r}")
    return dt


def iso(dt: datetime) -> str:
    return dt.astimezone().isoformat() if dt.tzinfo is None else dt.isoformat()


def now(tz: ZoneInfo) -> datetime:
    return datetime.now(tz)


def local_date(dt: datetime, tz: ZoneInfo) -> date_cls:
    """配额分桶依据：把瞬间转换到园区本地时区后取日期。"""
    return dt.astimezone(tz).date()


def combine_local(d: date_cls, hhmm: str, tz: ZoneInfo) -> datetime:
    """把本地墙钟时间（如 06:00）绑定到园区时区。"""
    hour, minute = (int(x) for x in hhmm.split(":"))
    return datetime.combine(d, time(hour=hour, minute=minute), tzinfo=tz)


def minutes_between(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds() / 60.0


def minutes(delta: timedelta) -> float:
    return delta.total_seconds() / 60.0
