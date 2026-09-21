from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc


def tzinfo(tz_name: str) -> ZoneInfo:
    return ZoneInfo(tz_name)


def parse(value: str | datetime, tz_name: str = "Asia/Shanghai") -> datetime:
    """解析 ISO 8601；朴素时间按园区时区处理。"""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat() if dt.tzinfo else dt.isoformat()


def local(dt: datetime, tz_name: str) -> datetime:
    return dt.astimezone(ZoneInfo(tz_name))


def day_key(dt: datetime, tz_name: str) -> str:
    return dt.astimezone(ZoneInfo(tz_name)).date().isoformat()


def parse_day(value: str) -> date:
    return date.fromisoformat(value)


def at_time(day: date, hm: str, tz_name: str) -> datetime:
    hh, mm = (int(x) for x in hm.split(":"))
    return datetime.combine(day, time(hh, mm), ZoneInfo(tz_name))


def day_segments(start: datetime, end: datetime, tz_name: str) -> list[tuple[date, float]]:
    """把 [start, end) 按园区本地午夜切成 (日期, 秒数) 段，用于跨午夜水量拆分。"""
    if end <= start:
        return []
    tz = ZoneInfo(tz_name)
    segments: list[tuple[date, float]] = []
    cur = start
    while cur < end:
        cur_local = cur.astimezone(tz)
        next_midnight = datetime.combine(
            cur_local.date() + timedelta(days=1), time(0, 0), tz
        )
        boundary = min(next_midnight, end)
        segments.append((cur_local.date(), (boundary - cur).total_seconds()))
        cur = boundary
    return segments
