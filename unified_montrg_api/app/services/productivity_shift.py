"""Utilities for determining business date based on Jakarta shift windows."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

JAKARTA_TZ = ZoneInfo("Asia/Jakarta")


def _dt_in_jakarta(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(tz=JAKARTA_TZ)
    if now.tzinfo is None:
        return now.replace(tzinfo=JAKARTA_TZ)
    return now.astimezone(JAKARTA_TZ)


def get_jakarta_shift_business_date(now: datetime | None = None) -> date:
    """Return business date based on Jakarta shift windows.

    Windows:
    - Mon-Thu: 06:30-14:30, 14:30-22:30, 22:30-06:30(+1)
    - Fri:     06:30-15:00, 15:00-23:00, 22:30-06:30(+1)  (overlap follows existing SQL)
    - Sat:     06:30-11:30, 11:30-16:30, 16:30-21:30 (before 06:30 returns current date)
    - Sun:     Not defined; fallback to current date
    """
    now_jkt = _dt_in_jakarta(now)
    weekday = now_jkt.weekday()  # Mon=0 ... Sun=6
    today = now_jkt.date()

    def combine(t: time, base: datetime = None) -> datetime:
        base = base or now_jkt
        return datetime.combine(base.date(), t, tzinfo=JAKARTA_TZ)

    start_dt: datetime | None = None

    if weekday in {0, 1, 2, 3}:  # Mon-Thu
        t1, t2, t3 = time(6, 30), time(14, 30), time(22, 30)
        if now_jkt < combine(t1):
            start_dt = combine(t3, now_jkt - timedelta(days=1))
        elif now_jkt < combine(t2):
            start_dt = combine(t1)
        elif now_jkt < combine(t3):
            start_dt = combine(t2)
        else:
            start_dt = combine(t3)
    elif weekday == 4:  # Fri
        t1, t2, t3, t_overlap = time(6, 30), time(15, 0), time(23, 0), time(22, 30)
        if now_jkt < combine(t1):
            start_dt = combine(t_overlap, now_jkt - timedelta(days=1))
        elif now_jkt < combine(t2):
            start_dt = combine(t1)
        elif now_jkt < combine(t3):
            start_dt = combine(t2)
        else:
            start_dt = combine(t_overlap)
    elif weekday == 5:  # Sat
        t1, t2, t3 = time(6, 30), time(11, 30), time(16, 30)
        if now_jkt < combine(t1):
            start_dt = None  # follow original SQL (returns NULL)
        elif now_jkt < combine(t2):
            start_dt = combine(t1)
        elif now_jkt < combine(t3):
            start_dt = combine(t2)
        else:
            start_dt = combine(t3)
    else:  # Sun fallback
        start_dt = None

    return (start_dt or now_jkt).date()
