# src/refresh.py
from __future__ import annotations
import pendulum
from datetime import datetime

CAIRO = pendulum.timezone("Africa/Cairo")


def _now():
    return pendulum.now(CAIRO)


def next_hourly_slots(slots_24h: list[int]) -> tuple[datetime, datetime]:
    """Return (prev, next) nearest run times today/tomorrow for given whole-hour slots in Cairo."""
    now = _now()
    today_slots = [
        now.replace(hour=h, minute=0, second=0, microsecond=0) for h in slots_24h
    ]
    future = [t for t in today_slots if t > now]
    prev = max(
        [t for t in today_slots if t <= now],
        default=now.replace(hour=0, minute=0, second=0, microsecond=0),
    )
    nxt = (
        future[0]
        if future
        else now.add(days=1).replace(
            hour=slots_24h[0], minute=0, second=0, microsecond=0
        )
    )
    return (prev, nxt)


def next_daily_at(h: int, m: int = 0) -> tuple[datetime, datetime]:
    """Return (prev, next) daily run around a fixed time in Cairo."""
    now = _now()
    today_run = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if now >= today_run:
        prev, nxt = today_run, today_run.add(days=1)
    else:
        prev, nxt = today_run.subtract(days=1), today_run
    return (prev, nxt)


def minutes_age(last_run_dt) -> float | None:
    """Return minutes since last_run_dt (aware/naive ok)."""
    if last_run_dt is None:
        return None
    try:
        # handles pandas.Timestamp / datetime
        dt = pendulum.instance(last_run_dt)  # may be naive or aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CAIRO)
        return (_now() - dt).total_seconds() / 60.0
    except Exception:
        return None


def badge_color(age_minutes: float | None, budget_minutes: float) -> str:
    """Green if within window, yellow if slightly late, red if stale/unknown."""
    if age_minutes is None:
        return "🟦"  # unknown
    if age_minutes <= budget_minutes:
        return "✅"
    if age_minutes <= budget_minutes * 1.5:
        return "🟨"
    return "🟥"
