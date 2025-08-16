#!/usr/bin/env python3
"""
ForexFactory Red-Folder (High Impact) News Bot

This script pulls the weekly JSON feed used by ForexFactory (ff_calendar_thisweek.json),
filters for High impact events, and sends daily digests to a Telegram chat using
python-telegram-bot (async).

Environment variables:
- TELEGRAM_BOT_TOKEN (required)
- TELEGRAM_CHAT_ID (optional; if not set, chat that uses /start will be used)
- DAILY_TIME_SAST (default 07:00)
- SUNDAY_TIME_SAST (default 17:00)
- ENABLE_WEEKLY_PREVIEW (true/false)
- IMPACT_LEVEL (e.g. High or High,Medium)
- TIMEZONE (default Africa/Johannesburg)
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import textwrap
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp
import pytz
from dateutil import parser as dateparser
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (Application, ApplicationBuilder, CallbackContext,
                          CommandHandler)

# ------------------------- Config -------------------------
FAIR_JSON_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
DEFAULT_TZ = os.getenv("TIMEZONE", "Africa/Johannesburg")
DAILY_TIME_SAST = os.getenv("DAILY_TIME_SAST", "07:00")
SUNDAY_TIME_SAST = os.getenv("SUNDAY_TIME_SAST", "17:00")
ENABLE_WEEKLY_PREVIEW = os.getenv("ENABLE_WEEKLY_PREVIEW", "true").lower() in {"1", "true", "yes", "y"}
IMPACT_LEVELS = {s.strip().capitalize() for s in os.getenv("IMPACT_LEVEL", "High").split(",") if s.strip()}

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DEFAULT_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # optional

TZ = pytz.timezone(DEFAULT_TZ)
UTC = pytz.UTC

# ------------------------- Logging -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("ff_red_folder_bot")

# ------------------------- Models -------------------------
@dataclass
class FFEvent:
    id: Optional[int]
    title: str
    country: str  # e.g., USD, EUR
    date_utc: dt.datetime
    impact: str   # High, Medium, Low, Holiday, etc.
    actual: Optional[str]
    forecast: Optional[str]
    previous: Optional[str]

    @property
    def date_local(self) -> dt.datetime:
        return self.date_utc.astimezone(TZ)

# ------------------------- Utilities -------------------------
COUNTRY_TO_FLAG = {
    "USD": "🇺🇸", "EUR": "🇪🇺", "GBP": "🇬🇧", "JPY": "🇯🇵", "AUD": "🇦🇺",
    "NZD": "🇳🇿", "CAD": "🇨🇦", "CHF": "🇨🇭", "CNY": "🇨🇳", "ZAR": "🇿🇦",
    "SEK": "🇸🇪", "NOK": "🇳🇴", "MXN": "🇲🇽", "TRY": "🇹🇷", "BRL": "🇧🇷",
    "INR": "🇮🇳", "RUB": "🇷🇺", "HKD": "🇭🇰", "SGD": "🇸🇬",
}

IMPACT_EMOJI = {
    "High": "🔴",
    "Medium": "🟠",
    "Low": "🟡",
    "Holiday": "🎌",
}

def flag_for(country: str) -> str:
    return COUNTRY_TO_FLAG.get(country.upper(), "🏳️")

def parse_hhmm(value: str) -> Tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)

# ------------------------- Fetch/Parse -------------------------
async def fetch_thisweek_json(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Fetch weekly JSON with retries/backoff. Returns list of raw event dicts."""
    backoffs = [1, 2, 5, 10]
    for attempt, pause in enumerate([0] + backoffs, start=1):
        if pause:
            await asyncio.sleep(pause)
        try:
            async with session.get(FAIR_JSON_URL, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
                elif r.status in (429, 500, 502, 503, 504):
                    log.warning("Feed status %s, attempt %s — backing off %ss", r.status, attempt, pause)
                    continue
                else:
                    text = await r.text()
                    log.error("Unexpected status %s: %s", r.status, text[:200])
                    return []
        except Exception as e:
            log.warning("Fetch error on attempt %s: %s", attempt, e)
    return []

def coerce_event(raw: Dict[str, Any]) -> Optional[FFEvent]:
    try:
        date_utc: dt.datetime
        if isinstance(raw.get("date"), str):
            date_utc = dateparser.parse(raw["date"]).astimezone(UTC)
        elif isinstance(raw.get("timestamp"), (int, float)):
            date_utc = dt.datetime.fromtimestamp(float(raw["timestamp"]) / 1000.0, tz=UTC)
        else:
            return None

        return FFEvent(
            id=int(raw.get("id") or raw.get("event_id") or raw.get("ff_event_id") or 0) or None,
            title=str(raw.get("title") or raw.get("event") or "").strip() or "(untitled)",
            country=str(raw.get("country") or raw.get("currency") or "").strip() or "",
            date_utc=date_utc,
            impact=str(raw.get("impact") or "").strip().capitalize() or "",
            actual=(None if raw.get("actual") in ("", None) else str(raw.get("actual"))),
            forecast=(None if raw.get("forecast") in ("", None) else str(raw.get("forecast"))),
            previous=(None if raw.get("previous") in ("", None) else str(raw.get("previous"))),
        )
    except Exception as e:
        log.debug("coerce_event failed: %s raw=%s", e, raw)
        return None

async def get_events_filtered(impacts: Iterable[str]) -> List[FFEvent]:
    impacts = {s.capitalize() for s in impacts}
    async with aiohttp.ClientSession(headers={"User-Agent": "FF-RedFolder-Bot/1.0"}) as session:
        raw = await fetch_thisweek_json(session)
    events = [e for e in (coerce_event(x) for x in raw) if e is not None]
    now = dt.datetime.now(tz=TZ)
    result = [e for e in events if (e.impact in impacts) and (e.date_local.date() >= now.date())]
    return sorted(result, key=lambda e: e.date_local)

# ------------------------- Formatting -------------------------
def escape_html(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))

def format_event_line(e: FFEvent) -> str:
    t = e.date_local.strftime("%H:%M")
    bits = []
    if e.actual is not None:
        bits.append(f"Actual: {e.actual}")
    if e.forecast is not None:
        bits.append(f"Forecast: {e.forecast}")
    if e.previous is not None:
        bits.append(f"Previous: {e.previous}")
    details = " • ".join(bits)
    emoji = IMPACT_EMOJI.get(e.impact, "🔔")
    flag = flag_for(e.country)
    base = f"{emoji} {t} {flag} {e.country} — <b>{escape_html(e.title)}</b>"
    return f"{base}\n{details}" if details else base

def group_by_day(events: List[FFEvent]) -> List[Tuple[dt.date, List[FFEvent]]]:
    days: Dict[dt.date, List[FFEvent]] = {}
    for e in events:
        days.setdefault(e.date_local.date(), []).append(e)
    return sorted(days.items(), key=lambda kv: kv[0])

def render_digest(events: List[FFEvent], title: str) -> str:
    if not events:
        return f"<b>{escape_html(title)}</b>\nNo matching events."
    parts = [f"<b>{escape_html(title)}</b>"]
    for day, evs in group_by_day(events):
        parts.append(f"\n<b>{day.strftime('%A, %d %b %Y')}</b>")
        for e in evs:
            parts.append(format_event_line(e))
    return "\n".join(parts)

# ------------------------- Telegram Handlers -------------------------
async def cmd_start(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id,
        text=("Hi! I'll send you <b>High Impact (Red Folder)</b> news from ForexFactory.\n\n"
              "Commands: /today /tomorrow /week \n"
              "You'll also receive daily digests automatically."),
        parse_mode=ParseMode.HTML)

async def cmd_today(update: Update, context: CallbackContext) -> None:
    events = await get_events_filtered(IMPACT_LEVELS)
    today = dt.datetime.now(tz=TZ).date()
    events = [e for e in events if e.date_local.date() == today]
    await update.effective_chat.send_message(render_digest(events, "Today's High Impact"), parse_mode=ParseMode.HTML)

async def cmd_tomorrow(update: Update, context: CallbackContext) -> None:
    events = await get_events_filtered(IMPACT_LEVELS)
    tomorrow = dt.datetime.now(tz=TZ).date() + dt.timedelta(days=1)
    events = [e for e in events if e.date_local.date() == tomorrow]
    await update.effective_chat.send_message(render_digest(events, "Tomorrow's High Impact"), parse_mode=ParseMode.HTML)

async def cmd_week(update: Update, context: CallbackContext) -> None:
    events = await get_events_filtered(IMPACT_LEVELS)
    await update.effective_chat.send_message(render_digest(events, "This Week: High Impact"), parse_mode=ParseMode.HTML)

# ------------------------- Scheduled Jobs -------------------------
async def job_daily(context: CallbackContext) -> None:
    chat_id = DEFAULT_CHAT_ID or context.job.chat_id
    events = await get_events_filtered(IMPACT_LEVELS)
    today = dt.datetime.now(tz=TZ).date()
    target = today + dt.timedelta(days=1)
    while target.weekday() >= 5:
        target += dt.timedelta(days=1)
    day_events = [e for e in events if e.date_local.date() == target]
    await context.bot.send_message(chat_id=chat_id, text=render_digest(day_events, f"{target.strftime('%A')} High Impact"), parse_mode=ParseMode.HTML)

async def job_weekly_preview(context: CallbackContext) -> None:
    if not ENABLE_WEEKLY_PREVIEW:
        return
    chat_id = DEFAULT_CHAT_ID or context.job.chat_id
    events = await get_events_filtered(IMPACT_LEVELS)
    today = dt.datetime.now(tz=TZ).date()
    days_ahead = (7 - today.weekday()) % 7
    start = today + dt.timedelta(days=days_ahead)
    end = start + dt.timedelta(days=4)
    week_events = [e for e in events if start <= e.date_local.date() <= end]
    await context.bot.send_message(chat_id=chat_id, text=render_digest(week_events, "Next Week Preview (High Impact)"), parse_mode=ParseMode.HTML)

# ------------------------- Main -------------------------
async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Please set TELEGRAM_BOT_TOKEN")

    app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("tomorrow", cmd_tomorrow))
    app.add_handler(CommandHandler("week", cmd_week))

    hour, minute = parse_hhmm(DAILY_TIME_SAST)
    sunday_hour, sunday_min = parse_hhmm(SUNDAY_TIME_SAST)

    app.job_queue.run_daily(job_daily, time=dt.time(hour, minute, tzinfo=TZ))

    if ENABLE_WEEKLY_PREVIEW:
        app.job_queue.run_daily(job_weekly_preview, days=(6,), time=dt.time(sunday_hour, sunday_min, tzinfo=TZ))

    log.info("Bot started. Daily at %s, Sunday at %s (TZ %s)", DAILY_TIME_SAST, SUNDAY_TIME_SAST, DEFAULT_TZ)
    await app.initialize()
    await app.start()
    try:
        await asyncio.Event().wait()
    finally:
        await app.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
