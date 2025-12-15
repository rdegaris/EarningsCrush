"""Shared-ish Finnhub earnings caching for EarningsCrush scripts.

Goals
- Avoid hammering Finnhub calendar endpoint.
- Cache both positive and negative results with TTL.
- Optional yfinance confirmation when installed.

This file intentionally has no repo-local imports so it can be used by multiple
scripts in this folder without circular dependencies.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


CACHE_TTL_SECONDS = int(os.environ.get("EARNINGS_CALENDAR_CACHE_TTL_SECONDS", "21600"))  # 6 hours
CONFIRM_WITH_YFINANCE = (os.environ.get("EARNINGS_CONFIRM_YFINANCE", "1").strip() != "0")


def _default_cache_path() -> str:
    base = Path(os.environ.get("FORWARD_VOL_CACHE_DIR") or (Path.home() / ".forward-volatility"))
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return str(base / "finnhub_earnings_calendar_cache.json")


CACHE_FILE = os.environ.get("EARNINGS_CALENDAR_CACHE_FILE") or _default_cache_path()


def _load_cache() -> Dict[str, Any]:
    try:
        if not os.path.exists(CACHE_FILE):
            return {"meta": {"version": 1}, "entries": {}}
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"meta": {"version": 1}, "entries": {}}
        if "entries" not in data or not isinstance(data.get("entries"), dict):
            return {"meta": {"version": 1}, "entries": {}}
        return data
    except Exception:
        return {"meta": {"version": 1}, "entries": {}}


def _save_cache(data: Dict[str, Any]) -> None:
    try:
        Path(CACHE_FILE).parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def _cache_key(symbol: str, from_date: date, to_date: date) -> str:
    return f"{symbol.upper()}|{from_date.strftime('%Y-%m-%d')}|{to_date.strftime('%Y-%m-%d')}"


def fetch_earnings_calendar(
    symbol: str,
    from_date: date,
    to_date: date,
    token: str,
) -> List[Dict[str, Any]]:
    url = (
        "https://finnhub.io/api/v1/calendar/earnings"
        f"?from={from_date.strftime('%Y-%m-%d')}"
        f"&to={to_date.strftime('%Y-%m-%d')}"
        f"&symbol={symbol}"
        f"&token={token}"
    )
    resp = requests.get(url, timeout=15)
    if resp.status_code != 200:
        return []
    data = resp.json() or {}
    cal = data.get("earningsCalendar") or []
    return [x for x in cal if isinstance(x, dict)]


def fetch_earnings_calendar_cached(
    symbol: str,
    from_date: date,
    to_date: date,
    token: str,
    ttl_seconds: int = CACHE_TTL_SECONDS,
) -> List[Dict[str, Any]]:
    k = _cache_key(symbol, from_date, to_date)
    now = time.time()

    cache = _load_cache()
    entries = cache.get("entries") or {}
    entry = entries.get(k)

    if isinstance(entry, dict):
        checked_at = float(entry.get("checked_at") or 0.0)
        if checked_at > 0 and (now - checked_at) <= ttl_seconds:
            data = entry.get("data")
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
            return []

    data = fetch_earnings_calendar(symbol, from_date, to_date, token)
    entries[k] = {"checked_at": now, "data": data}

    # Basic pruning to avoid unbounded growth
    if len(entries) > 4000:
        # Drop oldest ~500 entries
        items = []
        for key, val in entries.items():
            ts = 0.0
            if isinstance(val, dict):
                try:
                    ts = float(val.get("checked_at") or 0.0)
                except Exception:
                    ts = 0.0
            items.append((ts, key))
        items.sort()
        for _, key in items[:500]:
            entries.pop(key, None)

    cache["entries"] = entries
    _save_cache(cache)
    return data


def _yfinance_next_earnings_date(symbol: str) -> Optional[date]:
    try:
        import yfinance as yf

        cal = yf.Ticker(symbol).calendar
        if not cal or "Earnings Date" not in cal:
            return None
        earnings_dates = cal["Earnings Date"]
        if not earnings_dates or len(earnings_dates) == 0:
            return None
        d = earnings_dates[0]
        if hasattr(d, "year"):
            return date(d.year, d.month, d.day)
        return datetime.strptime(str(d), "%Y-%m-%d").date()
    except Exception:
        return None


def get_next_earnings_date_cached(
    symbol: str,
    days_ahead: int,
    token: str,
    confirm_with_yfinance: bool = CONFIRM_WITH_YFINANCE,
) -> Optional[str]:
    today = date.today()
    from_d = today
    to_d = today + timedelta(days=days_ahead)
    cal = fetch_earnings_calendar_cached(symbol, from_d, to_d, token)
    if not cal:
        return None
    first = cal[0]
    date_str = first.get("date")
    if not date_str:
        return None

    if confirm_with_yfinance:
        yf_date = _yfinance_next_earnings_date(symbol)
        if yf_date is not None:
            try:
                fh_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if abs((fh_date - yf_date).days) >= 4:
                    print(
                        f"  [WARN] Finnhub vs Yahoo mismatch for {symbol}: "
                        f"Finnhub={fh_date} Yahoo={yf_date}"
                    )
            except Exception:
                pass

    return date_str
