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
YFINANCE_FALLBACK = (os.environ.get("EARNINGS_YFINANCE_FALLBACK", "1").strip() != "0")

# Finnhub calendar endpoint supports fetching all earnings for a date range.
# Doing per-symbol requests for large universes can hit rate limits / return partial data.
USE_BULK_FINNHUB = (os.environ.get("EARNINGS_CALENDAR_USE_BULK", "1").strip() != "0")
DEBUG = (os.environ.get("EARNINGS_CALENDAR_DEBUG", "0").strip() != "0")


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


def _bulk_cache_key(from_date: date, to_date: date) -> str:
    return f"ALL|{from_date.strftime('%Y-%m-%d')}|{to_date.strftime('%Y-%m-%d')}"


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
        if DEBUG:
            try:
                print(f"  [WARN] Finnhub earnings request failed ({resp.status_code}) for {symbol}")
            except Exception:
                pass
        return []
    data = resp.json() or {}
    cal = data.get("earningsCalendar") or []
    return [x for x in cal if isinstance(x, dict)]


def fetch_earnings_calendar_bulk(
    from_date: date,
    to_date: date,
    token: str,
) -> List[Dict[str, Any]]:
    """Fetch Finnhub earnings calendar for the full range (all symbols)."""
    url = (
        "https://finnhub.io/api/v1/calendar/earnings"
        f"?from={from_date.strftime('%Y-%m-%d')}"
        f"&to={to_date.strftime('%Y-%m-%d')}"
        f"&token={token}"
    )
    resp = requests.get(url, timeout=20)
    if resp.status_code != 200:
        if DEBUG:
            try:
                print(f"  [WARN] Finnhub bulk earnings request failed ({resp.status_code})")
            except Exception:
                pass
        return []
    data = resp.json() or {}
    cal = data.get("earningsCalendar") or []
    return [x for x in cal if isinstance(x, dict)]


_bulk_index_memory: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}


def fetch_earnings_calendar_bulk_cached(
    from_date: date,
    to_date: date,
    token: str,
    ttl_seconds: int = CACHE_TTL_SECONDS,
) -> List[Dict[str, Any]]:
    """Cached wrapper for the bulk Finnhub calendar."""
    k = _bulk_cache_key(from_date, to_date)
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

    data = fetch_earnings_calendar_bulk(from_date, to_date, token)
    entries[k] = {"checked_at": now, "data": data}

    # Basic pruning to avoid unbounded growth
    if len(entries) > 4000:
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
    # Invalidate in-memory index for this range
    _bulk_index_memory.pop(k, None)
    return data


def _index_bulk_calendar(
    bulk_cache_key: str,
    calendar: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    idx = _bulk_index_memory.get(bulk_cache_key)
    if isinstance(idx, dict):
        return idx
    built: Dict[str, List[Dict[str, Any]]] = {}
    for e in calendar:
        sym = (e.get("symbol") or "").strip().upper()
        if not sym:
            continue
        built.setdefault(sym, []).append(e)
    _bulk_index_memory[bulk_cache_key] = built
    return built


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

    # Fetch: prefer bulk Finnhub (1 request per range) then filter.
    data: List[Dict[str, Any]] = []
    if USE_BULK_FINNHUB:
        bulk_k = _bulk_cache_key(from_date, to_date)
        bulk = fetch_earnings_calendar_bulk_cached(from_date, to_date, token, ttl_seconds=ttl_seconds)
        idx = _index_bulk_calendar(bulk_k, bulk)
        data = [x for x in (idx.get(symbol.upper()) or []) if isinstance(x, dict)]

    # Fallback to per-symbol Finnhub call if bulk is disabled or missing the symbol.
    if not data:
        data = fetch_earnings_calendar(symbol, from_date, to_date, token)

    # Optional fallback to yfinance when Finnhub has no entry.
    # This helps when Finnhub misses small-caps / odd tickers or when the API returns partial data.
    if (not data) and YFINANCE_FALLBACK:
        yf_date = _yfinance_next_earnings_date(symbol)
        if yf_date is not None and from_date <= yf_date <= to_date:
            data = [
                {
                    "symbol": symbol.upper(),
                    "date": yf_date.strftime("%Y-%m-%d"),
                    "hour": None,
                    "source": "yahoo",
                }
            ]
            if DEBUG:
                try:
                    print(f"  [INFO] Yahoo fallback earnings for {symbol}: {data[0]['date']}")
                except Exception:
                    pass

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
        # As a last resort, allow a direct yfinance lookup (in case caching returns empty).
        if YFINANCE_FALLBACK:
            yf_date = _yfinance_next_earnings_date(symbol)
            if yf_date is not None and from_d <= yf_date <= to_d:
                return yf_date.strftime("%Y-%m-%d")
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


def fetch_historical_earnings_surprise(
    symbol: str,
    token: str,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    """
    Fetch historical earnings from Finnhub's earnings surprise endpoint.
    
    This returns PAST earnings with actual results, unlike the calendar endpoint
    which only returns FUTURE earnings.
    
    Returns list of dicts with 'period' (YYYY-MM-DD), 'actual', 'estimate', etc.
    """
    url = f"https://finnhub.io/api/v1/stock/earnings?symbol={symbol}&limit={limit}&token={token}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            if DEBUG:
                print(f"  [WARN] Finnhub earnings surprise request failed ({resp.status_code}) for {symbol}")
            return []
        data = resp.json()
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        if DEBUG:
            print(f"  [WARN] Finnhub earnings surprise request error for {symbol}: {e}")
        return []


def fetch_historical_earnings_cached(
    symbol: str,
    token: str,
    limit: int = 8,
    ttl_seconds: int = CACHE_TTL_SECONDS,
) -> List[Dict[str, Any]]:
    """
    Cached wrapper for historical earnings surprise data.
    
    Returns list of dicts with 'period' (quarter end date), 'actual', 'estimate', etc.
    Note: 'period' is the fiscal quarter end, not the announcement date. But for
    gap move analysis, the period date is close enough (usually within days of announcement).
    """
    k = f"HIST_EARNINGS|{symbol.upper()}|{limit}"
    now = time.time()

    cache = _load_cache()
    entries = cache.get("entries") or {}
    entry = entries.get(k)

    if isinstance(entry, dict):
        checked_at = float(entry.get("checked_at") or 0.0)
        if checked_at > 0 and (now - checked_at) <= ttl_seconds:
            data = entry.get("data")
            if isinstance(data, list):
                return data
            return []

    data = fetch_historical_earnings_surprise(symbol, token, limit)
    entries[k] = {"checked_at": now, "data": data}

    # Basic pruning
    if len(entries) > 4000:
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
