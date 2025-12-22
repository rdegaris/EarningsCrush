"""
Pre-Earnings Long Straddle Scanner (Finnhub + Interactive Brokers)

Goal (per provided transcript):
- Look ~14 days before earnings
- Buy an ATM straddle that expires AFTER earnings (prefer the nearest monthly after earnings)
- Exit before earnings (scanner only provides candidates; execution/exit is manual)

This scanner focuses on:
- Current implied move from ATM straddle (IB market data)
- Historical realized earnings *gap* moves (Finnhub earnings calendar + IB daily bars)
- Simple relative value signals: implied vs historical realized

Output:
- pre_earnings_straddle_latest.json

Usage:
    python run_preearnings_straddle_scan_ib.py

Notes:
- Requires IB Gateway or TWS with API enabled.
- Requires FINNHUB_API_KEY env var.
"""

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pathlib import Path

import requests

from earnings_cache import fetch_earnings_calendar_cached

try:
    from ib_insync import IB, Stock, Option

    IB_AVAILABLE = True
except ImportError:
    IB_AVAILABLE = False


def _load_env_file(path: Path) -> bool:
    """Load KEY=VALUE lines into os.environ (no overrides).

    Lightweight .env reader to support Task Scheduler / .bat runs.
    """
    try:
        if not path.exists() or not path.is_file():
            return False
        loaded_any = False
        for raw in path.read_text(encoding='utf-8').splitlines():
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            if line.lower().startswith('export '):
                line = line[7:].strip()
            if '=' not in line:
                continue
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if not key:
                continue
            if key in os.environ and (os.environ.get(key) or '').strip():
                continue
            os.environ[key] = value
            loaded_any = True
        return loaded_any
    except Exception:
        return False


def load_local_secrets() -> None:
    script_dir = Path(__file__).parent.resolve()
    for candidate in (script_dir / '.secrets.env', script_dir / '.env'):
        _load_env_file(candidate)


load_local_secrets()


# --- Config ---
ENTRY_DAYS_TARGET = 14
ENTRY_WINDOW_DAYS = 3  # scan earnings in [target-window, target+window]
EARNINGS_LOOKAHEAD_DAYS = 45
HISTORICAL_EARNINGS_LOOKBACK_DAYS = 730
HISTORICAL_EVENTS_MAX = 6

# Liquidity / data sanity
MAX_REL_BID_ASK_SPREAD = 0.35  # skip if (ask-bid)/mid too wide


IB_HOST = os.environ.get("IB_HOST", "127.0.0.1")
IB_PORTS = [
    int(p)
    for p in os.environ.get("IB_PORTS", "7498,4002,7496,4001").split(",")
    if p.strip()
]
IB_CLIENT_ID = int(os.environ.get("IB_CLIENT_ID", "1001"))


def _try_import_forward_vol_lists() -> tuple[list[str] | None, list[str] | None]:
    """Try to import NASDAQ100 + MidCap400 lists from forward-volatility-calculator.

    This repo lives alongside this scanner in the workspace, but not as a Python package.
    We add it to sys.path at runtime to reuse its ticker list sources.
    """
    try:
        cta_root = Path(__file__).resolve().parents[2]  # .../CTA Business
        fv_calc = cta_root / "Forward Volatility" / "forward-volatility-calculator"
        if not fv_calc.exists():
            return None, None

        if str(fv_calc) not in sys.path:
            sys.path.insert(0, str(fv_calc))

        from nasdaq100 import get_nasdaq_100_list  # type: ignore
        from midcap400 import get_midcap400_list  # type: ignore

        return list(get_nasdaq_100_list()), list(get_midcap400_list())
    except Exception:
        return None, None


@dataclass
class HistoricalMove:
    earnings_date: str
    hour: Optional[str]
    realized_move_pct: float


def _finnhub_key() -> str:
    return (os.environ.get("FINNHUB_API_KEY") or "").strip()


def get_atm_strike(price: float) -> float:
    """Round to a typical liquid strike increment."""
    if price < 50:
        return round(price / 2.5) * 2.5
    if price < 200:
        return round(price / 5) * 5
    return round(price / 10) * 10


def fetch_earnings_calendar(
    symbol: str,
    from_date: date,
    to_date: date,
) -> List[Dict[str, Any]]:
    token = _finnhub_key()
    if not token:
        return []
    return fetch_earnings_calendar_cached(symbol, from_date, to_date, token)


def get_next_earnings_within(
    tickers: Sequence[str],
    days_ahead: int,
) -> List[Tuple[str, str, int]]:
    """Return (ticker, earnings_date_str, days_until) for upcoming earnings."""
    today = date.today()
    upcoming: List[Tuple[str, str, int]] = []

    from_d = today
    to_d = today + timedelta(days=days_ahead)

    for ticker in tickers:
        try:
            cal = fetch_earnings_calendar(ticker, from_d, to_d)
            if not cal:
                continue

            # Finnhub typically returns sorted nearest-first
            first = cal[0]
            date_str = first.get("date")
            if not date_str:
                continue
            e_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            days_until = (e_date - today).days
            if 0 <= days_until <= days_ahead:
                upcoming.append((ticker, date_str, days_until))
        except Exception:
            continue

    upcoming.sort(key=lambda x: x[2])
    return upcoming


def is_monthly_expiration(expiry_yyyymmdd: str) -> bool:
    """Approximate: monthly equity options are the 3rd Friday (day 15-21)."""
    try:
        exp = datetime.strptime(expiry_yyyymmdd, "%Y%m%d").date()
    except Exception:
        return False
    return exp.weekday() == 4 and 15 <= exp.day <= 21


def get_expirations_ib(ib: IB, ticker: str) -> List[str]:
    stock = Stock(ticker, "SMART", "USD")
    ib.qualifyContracts(stock)

    chains = ib.reqSecDefOptParams(stock.symbol, "", stock.secType, stock.conId)
    if not chains:
        return []

    expirations = sorted(chains[0].expirations)
    return expirations


def pick_straddle_expiration_after_earnings(
    expirations: Sequence[str],
    days_until_earnings: int,
) -> Optional[Tuple[str, int]]:
    """Pick the nearest *monthly* expiration after earnings; fallback to nearest after."""
    today = date.today()

    candidates: List[Tuple[str, int]] = []
    for exp_str in expirations:
        try:
            exp_date = datetime.strptime(exp_str, "%Y%m%d").date()
        except Exception:
            continue
        dte = (exp_date - today).days
        if dte < days_until_earnings + 1:
            continue
        if dte > days_until_earnings + 90:
            continue
        candidates.append((exp_str, dte))

    if not candidates:
        return None

    monthlies = [c for c in candidates if is_monthly_expiration(c[0])]
    if monthlies:
        monthlies.sort(key=lambda x: x[1])
        return monthlies[0]

    candidates.sort(key=lambda x: x[1])
    return candidates[0]


def get_option_quote(ib: IB, opt: Option, stock_price: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (mid, bid, ask) for an option contract."""
    contracts = ib.qualifyContracts(opt)
    if not contracts:
        return None, None, None

    qc = contracts[0]
    tkr = ib.reqMktData(qc, "", False, False)
    ib.sleep(2)

    bid = tkr.bid if tkr.bid and tkr.bid > 0 else None
    ask = tkr.ask if tkr.ask and tkr.ask > 0 else None
    last = tkr.last if tkr.last and tkr.last > 0 else None

    if bid is not None and ask is not None:
        mid = (bid + ask) / 2.0
    elif last is not None:
        mid = last
    elif bid is not None:
        mid = bid
    elif ask is not None:
        mid = ask
    else:
        mid = None

    ib.cancelMktData(qc)
    return mid, bid, ask


def spread_ok(bid: Optional[float], ask: Optional[float], mid: Optional[float]) -> bool:
    if bid is None or ask is None or mid is None or mid <= 0:
        # If we don't have both sides, we still allow (some accounts only see last)
        return True
    spr = ask - bid
    rel = spr / mid if mid else 0
    return rel <= MAX_REL_BID_ASK_SPREAD


def fetch_historical_gap_moves(
    ib: IB,
    ticker: str,
    max_events: int = HISTORICAL_EVENTS_MAX,
) -> List[HistoricalMove]:
    """Compute realized earnings gap moves for recent historical earnings."""
    today = date.today()
    from_d = today - timedelta(days=HISTORICAL_EARNINGS_LOOKBACK_DAYS)
    to_d = today

    cal = fetch_earnings_calendar(ticker, from_d, to_d)
    # Keep only past events (strictly < today)
    events = []
    for e in cal:
        d = e.get("date")
        if not d:
            continue
        try:
            ed = datetime.strptime(d, "%Y-%m-%d").date()
        except Exception:
            continue
        if ed >= today:
            continue
        events.append(e)

    # Sort most recent first
    events.sort(key=lambda x: x.get("date", ""), reverse=True)
    events = events[:max_events]

    if not events:
        return []

    stock = Stock(ticker, "SMART", "USD")
    ib.qualifyContracts(stock)

    out: List[HistoricalMove] = []

    for e in events:
        d_str = e.get("date")
        hour = e.get("hour")  # often 'bmo'/'amc' or None
        try:
            ed = datetime.strptime(d_str, "%Y-%m-%d").date()
        except Exception:
            continue

        # Request a handful of daily bars around the event.
        end_dt = datetime.combine(ed + timedelta(days=3), datetime.min.time()).strftime("%Y%m%d %H:%M:%S")

        bars = ib.reqHistoricalData(
            stock,
            endDateTime=end_dt,
            durationStr="10 D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )

        # Map bars by date
        by_date: Dict[date, Any] = {}
        for b in bars or []:
            try:
                bd = b.date.date() if hasattr(b.date, "date") else datetime.strptime(str(b.date), "%Y-%m-%d").date()
            except Exception:
                continue
            by_date[bd] = b

        prev_day = ed - timedelta(days=1)
        next_day = ed + timedelta(days=1)

        bar_prev = by_date.get(prev_day)
        bar_e = by_date.get(ed)
        bar_next = by_date.get(next_day)

        realized: Optional[float] = None

        # Prefer open gaps using bmo/amc if available
        if hour and hour.lower() == "bmo":
            if bar_prev and bar_e and getattr(bar_prev, "close", None) and getattr(bar_e, "open", None):
                realized = abs(bar_e.open - bar_prev.close) / bar_prev.close
        elif hour and hour.lower() == "amc":
            if bar_e and bar_next and getattr(bar_e, "close", None) and getattr(bar_next, "open", None):
                realized = abs(bar_next.open - bar_e.close) / bar_e.close

        # Fallbacks
        if realized is None:
            if bar_e and bar_next and getattr(bar_e, "close", None) and getattr(bar_next, "close", None):
                realized = abs(bar_next.close - bar_e.close) / bar_e.close

        if realized is None:
            continue

        out.append(
            HistoricalMove(
                earnings_date=d_str,
                hour=hour,
                realized_move_pct=round(realized * 100.0, 2),
            )
        )

        # small pause to reduce pacing violations
        ib.sleep(0.3)

    return out


def get_scan_universe() -> List[str]:
    # Desired: scan NASDAQ100 + MidCap400 (drop MAG7 special-casing).
    n100, mid400 = _try_import_forward_vol_lists()
    if n100 and mid400:
        return sorted(set([t.strip().upper() for t in (n100 + mid400) if str(t).strip()]))

    # Fallback: previous small universe (kept only as a safety net).
    from_nasdaq100 = [
        "ADBE",
        "AMD",
        "ABNB",
        "AVGO",
        "BKNG",
        "CMCSA",
        "COST",
        "CSCO",
        "CRWD",
        "DDOG",
        "DIS",
        "EA",
        "GILD",
        "INTC",
        "INTU",
        "ISRG",
        "KLAC",
        "LRCX",
        "MELI",
        "MRNA",
        "NFLX",
        "NOW",
        "PANW",
        "PYPL",
        "QCOM",
        "SBUX",
        "SHOP",
        "SNOW",
        "TEAM",
        "TTWO",
        "UBER",
        "WDAY",
        "ZS",
    ]

    return sorted(set(from_nasdaq100))


def run_scan(ib: IB, tickers: Sequence[str]) -> Dict[str, Any]:
    print("=" * 80)
    print("PRE-EARNINGS LONG STRADDLE SCANNER (IB)")
    print("=" * 80)
    print()

    upcoming = get_next_earnings_within(tickers, days_ahead=EARNINGS_LOOKAHEAD_DAYS)

    min_days = ENTRY_DAYS_TARGET - ENTRY_WINDOW_DAYS
    max_days = ENTRY_DAYS_TARGET + ENTRY_WINDOW_DAYS

    candidates = [x for x in upcoming if min_days <= x[2] <= max_days]

    print(f"Universe: {len(tickers)} tickers")
    print(f"Upcoming earnings (<= {EARNINGS_LOOKAHEAD_DAYS}d): {len(upcoming)}")
    print(f"Entry window: {min_days}-{max_days} days -> {len(candidates)} candidates")
    print()

    opportunities: List[Dict[str, Any]] = []

    for ticker, earnings_date, days_until in candidates:
        try:
            print(f"[SCAN] {ticker} (Earnings: {earnings_date}, {days_until} days)")

            stock = Stock(ticker, "SMART", "USD")
            ib.qualifyContracts(stock)
            ib.reqMktData(stock, "", False, False)
            ib.sleep(1)

            stk_tkr = ib.reqTickers(stock)[0]
            price = stk_tkr.marketPrice() if stk_tkr.marketPrice() and stk_tkr.marketPrice() > 0 else None
            ib.cancelMktData(stock)

            if not price:
                print("  [SKIP] Could not get stock price")
                continue

            atm_strike = get_atm_strike(price)

            expirations = get_expirations_ib(ib, ticker)
            if not expirations:
                print("  [SKIP] No expirations")
                continue

            picked = pick_straddle_expiration_after_earnings(expirations, days_until)
            if not picked:
                print("  [SKIP] No suitable expiration after earnings")
                continue

            exp_str, exp_dte = picked
            exp_date_fmt = datetime.strptime(exp_str, "%Y%m%d").strftime("%Y-%m-%d")
            is_monthly = is_monthly_expiration(exp_str)

            call = Option(ticker, exp_str, atm_strike, "C", "SMART")
            put = Option(ticker, exp_str, atm_strike, "P", "SMART")

            call_mid, call_bid, call_ask = get_option_quote(ib, call, price)
            put_mid, put_bid, put_ask = get_option_quote(ib, put, price)

            if call_mid is None or put_mid is None or call_mid <= 0 or put_mid <= 0:
                print("  [SKIP] Missing option mids")
                continue

            if not spread_ok(call_bid, call_ask, call_mid) or not spread_ok(put_bid, put_ask, put_mid):
                print("  [SKIP] Options too wide / illiquid")
                continue

            straddle_mid = call_mid + put_mid
            implied_move_pct = (straddle_mid / price) * 100.0

            # Historical realized earnings moves
            hist = fetch_historical_gap_moves(ib, ticker)
            realized_avg = (sum(x.realized_move_pct for x in hist) / len(hist)) if hist else None
            realized_last = hist[0].realized_move_pct if hist else None

            ratio_avg = (implied_move_pct / realized_avg) if (realized_avg and realized_avg > 0) else None
            ratio_last = (implied_move_pct / realized_last) if (realized_last and realized_last > 0) else None

            # Simple score: bigger positive = more "cheap" implied vs realized
            score = None
            if realized_avg and implied_move_pct > 0:
                score = (realized_avg - implied_move_pct) / implied_move_pct

            # Recommendation heuristic
            # (kept conservative until we have more historical implied-move data)
            recommendation = "WATCH"
            if ratio_avg is not None and ratio_last is not None:
                if ratio_avg <= 0.90 and ratio_last <= 0.95:
                    recommendation = "CANDIDATE"
                elif ratio_avg <= 1.00:
                    recommendation = "WATCH"
                else:
                    recommendation = "PASS"

            opportunities.append(
                {
                    "ticker": ticker,
                    "price": round(price, 2),
                    "earnings_date": earnings_date,
                    "days_to_earnings": days_until,
                    "expiry": exp_date_fmt,
                    "expiry_dte": exp_dte,
                    "expiry_is_monthly": is_monthly,
                    "strike": float(atm_strike),
                    "call_mid": round(call_mid, 2),
                    "put_mid": round(put_mid, 2),
                    "straddle_mid": round(straddle_mid, 2),
                    "implied_move_pct": round(implied_move_pct, 2),
                    "historical_realized_moves": [asdict(x) for x in hist],
                    "realized_move_avg_pct": round(realized_avg, 2) if realized_avg is not None else None,
                    "realized_move_last_pct": round(realized_last, 2) if realized_last is not None else None,
                    "ratio_implied_to_avg_realized": round(ratio_avg, 3) if ratio_avg is not None else None,
                    "ratio_implied_to_last_realized": round(ratio_last, 3) if ratio_last is not None else None,
                    "score": round(score, 3) if score is not None else None,
                    "recommendation": recommendation,
                }
            )

            print(
                f"  {recommendation}: ${price:.2f} ATM {atm_strike} exp {exp_date_fmt}"
                f" | straddle ${straddle_mid:.2f} | implied ±{implied_move_pct:.2f}%"
            )
            if realized_avg is not None:
                print(f"    Hist realized avg: {realized_avg:.2f}% | last: {realized_last:.2f}%")

            ib.sleep(0.5)

        except Exception as e:
            print(f"  [ERROR] Failed {ticker}: {e}")
            continue

    # Sort by score descending (most attractive first)
    opportunities.sort(key=lambda x: (x.get("score") is not None, x.get("score") or -1e9), reverse=True)

    return {
        "timestamp": datetime.now().isoformat(),
        "date": datetime.now().strftime("%Y-%m-%d"),
        "entry_target_days": ENTRY_DAYS_TARGET,
        "entry_window_days": ENTRY_WINDOW_DAYS,
        "universe_size": len(tickers),
        "earnings_found": len(upcoming),
        "candidates_scanned": len(candidates),
        "opportunities": opportunities,
        "summary": {
            "total_opportunities": len(opportunities),
            "total_candidate": sum(1 for o in opportunities if o.get("recommendation") == "CANDIDATE"),
            "total_watch": sum(1 for o in opportunities if o.get("recommendation") == "WATCH"),
            "total_pass": sum(1 for o in opportunities if o.get("recommendation") == "PASS"),
        },
    }


def main() -> int:
    if not _finnhub_key():
        print("ERROR: FINNHUB_API_KEY not set; cannot fetch earnings calendar")
        print("Set FINNHUB_API_KEY in your environment and re-run")
        return 1

    if not IB_AVAILABLE:
        print("ERROR: ib_insync not installed")
        print("Install with: pip install ib_insync")
        return 1

    ib = IB()
    success = False
    try:
        print("Connecting to Interactive Brokers...")
        print("Make sure IB Gateway or TWS is running with API enabled")
        print()

        connected = False
        for port in IB_PORTS:
            try:
                ib.connect(IB_HOST, port, clientId=IB_CLIENT_ID)
                connected = True
                print(f"✓ Connected on port {port}")
                break
            except Exception:
                continue

        if not connected:
            print("ERROR: Could not connect to IB Gateway/TWS")
            return 1

        tickers = get_scan_universe()
        results = run_scan(ib, tickers)

        out_file = "pre_earnings_straddle_latest.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)

        success = True

        print()
        print(f"[OK] Results saved to {out_file}")
        return 0

    except Exception as e:
        print(f"[ERROR] Scan failed: {e}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass

        # Workaround: on Windows we sometimes see an access-violation crash on interpreter
        # shutdown after ib_insync usage, even when the scan completed successfully.
        # If the run succeeded, hard-exit with code 0 to avoid a false failure.
        if success and os.name == "nt":
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass
            os._exit(0)


if __name__ == "__main__":
    raise SystemExit(main())
