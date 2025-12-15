"""
Earnings Crush Scanner using Finnhub + Interactive Brokers - Daily Runner

Data Sources:
- Earnings Dates: Finnhub API (no rate limits)
- Stock Prices: Interactive Brokers (real-time)
- Option Prices: Interactive Brokers (real-time market data)
- Implied Volatility: Interactive Brokers (greeks)
- Trade Suggestions: Interactive Brokers (actual market prices)

Scans stocks with upcoming earnings and generates recommendations
for earnings crush trades based on volatility analysis using IB data.

Usage:
    python run_earnings_scan_ib.py
"""

import json
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
import requests
import os

from earnings_cache import get_next_earnings_date_cached

try:
    from ib_insync import IB, Stock, Option, util
    IB_AVAILABLE = True
except ImportError:
    IB_AVAILABLE = False
    print("ERROR: ib_insync not installed")
    print("Install with: pip install ib_insync")
    sys.exit(1)


IB_HOST = os.environ.get("IB_HOST", "127.0.0.1")
IB_PORTS = [
    int(p)
    for p in os.environ.get("IB_PORTS", "7498,4002,7496,4001").split(",")
    if p.strip()
]
IB_CLIENT_ID = int(os.environ.get("IB_CLIENT_ID", "999"))


def get_upcoming_earnings(tickers, days_ahead=30):
    """
    Get tickers with earnings in the next N days using Finnhub API.
    
    Args:
        tickers: List of ticker symbols
        days_ahead: Number of days to look ahead for earnings
    
    Returns:
        List of (ticker, earnings_date, days_until) tuples
    """
    upcoming = []
    today = date.today()
    
    # Use Finnhub API key
    api_key = (os.environ.get('FINNHUB_API_KEY') or '').strip()
    if not api_key:
        print("  [ERROR] FINNHUB_API_KEY not set; cannot fetch upcoming earnings")
        return []
    
    # Get earnings calendar for next N days
    from_date = today.strftime('%Y-%m-%d')
    to_date = (today + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
    
    for ticker in tickers:
        try:
            date_str = get_next_earnings_date_cached(ticker, days_ahead=days_ahead, token=api_key)
            if date_str:
                earnings_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                days_until = (earnings_date - today).days

                # Only include if within our timeframe and in the future
                if 0 <= days_until <= days_ahead:
                    upcoming.append((ticker, date_str, days_until))
                    print(f"  [INFO] {ticker}: Earnings in {days_until} days ({date_str})")
        except Exception as e:
            print(f"  [WARNING] Could not get earnings for {ticker}: {e}")
            continue
    
    # Sort by days until earnings
    upcoming.sort(key=lambda x: x[2])
    return upcoming


def get_atm_strike(price):
    """Round to nearest liquid strike."""
    if price < 50:
        return round(price / 2.5) * 2.5
    elif price < 100:
        return round(price / 5) * 5
    elif price < 200:
        return round(price / 5) * 5
    else:
        return round(price / 10) * 10


def get_option_chain_ib(ib, ticker, strike, days_target_min, days_target_max):
    """
    Get option chain from IB for a specific strike and DTE range.
    
    Returns:
        List of (expiration, dte, call_contract) tuples
    """
    try:
        stock = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(stock)
        
        # Get all expirations
        chains = ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        
        if not chains:
            print(f"    [WARNING] No option chains found for {ticker}")
            return []
        
        # Get expirations from first chain (usually the main exchange)
        chain = chains[0]
        expirations = sorted(chain.expirations)
        
        print(f"    Found {len(expirations)} expirations")
        
        today = date.today()
        matching_exps = []
        
        for exp_str in expirations:
            exp_date = datetime.strptime(exp_str, '%Y%m%d').date()
            dte = (exp_date - today).days
            
            if days_target_min <= dte <= days_target_max:
                # Create call option contract
                call = Option(ticker, exp_str, strike, 'C', 'SMART')
                matching_exps.append((exp_str, dte, call))
                print(f"      Match: {exp_str} (DTE: {dte})")
        
        return matching_exps
        
    except Exception as e:
        print(f"    [WARNING] Error getting option chain for {ticker}: {e}")
        return []


def get_option_price_and_iv(ib, option_contract, stock_price=None):
    """
    Get market price and IV for an option from IB.
    
    Returns:
        Tuple of (mid_price, implied_vol) or (None, None) if unavailable
    """
    try:
        # Qualify the contract first
        contracts = ib.qualifyContracts(option_contract)
        if not contracts:
            print(f"      Could not qualify contract")
            return None, None
        
        qualified_contract = contracts[0]
        
        # Request market data with greeks (106 = option IV)
        ticker = ib.reqMktData(qualified_contract, '106', False, False)
        
        # Wait for data to populate (2 seconds like the working scanner)
        ib.sleep(2)
        
        if not ticker:
            print(f"      No ticker data received")
            ib.cancelMktData(qualified_contract)
            return None, None
        
        # Get mid price
        bid = ticker.bid if ticker.bid and ticker.bid > 0 else None
        ask = ticker.ask if ticker.ask and ticker.ask > 0 else None
        last = ticker.last if ticker.last and ticker.last > 0 else None
        
        if bid and ask:
            mid = (bid + ask) / 2.0
        elif last:
            mid = last
        elif bid:
            mid = bid
        elif ask:
            mid = ask
        else:
            mid = None
        
        # Get IV - try multiple methods
        iv = None
        
        # Method 1: modelGreeks (pre-calculated by IB for liquid options)
        if ticker.modelGreeks and ticker.modelGreeks.impliedVol:
            iv = ticker.modelGreeks.impliedVol
        elif ticker.bidGreeks and ticker.bidGreeks.impliedVol:
            iv = ticker.bidGreeks.impliedVol
        elif ticker.askGreeks and ticker.askGreeks.impliedVol:
            iv = ticker.askGreeks.impliedVol
        elif ticker.lastGreeks and ticker.lastGreeks.impliedVol:
            iv = ticker.lastGreeks.impliedVol
        
        # Method 2: Calculate IV from option price if modelGreeks not available
        # This is critical for less liquid stocks where IB doesn't provide modelGreeks
        if not iv and mid and mid > 0 and stock_price and stock_price > 0:
            try:
                calc_result = ib.calculateImpliedVolatility(
                    qualified_contract, 
                    mid,  # option price
                    stock_price  # underlying price
                )
                ib.sleep(1)  # Wait for calculation
                if calc_result and hasattr(calc_result, 'impliedVolatility') and calc_result.impliedVolatility:
                    iv = calc_result.impliedVolatility
                    print(f"      IV calculated from price: {iv*100:.1f}%")
            except Exception as e:
                print(f"      Could not calculate IV: {e}")
        
        # Cancel market data
        ib.cancelMktData(qualified_contract)
        
        return mid, iv
        
    except Exception as e:
        print(f"      Error getting option data: {e}")
        return None, None


def run_earnings_scan_ib(ib, tickers, days_ahead=30):
    """
    Scan stocks with upcoming earnings and generate recommendations using IB data.
    
    Args:
        ib: Connected IB instance
        tickers: List of ticker symbols to scan
        days_ahead: Number of days to look ahead for earnings
    
    Returns:
        Dict with scan results
    """
    print("=" * 80)
    print("EARNINGS CRUSH SCANNER (IB)")
    print("=" * 80)
    print()
    
    print(f"Scanning {len(tickers)} tickers for earnings in next {days_ahead} days...")
    print()
    
    # Get stocks with upcoming earnings
    upcoming_earnings = get_upcoming_earnings(tickers, days_ahead)
    
    if not upcoming_earnings:
        print("[INFO] No upcoming earnings found in the specified timeframe")
        return {
            'timestamp': datetime.now().isoformat(),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'total_scanned': len(tickers),
            'earnings_found': 0,
            'opportunities': [],
            'summary': {
                'total_recommended': 0,
                'total_consider': 0,
                'total_avoid': 0,
                'avg_iv': 0,
                'avg_expected_move': 0
            }
        }
    
    print(f"\nFound {len(upcoming_earnings)} stocks with upcoming earnings")
    print("\nAnalyzing options data from IB...")
    print()
    
    opportunities = []
    recommended_count = 0
    consider_count = 0
    avoid_count = 0
    
    for ticker, earnings_date, days_until in upcoming_earnings:
        try:
            print(f"[SCAN] {ticker} (Earnings: {earnings_date}, {days_until} days)")
            
            # Get stock price from IB
            stock = Stock(ticker, 'SMART', 'USD')
            ib.qualifyContracts(stock)
            ib.reqMktData(stock, '', False, False)
            ib.sleep(1)
            
            stock_ticker = ib.reqTickers(stock)[0]
            if stock_ticker.marketPrice() and stock_ticker.marketPrice() > 0:
                price = stock_ticker.marketPrice()
            else:
                print(f"  [SKIP] Could not get price for {ticker}")
                ib.cancelMktData(stock)
                continue
            
            ib.cancelMktData(stock)
            
            # Get ATM strike
            atm_strike = get_atm_strike(price)
            
            # Get front month options (near earnings - first expiry after earnings)
            front_options = get_option_chain_ib(ib, ticker, atm_strike, 
                                               max(1, days_until - 3), 
                                               days_until + 7)
            
            if not front_options:
                print(f"  [SKIP] No front month options found")
                continue
            
            # Use first matching expiration for front
            front_exp, front_dte, front_call = front_options[0]
            
            # Get back month options (~30 days from FRONT expiry, not from today)
            # Calculate target DTE range: front_dte + 25 to front_dte + 35
            back_dte_min = front_dte + 25
            back_dte_max = front_dte + 35
            back_options = get_option_chain_ib(ib, ticker, atm_strike, back_dte_min, back_dte_max)
            
            if not back_options:
                print(f"  [SKIP] No back month options found (need {back_dte_min}-{back_dte_max} DTE)")
                continue
            
            # Use first matching expiration for back
            back_exp, back_dte, back_call = back_options[0]
            
            print(f"    Front: {front_exp} ({front_dte} DTE), Back: {back_exp} ({back_dte} DTE), Gap: {back_dte - front_dte} days")
            
            # Get option prices and IVs (pass stock price for IV calculation fallback)
            print(f"    Getting front month option data...")
            front_price, front_iv = get_option_price_and_iv(ib, front_call, price)
            
            print(f"    Getting back month option data...")
            back_price, back_iv = get_option_price_and_iv(ib, back_call, price)
            
            if not front_price or not back_price:
                print(f"  [SKIP] Could not get option prices (Front: {front_price}, Back: {back_price})")
                continue
            
            if not front_iv or not back_iv:
                print(f"  [WARNING] Missing IV data (Front IV: {front_iv}, Back IV: {back_iv})")
                # Use a default IV estimate based on option price if IV not available
                if not front_iv:
                    front_iv = 0.5  # Default to 50%
                if not back_iv:
                    back_iv = 0.4  # Default to 40%
            
            # Calculate metrics
            atm_iv = front_iv * 100  # Convert to percentage
            back_iv_pct = back_iv * 100
            
            # IV term structure slope (front vs back)
            # Positive slope = front IV higher than back (good for earnings crush)
            iv_slope = front_iv - back_iv  # In decimal form
            iv_slope_pct = (front_iv / back_iv - 1) * 100 if back_iv > 0 else 0  # % premium
            
            # Estimate expected move based on straddle
            straddle_price = front_price * 2  # Approximate (call price * 2 for ATM)
            expected_move_pct = (straddle_price / price) * 100
            expected_move_dollars = straddle_price
            
            # Recommendation logic incorporating IV slope
            # For earnings crush, we NEED front IV > back IV (positive term structure)
            # RECOMMENDED: High IV (>60%), front > back, close to earnings
            # CONSIDER: Moderate IV (>50%), front > back
            # AVOID: Low IV, or front IV <= back IV (no crush opportunity)
            
            has_positive_slope = front_iv > back_iv
            
            if not has_positive_slope:
                # No term structure edge - can't profit from IV crush
                recommendation = "AVOID"
                avoid_count += 1
            elif atm_iv > 60 and days_until <= 5 and iv_slope_pct > 10:
                # High IV, good slope (>10% premium), close to earnings
                recommendation = "RECOMMENDED"
                recommended_count += 1
            elif atm_iv > 50 and days_until <= 7 and iv_slope_pct > 5:
                # Moderate IV, decent slope
                recommendation = "CONSIDER"
                consider_count += 1
            else:
                recommendation = "AVOID"
                avoid_count += 1
            
            # Create suggested trade
            suggested_trade = {
                'strike': float(atm_strike),
                'sell_expiration': datetime.strptime(front_exp, '%Y%m%d').strftime('%Y-%m-%d'),
                'buy_expiration': datetime.strptime(back_exp, '%Y%m%d').strftime('%Y-%m-%d'),
                'sell_dte': front_dte,
                'buy_dte': back_dte,
                'sell_price': round(front_price, 2),
                'buy_price': round(back_price, 2),
                'net_credit': round(front_price - back_price, 2)
            }
            
            opportunity = {
                'ticker': ticker,
                'price': round(price, 2),
                'earnings_date': earnings_date,
                'days_to_earnings': days_until,
                'iv': round(atm_iv, 1),
                'expected_move': round(expected_move_dollars, 2),
                'expected_move_pct': round(expected_move_pct, 1),
                'recommendation': recommendation,
                'criteria': {
                    'avg_volume': True,  # Assume liquid stocks
                    'iv30_rv30': atm_iv > 50,
                    'ts_slope_positive': has_positive_slope,  # Front IV > Back IV
                    'iv_slope_pct': round(iv_slope_pct, 1)  # % premium of front over back
                },
                'front_iv': round(atm_iv, 1),
                'back_iv': round(back_iv_pct, 1),
                'suggested_trade': suggested_trade
            }
            
            opportunities.append(opportunity)
            
            slope_str = f"+{iv_slope_pct:.1f}%" if iv_slope_pct > 0 else f"{iv_slope_pct:.1f}%"
            print(f"  [{recommendation}] Price: ${price:.2f}, Front IV: {atm_iv:.1f}%, Back IV: {back_iv_pct:.1f}%, Slope: {slope_str}")
            print(f"    Expected Move: ±{expected_move_pct:.1f}%")
            print(f"    Trade: Sell {suggested_trade['sell_expiration']} / Buy {suggested_trade['buy_expiration']} ${atm_strike} CALL")
            print(f"    Net Credit: ${suggested_trade['net_credit']:.2f} (Sell ${suggested_trade['sell_price']:.2f} - Buy ${suggested_trade['buy_price']:.2f})")
            
        except Exception as e:
            import traceback
            print(f"  [ERROR] Failed to analyze {ticker}: {e}")
            print(f"    {traceback.format_exc()}")
            continue
    
    # Calculate summary statistics
    if opportunities:
        avg_iv = sum(opp['iv'] for opp in opportunities) / len(opportunities)
        avg_expected_move = sum(opp['expected_move_pct'] for opp in opportunities) / len(opportunities)
    else:
        avg_iv = 0
        avg_expected_move = 0
    
    print()
    print("=" * 80)
    print("SCAN COMPLETE")
    print("=" * 80)
    print(f"Total analyzed: {len(opportunities)}")
    print(f"Recommended: {recommended_count}")
    print(f"Consider: {consider_count}")
    print(f"Avoid: {avoid_count}")
    print()
    
    return {
        'timestamp': datetime.now().isoformat(),
        'date': datetime.now().strftime('%Y-%m-%d'),
        'total_scanned': len(tickers),
        'earnings_found': len(upcoming_earnings),
        'opportunities': opportunities,
        'summary': {
            'total_recommended': recommended_count,
            'total_consider': consider_count,
            'total_avoid': avoid_count,
            'avg_iv': round(avg_iv, 1),
            'avg_expected_move': round(avg_expected_move, 1)
        }
    }


def get_scan_universe():
    """Get list of tickers to scan for earnings."""
    from_mag7 = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA']
    
    from_nasdaq100 = [
        'ADBE', 'AMD', 'ABNB', 'AVGO', 'BKNG', 'CMCSA', 'COST', 'CSCO', 
        'CRWD', 'DDOG', 'DIS', 'EA', 'GILD', 'INTC', 'INTU', 'ISRG',
        'KLAC', 'LRCX', 'MELI', 'MRNA', 'NFLX', 'NOW', 'PANW', 'PYPL',
        'QCOM', 'SBUX', 'SHOP', 'SNOW', 'TEAM', 'TTWO', 'UBER', 'WDAY', 'ZS'
    ]
    
    # Combine and deduplicate
    all_tickers = list(set(from_mag7 + from_nasdaq100))
    all_tickers.sort()
    
    return all_tickers


if __name__ == "__main__":
    if not IB_AVAILABLE:
        print("ERROR: ib_insync library not available")
        sys.exit(1)
    
    # Connect to IB
    ib = IB()
    try:
        print("Connecting to Interactive Brokers...")
        print("Make sure IB Gateway or TWS is running with API enabled")
        print()
        
        # Try common ports
        connected = False
        for port in IB_PORTS:
            try:
                ib.connect(IB_HOST, port, clientId=IB_CLIENT_ID)
                connected = True
                print(f"✓ Connected on port {port}")
                break
            except:
                continue
        
        if not connected:
            print("ERROR: Could not connect to IB Gateway/TWS")
            print("Make sure:")
            print("  1. IB Gateway or TWS is running")
            print("  2. API connections are enabled")
            print("  3. Socket port is correct (7498 for TWS paper, 4002 for Gateway paper)")
            sys.exit(1)
        
        print()
        
        # Run scan
        tickers = get_scan_universe()
        results = run_earnings_scan_ib(ib, tickers, days_ahead=30)
        
        if results:
            # Save to JSON
            output_file = 'earnings_crush_latest.json'
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"[OK] Results saved to {output_file}")
            print()
        
        ib.disconnect()
        print("Disconnected from IB")
        
        # Exit with success if we got results
        if results:
            sys.exit(0)
        else:
            print("[ERROR] Scan returned no results")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n[ERROR] Scan failed with exception: {e}")
        import traceback
        traceback.print_exc()
        try:
            ib.disconnect()
        except:
            pass
        sys.exit(1)
