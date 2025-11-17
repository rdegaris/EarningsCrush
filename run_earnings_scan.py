"""
Earnings Crush Scanner - Daily Runner

Scans stocks with upcoming earnings and generates recommendations
for earnings crush trades based on volatility analysis.

Usage:
    python run_earnings_scan.py
"""

import json
import sys
from datetime import datetime, date
from pathlib import Path
import yfinance as yf
from calculator import compute_recommendation, get_current_price, filter_dates


def get_upcoming_earnings(tickers, days_ahead=30):
    """
    Get tickers with earnings in the next N days.
    
    Args:
        tickers: List of ticker symbols
        days_ahead: Number of days to look ahead for earnings
    
    Returns:
        List of (ticker, earnings_date, days_until) tuples
    """
    upcoming = []
    today = date.today()
    
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            
            # Get earnings date from calendar
            calendar = stock.calendar
            if calendar is not None and not calendar.empty:
                if 'Earnings Date' in calendar.index:
                    earnings_dates = calendar.loc['Earnings Date']
                    
                    # Handle single date or date range
                    if hasattr(earnings_dates, '__iter__') and not isinstance(earnings_dates, str):
                        # Take first date if range
                        earnings_date = earnings_dates[0] if len(earnings_dates) > 0 else None
                    else:
                        earnings_date = earnings_dates
                    
                    if earnings_date:
                        # Convert to date object
                        if hasattr(earnings_date, 'date'):
                            earnings_date = earnings_date.date()
                        elif isinstance(earnings_date, str):
                            earnings_date = datetime.strptime(earnings_date, '%Y-%m-%d').date()
                        
                        days_until = (earnings_date - today).days
                        
                        # Only include if within our timeframe and in the future
                        if 0 <= days_until <= days_ahead:
                            upcoming.append((ticker, earnings_date.strftime('%Y-%m-%d'), days_until))
                            print(f"  [INFO] {ticker}: Earnings in {days_until} days ({earnings_date})")
        except Exception as e:
            print(f"  [WARNING] Could not get earnings for {ticker}: {e}")
            continue
    
    # Sort by days until earnings
    upcoming.sort(key=lambda x: x[2])
    return upcoming


def run_earnings_scan(tickers, days_ahead=30):
    """
    Scan stocks with upcoming earnings and generate recommendations.
    
    Args:
        tickers: List of ticker symbols to scan
        days_ahead: Number of days to look ahead for earnings
    
    Returns:
        Dict with scan results
    """
    print("=" * 80)
    print("EARNINGS CRUSH SCANNER")
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
    print("\nAnalyzing options data...")
    print()
    
    opportunities = []
    recommended_count = 0
    consider_count = 0
    avoid_count = 0
    
    for ticker, earnings_date, days_until in upcoming_earnings:
        try:
            print(f"[SCAN] {ticker} (Earnings: {earnings_date}, {days_until} days)")
            
            # Get recommendation
            result = compute_recommendation(ticker)
            
            if isinstance(result, str):
                # Error message
                print(f"  [SKIP] {result}")
                continue
            
            # Get current price
            stock = yf.Ticker(ticker)
            price = get_current_price(stock)
            
            # Extract recommendation details
            avg_volume_pass = result['avg_volume']
            iv30_rv30_pass = result['iv30_rv30']
            ts_slope_pass = result['ts_slope_0_45']
            expected_move_str = result['expected_move']
            
            # Parse expected move percentage
            expected_move_pct = 0
            if expected_move_str:
                try:
                    expected_move_pct = float(expected_move_str.rstrip('%'))
                except:
                    pass
            
            expected_move_dollars = price * (expected_move_pct / 100)
            
            # Determine recommendation level
            if avg_volume_pass and iv30_rv30_pass and ts_slope_pass:
                recommendation = "RECOMMENDED"
                recommended_count += 1
            elif ts_slope_pass and ((avg_volume_pass and not iv30_rv30_pass) or (iv30_rv30_pass and not avg_volume_pass)):
                recommendation = "CONSIDER"
                consider_count += 1
            else:
                recommendation = "AVOID"
                avoid_count += 1
            
            # Get IV for the first expiration
            try:
                exp_dates = filter_dates(list(stock.options))
                if exp_dates:
                    chain = stock.option_chain(exp_dates[0])
                    calls = chain.calls
                    puts = chain.puts
                    
                    if not calls.empty and not puts.empty:
                        call_diffs = (calls['strike'] - price).abs()
                        call_idx = call_diffs.idxmin()
                        call_iv = calls.loc[call_idx, 'impliedVolatility']
                        
                        put_diffs = (puts['strike'] - price).abs()
                        put_idx = put_diffs.idxmin()
                        put_iv = puts.loc[put_idx, 'impliedVolatility']
                        
                        atm_iv = ((call_iv + put_iv) / 2.0) * 100
                    else:
                        atm_iv = 0
                else:
                    atm_iv = 0
            except:
                atm_iv = 0
            
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
                    'avg_volume': avg_volume_pass,
                    'iv30_rv30': iv30_rv30_pass,
                    'ts_slope_0_45': ts_slope_pass
                }
            }
            
            opportunities.append(opportunity)
            
            print(f"  [{recommendation}] Price: ${price:.2f}, IV: {atm_iv:.1f}%, Expected Move: ±{expected_move_pct:.1f}%")
            print(f"    Criteria: Vol={avg_volume_pass}, IV/RV={iv30_rv30_pass}, Slope={ts_slope_pass}")
            
        except Exception as e:
            print(f"  [ERROR] Failed to analyze {ticker}: {e}")
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
    # You can customize this list or load from a file
    # For now, using a common list of liquid stocks
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
    tickers = get_scan_universe()
    results = run_earnings_scan(tickers, days_ahead=30)
    
    if results:
        # Save to JSON
        output_file = 'earnings_crush_latest.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"[OK] Results saved to {output_file}")
        
        # Also save to web repo if it exists
        web_public = Path(__file__).parent.parent.parent / 'forward-volatility-web' / 'public'
        if web_public.exists():
            web_file = web_public / output_file
            with open(web_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"[OK] Results copied to {web_file}")
