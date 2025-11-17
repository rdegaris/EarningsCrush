# Earnings Crush Calculator

Scan stocks with upcoming earnings for volatility crush opportunities.

## ⚠️ Important: Use IB Scanner

**The recommended scanner is `run_earnings_scan_ib.py` which uses Interactive Brokers.**

### Why IB Scanner?
- ✅ Real-time market data
- ✅ Accurate option prices and IVs
- ✅ No rate limiting
- ✅ Generates exact trade suggestions with prices
- ✅ Production-ready

### Deprecated Files
- ❌ `calculator.py` - GUI tool (uses yfinance)
- ❌ `run_earnings_scan.py` - Batch scanner (uses yfinance, rate limited)

## Setup

### Requirements
```bash
pip install ib_insync requests
```

### Interactive Brokers Setup
1. Install IB Gateway or Trader Workstation
2. Enable API connections in Global Configuration > API > Settings
3. Set Socket Port:
   - TWS Paper: 7497
   - TWS Live: 7496
   - Gateway Paper: 4002
   - Gateway Live: 4001

## Usage

### Run IB Scanner
```bash
python run_earnings_scan_ib.py
```

This will:
1. Connect to Interactive Brokers
2. Scan configured tickers for upcoming earnings
3. Get real-time prices and option data
4. Generate trade recommendations
5. Create suggested calendar spreads
6. Output to `earnings_crush_latest.json`
7. Copy to web folder if available

### Output Format
```json
{
  "opportunities": [
    {
      "ticker": "PANW",
      "price": 202.90,
      "earnings_date": "2025-11-19",
      "days_to_earnings": 2,
      "iv": 81.5,
      "recommendation": "RECOMMENDED",
      "suggested_trade": {
        "strike": 205,
        "sell_expiration": "2025-11-22",
        "buy_expiration": "2025-12-19",
        "sell_dte": 5,
        "buy_dte": 32,
        "sell_price": 8.50,
        "buy_price": 12.30,
        "net_credit": -3.80
      }
    }
  ]
}
```

## Strategy

**Earnings Crush Calendar Spread:**
- Sell near-term ATM call (expires around earnings)
- Buy 30-day ATM call (protection)
- Profit from IV collapse after earnings
- Front month decays faster than back month

## Customization

Edit `get_scan_universe()` in `run_earnings_scan_ib.py` to change tickers.

Default scans:
- MAG7: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA
- Top NASDAQ100 names with liquid options
