import json
import os
import sys
import time
from datetime import datetime, timezone

try:
    import yfinance as yf
except ImportError:
    os.system("pip install yfinance --break-system-packages -q")
    import yfinance as yf

BATCH_SIZE = 50
DATA_FILE = "buyback_data.json"
TICKERS_FILE = "sp500_tickers.json"


def load_sp500_list():
    """Load S&P 500 list from local JSON file."""
    print("Loading S&P 500 list from local file...")
    if not os.path.exists(TICKERS_FILE):
        print(f"ERROR: {TICKERS_FILE} not found")
        return None
    with open(TICKERS_FILE, "r") as f:
        data = json.load(f)
    print(f"  Loaded {len(data)} tickers")
    return data


def fetch_buyback_data(symbol):
    """Fetch quarterly cash flow data using yfinance (free, no API key)."""
    try:
        ticker = yf.Ticker(symbol)

        # Get quarterly cash flow statement
        cf = ticker.quarterly_cashflow
        if cf is None or cf.empty:
            return None

        # Get current market cap for yield calculation
        info = ticker.fast_info
        market_cap = getattr(info, 'market_cap', 0) or 0

        quarters = []
        for col in cf.columns:
            date_str = col.strftime("%Y-%m-%d") if hasattr(col, 'strftime') else str(col)[:10]
            year = str(col.year) if hasattr(col, 'year') else date_str[:4]
            month = col.month if hasattr(col, 'month') else int(date_str[5:7])
            q_num = (month - 1) // 3 + 1

            # Extract buyback data - yfinance uses "Repurchase Of Capital Stock"
            buyback = 0
            for key in ['Repurchase Of Capital Stock', 'Common Stock Repurchased',
                        'RepurchaseOfCapitalStock']:
                if key in cf.index:
                    val = cf.loc[key, col]
                    if val is not None and str(val) != 'nan':
                        buyback = float(val)
                        break

            # Shares outstanding
            shares = 0
            for key in ['Diluted Average Shares', 'Basic Average Shares',
                        'DilutedAverageShares']:
                if key in cf.index:
                    val = cf.loc[key, col]
                    if val is not None and str(val) != 'nan':
                        shares = float(val)
                        break

            # If shares not in cash flow, try from balance sheet
            if shares == 0:
                shares = getattr(info, 'shares', 0) or 0

            # Free cash flow
            fcf = 0
            for key in ['Free Cash Flow', 'FreeCashFlow']:
                if key in cf.index:
                    val = cf.loc[key, col]
                    if val is not None and str(val) != 'nan':
                        fcf = float(val)
                        break

            quarters.append({
                "date": date_str,
                "period": f"Q{q_num}",
                "year": year,
                "buyback_amount": buyback,
                "shares_outstanding": shares,
                "shares_diluted": shares,
                "free_cash_flow": fcf,
            })

        return {"quarters": quarters, "market_cap": market_cap}

    except Exception as e:
        print(f"Error: {e}")
        return None


def load_data():
    """Load existing data file or create empty structure."""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {
        "last_updated": "",
        "batch_index": 0,
        "total_batches": 0,
        "sp500_list": [],
        "data": {},
        "collection_started": "",
        "full_cycles_completed": 0,
    }


def save_data(data):
    """Save data to JSON file."""
    with open(DATA_FILE, "w") as f:
        json.dump(data, f)
    # Check file size
    size_mb = os.path.getsize(DATA_FILE) / (1024 * 1024)
    print(f"Data saved to {DATA_FILE} ({size_mb:.1f} MB)")


def main():
    db = load_data()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: Load S&P 500 list from local file
    sp500 = load_sp500_list()
    if not sp500:
        print("Failed to load S&P 500 list. Exiting.")
        sys.exit(1)

    db["sp500_list"] = sp500
    symbols = [s["symbol"] for s in sp500]
    total_batches = (len(symbols) + BATCH_SIZE - 1) // BATCH_SIZE

    # Step 2: Determine current batch
    batch_index = db.get("batch_index", 0) % total_batches
    start = batch_index * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(symbols))
    batch_symbols = symbols[start:end]

    print(f"\nBatch {batch_index + 1}/{total_batches}: fetching {len(batch_symbols)} tickers")
    print(f"  Range: {batch_symbols[0]} ~ {batch_symbols[-1]}")

    # Build a lookup for name/sector from sp500 list
    info_lookup = {s["symbol"]: s for s in sp500}

    # Step 3: Fetch data for each ticker using yfinance
    success_count = 0
    fail_count = 0

    for i, symbol in enumerate(batch_symbols):
        print(f"  [{i+1}/{len(batch_symbols)}] Fetching {symbol}...", end=" ")
        result = fetch_buyback_data(symbol)

        if result and result["quarters"]:
            info = info_lookup.get(symbol, {})
            db["data"][symbol] = {
                "name": info.get("name", symbol),
                "sector": info.get("sector", "Unknown"),
                "subSector": "",
                "quarters": result["quarters"],
                "market_cap": result["market_cap"],
                "last_fetched": now,
            }
            buyback_total = sum(abs(min(q["buyback_amount"], 0)) for q in result["quarters"])
            print(f"OK ({len(result['quarters'])} quarters, buyback: ${buyback_total/1e9:.1f}B)")
            success_count += 1
        else:
            print("FAILED")
            fail_count += 1

        # Small delay to avoid rate limiting
        time.sleep(0.5)

    # Step 4: Update metadata
    db["last_updated"] = now
    db["batch_index"] = (batch_index + 1) % total_batches
    db["total_batches"] = total_batches

    if not db["collection_started"]:
        db["collection_started"] = now

    if batch_index + 1 == total_batches:
        db["full_cycles_completed"] = db.get("full_cycles_completed", 0) + 1
        print(f"\n🎉 Full cycle completed! (#{db['full_cycles_completed']})")

    # Step 5: Summary
    total_tickers_collected = len(db["data"])
    total_with_buybacks = sum(
        1 for v in db["data"].values()
        if any(q["buyback_amount"] < 0 for q in v.get("quarters", []))
    )

    print(f"\n--- Summary ---")
    print(f"  Tickers collected so far: {total_tickers_collected}/{len(symbols)}")
    print(f"  Tickers with buyback activity: {total_with_buybacks}")
    print(f"  Success: {success_count}, Failed: {fail_count}")
    print(f"  Next batch index: {db['batch_index']}")
    print(f"  No API key needed (yfinance)")

    save_data(db)


if __name__ == "__main__":
    main()
