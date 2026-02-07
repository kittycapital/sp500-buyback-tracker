import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

API_KEY = os.environ.get("FMP_API_KEY", "")
BATCH_SIZE = 50
DATA_FILE = "buyback_data.json"
TICKERS_FILE = "sp500_tickers.json"
BASE_URL = "https://financialmodelingprep.com/api/v3"


def api_get(endpoint):
    """Make a GET request to FMP API."""
    url = f"{BASE_URL}/{endpoint}&apikey={API_KEY}" if "?" in endpoint else f"{BASE_URL}/{endpoint}?apikey={API_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, Exception) as e:
        print(f"  API error for {endpoint}: {e}")
        return None


def load_sp500_list():
    """Load S&P 500 list from local JSON file (0 API calls)."""
    print("Loading S&P 500 list from local file...")
    if not os.path.exists(TICKERS_FILE):
        print(f"ERROR: {TICKERS_FILE} not found")
        return None
    with open(TICKERS_FILE, "r") as f:
        data = json.load(f)
    print(f"  Loaded {len(data)} tickers")
    return data


def fetch_cash_flow(symbol):
    """Fetch quarterly cash flow statement for a ticker (1 API call)."""
    data = api_get(f"cash-flow-statement/{symbol}?period=quarter&limit=20")
    if not data or not isinstance(data, list):
        return None

    quarters = []
    for q in data:
        buyback = q.get("commonStockRepurchased", 0) or 0
        shares = q.get("weightedAverageShsOut", 0) or 0
        shares_diluted = q.get("weightedAverageShsOutDil", 0) or 0

        quarters.append({
            "date": q.get("date", ""),
            "period": q.get("period", ""),
            "year": q.get("calendarYear", ""),
            "buyback_amount": buyback,
            "shares_outstanding": shares,
            "shares_diluted": shares_diluted,
            "free_cash_flow": q.get("freeCashFlow", 0) or 0,
        })

    return quarters


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
        json.dump(data, f, indent=2)
    print(f"Data saved to {DATA_FILE}")


def main():
    if not API_KEY:
        print("ERROR: FMP_API_KEY environment variable not set")
        sys.exit(1)

    db = load_data()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: Load S&P 500 list from local file (0 API calls)
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

    # Step 3: Fetch cash flow data for each ticker (50 API calls)
    success_count = 0
    fail_count = 0

    for i, symbol in enumerate(batch_symbols):
        print(f"  [{i+1}/{len(batch_symbols)}] Fetching {symbol}...", end=" ")
        quarters = fetch_cash_flow(symbol)

        if quarters:
            info = info_lookup.get(symbol, {})
            db["data"][symbol] = {
                "name": info.get("name", symbol),
                "sector": info.get("sector", "Unknown"),
                "subSector": "",
                "quarters": quarters,
                "last_fetched": now,
            }
            print(f"OK ({len(quarters)} quarters)")
            success_count += 1
        else:
            print("FAILED")
            fail_count += 1

        # Small delay to be respectful to API
        time.sleep(0.3)

    # Step 4: Update metadata
    db["last_updated"] = now
    db["batch_index"] = (batch_index + 1) % total_batches
    db["total_batches"] = total_batches

    if not db["collection_started"]:
        db["collection_started"] = now

    if batch_index + 1 == total_batches:
        db["full_cycles_completed"] = db.get("full_cycles_completed", 0) + 1
        print(f"\n🎉 Full cycle completed! (#{db['full_cycles_completed']})")

    # Step 5: Generate summary stats
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
    print(f"  API calls used: {len(batch_symbols)} (cash flows only, 0 for list)")

    save_data(db)


if __name__ == "__main__":
    main()
