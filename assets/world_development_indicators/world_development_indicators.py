"""World Development Indicators Asset - Handles downloading and processing indicator data"""
import asyncio
import json
import pyarrow as pa
import httpx
from datetime import datetime, date
from pathlib import Path
from tenacity import retry, wait_exponential, stop_after_attempt
from typing import Dict, List, Any

BASE_URL = "https://api.worldbank.org/v2"
CONCURRENT_REQUESTS = 5  # Further reduced to avoid rate limiting

def get_last_update() -> date:
    """Get the last update date from state, return a default if not found"""
    state_file = Path("data/state/wdi_last_update.json")
    if state_file.exists():
        with open(state_file, 'r') as f:
            state = json.load(f)
            return datetime.fromisoformat(state['last_date']).date()
    # Default to start from recent past if no state (full historical data is too large)
    return date(2020, 1, 1)

def save_last_update(last_date: date):
    """Save the latest update date to state"""
    state_file = Path("data/state/wdi_last_update.json")
    state_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(state_file, 'w') as f:
        json.dump({
            'last_date': last_date.isoformat(),
            'updated_at': datetime.now().isoformat()
        }, f, indent=2)

def get_processed_indicators() -> set:
    """Get list of indicators already processed"""
    state_file = Path("data/state/processed_indicators.json")
    if state_file.exists():
        with open(state_file, 'r') as f:
            state = json.load(f)
            return set(state.get('indicators', []))
    return set()

def save_processed_indicators(indicators: set):
    """Save list of processed indicators"""
    state_file = Path("data/state/processed_indicators.json")
    state_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(state_file, 'w') as f:
        json.dump({
            'indicators': list(indicators),
            'updated_at': datetime.now().isoformat()
        }, f, indent=2)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def fetch_data(session: httpx.AsyncClient, url: str, params: Dict = None) -> Dict[str, Any]:
    """Fetch data from World Bank API with retry."""
    try:
        response = await session.get(url, params=params)
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error {e.response.status_code} for URL: {url}")
        print(f"Response text: {e.response.text[:500]}")
        raise
    
    data = response.json()
    
    if isinstance(data, list) and len(data) >= 2:
        return {"metadata": data[0], "data": data[1]}
    elif isinstance(data, list) and len(data) == 1:
        return {"metadata": {}, "data": data[0]}
    return {"metadata": {}, "data": data}

async def fetch_paginated(session: httpx.AsyncClient, url: str, params: Dict) -> List[Dict]:
    """Fetch all pages from a paginated endpoint."""
    items = []
    page = 1
    
    while True:
        result = await fetch_data(session, url, {**params, "page": page})
        if not result["data"]:
            break
        items.extend(result["data"])
        if page >= result["metadata"].get("pages", 1):
            break
        page += 1
    
    return items

async def fetch_indicator_data(session: httpx.AsyncClient, indicator_id: str, countries_map: Dict, indicators_map: Dict, semaphore: asyncio.Semaphore) -> List[Dict]:
    """Fetch data for a specific indicator across all countries."""
    async with semaphore:
        url = f"{BASE_URL}/country/all/indicator/{indicator_id}"
        params = {"format": "json", "per_page": 1000, "date": "2022:2024"}  # Reduced page size and date range
        
        try:
            data = await fetch_paginated(session, url, params)
        except Exception as e:
            print(f"Failed to fetch data for indicator {indicator_id}: {e}")
            return []  # Return empty list for this indicator instead of failing entirely
        
        # Process and validate data
        processed_data = []
        for record in data:
            if not record or not isinstance(record, dict):
                continue
                
            # Skip records without value or with empty country code
            if record.get("value") is None or not record.get("countryiso3code"):
                continue
                
            country_code = record["countryiso3code"]
            country_name = countries_map.get(country_code, "Unknown")
            indicator_info = indicators_map.get(indicator_id, {})
            
            try:
                year = int(record["date"])
                value = float(record["value"])
            except (ValueError, TypeError):
                continue  # Skip invalid data
                
            processed_record = {
                "country_code": country_code,
                "country_name": country_name,
                "indicator_code": indicator_id,
                "indicator_name": indicator_info.get("name", ""),
                "year": year,
                "value": value,
                "unit": indicator_info.get("unit"),
                "scale": indicator_info.get("scale"),
                "decimal_places": indicator_info.get("decimalPlaces")
            }
            processed_data.append(processed_record)
        
        return processed_data

async def process_world_development_indicators() -> pa.Table:
    """
    Download and process World Development Indicators data.
    Returns only new/changed data since last update.
    """
    print("📊 Fetching World Development Indicators data...")
    
    # Get state
    processed_indicators = get_processed_indicators()
    
    async with httpx.AsyncClient(
        timeout=120.0,  # Increased timeout
        limits=httpx.Limits(max_connections=CONCURRENT_REQUESTS)
    ) as session:
        # Fetch metadata first
        print("Fetching countries metadata...")
        countries = await fetch_paginated(session, f"{BASE_URL}/country", 
                                        {"format": "json", "per_page": 500})
        countries_map = {c.get("id"): c.get("name") for c in countries if c.get("id") and c.get("name")}
        print(f"Loaded {len(countries_map)} countries")
        
        print("Fetching indicators metadata...")
        # Find WDI source ID
        sources = await fetch_data(session, f"{BASE_URL}/source", {"format": "json", "per_page": 100})
        wdi_id = next((s["id"] for s in sources.get("data", []) 
                      if "World Development Indicators" in s.get("name", "")), "2")
        
        # Fetch WDI indicators
        indicators = await fetch_paginated(session, f"{BASE_URL}/indicator", 
                                         {"format": "json", "per_page": 500, "source": wdi_id})
        
        # Filter to active indicators and create lookup
        active_indicators = [i for i in indicators if i.get("id") and i.get("name")]
        indicators_map = {i["id"]: i for i in active_indicators}
        print(f"Found {len(active_indicators)} active indicators")
        
        # Filter to new indicators (not yet processed)
        new_indicators = [i for i in active_indicators if i["id"] not in processed_indicators]
        
        if not new_indicators:
            print("No new indicators to process")
            return pa.table({})
        
        print(f"Processing {len(new_indicators)} new indicators...")
        
        # Fetch data for indicators with controlled concurrency
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        all_records = []
        
        # Process indicators in batches to avoid overwhelming the API
        batch_size = 5  # Reduced batch size
        for i in range(0, len(new_indicators), batch_size):
            batch = new_indicators[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(new_indicators) + batch_size - 1)//batch_size}")
            
            tasks = [
                fetch_indicator_data(session, indicator["id"], countries_map, indicators_map, semaphore)
                for indicator in batch
            ]
            
            batch_results = await asyncio.gather(*tasks)
            
            for result in batch_results:
                all_records.extend(result)
            
            # Update processed indicators for this batch
            batch_ids = {indicator["id"] for indicator in batch}
            processed_indicators.update(batch_ids)
            save_processed_indicators(processed_indicators)
            
            # Add a small delay between batches to avoid rate limiting
            if i + batch_size < len(new_indicators):
                await asyncio.sleep(2)
    
    if not all_records:
        print("No data records found")
        return pa.table({})
    
    print(f"Processed {len(all_records)} data records")
    
    # Update state with current date
    save_last_update(datetime.now().date())
    
    # Convert to PyArrow table
    return pa.Table.from_pylist(all_records)

if __name__ == "__main__":
    # For testing the asset independently
    table = asyncio.run(process_world_development_indicators())
    print(f"Generated table with {table.num_rows} rows and {table.num_columns} columns")