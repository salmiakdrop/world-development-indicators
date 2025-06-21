"""World Bank Countries Asset - Handles downloading and processing country metadata"""
import json
import pyarrow as pa
import httpx
from datetime import datetime, date
from pathlib import Path
from tenacity import retry, wait_exponential, stop_after_attempt
from typing import Dict, List, Any

BASE_URL = "https://api.worldbank.org/v2"

def get_last_update() -> date:
    """Get the last update date from state, return a default if not found"""
    state_file = Path("data/state/countries_last_update.json")
    if state_file.exists():
        with open(state_file, 'r') as f:
            state = json.load(f)
            return datetime.fromisoformat(state['last_date']).date()
    # Default to start from beginning if no state
    return date(2000, 1, 1)

def save_last_update(last_date: date):
    """Save the latest update date to state"""
    state_file = Path("data/state/countries_last_update.json")
    state_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(state_file, 'w') as f:
        json.dump({
            'last_date': last_date.isoformat(),
            'updated_at': datetime.now().isoformat()
        }, f, indent=2)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def fetch_data(session: httpx.AsyncClient, url: str, params: Dict = None) -> Dict[str, Any]:
    """Fetch data from World Bank API with retry."""
    response = await session.get(url, params=params)
    response.raise_for_status()
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

async def process_countries() -> pa.Table:
    """
    Download and process World Bank countries metadata.
    Returns only new/changed data since last update.
    """
    print("🌍 Fetching World Bank countries metadata...")
    
    async with httpx.AsyncClient(timeout=60.0) as session:
        # Fetch countries
        url = f"{BASE_URL}/country"
        params = {"format": "json", "per_page": 300}
        countries = await fetch_paginated(session, url, params)
    
    if not countries:
        print("No countries data found")
        return pa.table({})
    
    # Process countries data - fail fast if any required field is missing
    processed_countries = []
    for country in countries:
        # Validate required fields
        if not country.get("id"):
            raise ValueError(f"Country missing required 'id' field: {country}")
        if not country.get("name"):
            raise ValueError(f"Country missing required 'name' field: {country}")
            
        processed_country = {
            "country_code": country["id"],
            "iso2_code": country.get("iso2Code"),
            "country_name": country["name"],
            "capital_city": country.get("capitalCity"),
            "region_code": country.get("region", {}).get("id") if country.get("region") else None,
            "region_name": country.get("region", {}).get("value") if country.get("region") else None,
            "income_level_code": country.get("incomeLevel", {}).get("id") if country.get("incomeLevel") else None,
            "income_level_name": country.get("incomeLevel", {}).get("value") if country.get("incomeLevel") else None,
            "lending_type_code": country.get("lendingType", {}).get("id") if country.get("lendingType") else None,
            "lending_type_name": country.get("lendingType", {}).get("value") if country.get("lendingType") else None,
        }
        processed_countries.append(processed_country)
    
    print(f"Processed {len(processed_countries)} countries")
    
    # Update state with current date (countries metadata doesn't change frequently)
    save_last_update(datetime.now().date())
    
    # Convert to PyArrow table
    return pa.Table.from_pylist(processed_countries)

if __name__ == "__main__":
    # For testing the asset independently
    import asyncio
    table = asyncio.run(process_countries())
    print(f"Generated table with {table.num_rows} rows and {table.num_columns} columns")