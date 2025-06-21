"""World Development Indicators Connector"""
import os
import json
import asyncio
from pathlib import Path
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from assets.countries.countries import process_countries
from assets.indicators.indicators import process_indicators
from assets.world_development_indicators.world_development_indicators import process_world_development_indicators

def validate_environment():
    """Validate required environment variables exist"""
    required = ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT_URL", "R2_BUCKET_NAME"]
    missing = [var for var in required if var not in os.environ]
    if missing:
        raise ValueError(f"Missing environment variables: {missing}")

def upload_to_r2(data, dataset_name):
    """Upload data to Cloudflare R2 as Parquet"""
    from datetime import datetime
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["R2_ENDPOINT_URL"]
    )
    
    buffer = pq.BufferOutputStream()
    pq.write_table(data, buffer)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{dataset_name}/{timestamp}.parquet"
    s3_client.put_object(
        Bucket=os.environ["R2_BUCKET_NAME"],
        Key=key,
        Body=buffer.getvalue().to_pybytes()
    )

async def main():
    validate_environment()
    
    # Process assets in DAG order (returns only new/changed data)
    countries_data = await process_countries()
    indicators_data = await process_indicators()
    wdi_data = await process_world_development_indicators()
    
    # Upload data to R2
    upload_to_r2(countries_data, "countries")
    upload_to_r2(indicators_data, "indicators")
    upload_to_r2(wdi_data, "world_development_indicators")

if __name__ == "__main__":
    asyncio.run(main())