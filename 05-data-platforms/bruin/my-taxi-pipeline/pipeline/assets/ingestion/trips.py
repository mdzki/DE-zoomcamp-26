"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""

import json
import os
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def generate_months_to_ingest(start_date: str, end_date: str) -> List[Tuple[int, int]]:
    """
    Generate list of (year, month) tuples for the date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        List of (year, month) tuples to ingest
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    months = []
    current = start
    
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    
    return months


def build_parquet_url(taxi_type: str, year: int, month: int) -> str:
    """
    Build the full URL for a parquet file.
    
    Args:
        taxi_type: Type of taxi (e.g., 'yellow', 'green')
        year: Year (e.g., 2022)
        month: Month (1-12)
    
    Returns:
        Full URL to the parquet file
    """
    file_name = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
    return f"{BASE_URL}/{file_name}"


def fetch_trip_data(url: str, taxi_type: str) -> pd.DataFrame:
    """
    Fetch trip data from a parquet URL.
    
    Args:
        url: Full URL to the parquet file
        taxi_type: Type of taxi for metadata tracking
    
    Returns:
        DataFrame with trip data and metadata columns, or empty DataFrame if fetch fails
    """
    try:
        print(f"Fetching: {url}")
        df = pd.read_parquet(url)
        
        # Add metadata columns for lineage
        df["taxi_type"] = taxi_type
        df["extracted_at"] = datetime.utcnow()
        
        print(f"  Successfully fetched {len(df)} rows")
        return df
        
    except Exception as e:
        print(f"  Warning: Could not fetch {url}: {e}")
        return pd.DataFrame()


def materialize() -> pd.DataFrame:
    """
    Main materialization function: orchestrates ingestion for all taxi types and dates.
    
    Uses Bruin runtime environment variables:
    - BRUIN_START_DATE: Start date (YYYY-MM-DD)
    - BRUIN_END_DATE: End date (YYYY-MM-DD)
    - BRUIN_VARS: JSON string containing pipeline variables (taxi_types)
    
    Returns:
        Concatenated DataFrame with all ingested trip data
    """
    # Get date range from Bruin runtime environment
    start_date = os.getenv("BRUIN_START_DATE", "2022-01-01")
    end_date = os.getenv("BRUIN_END_DATE", "2022-02-28")
    
    # Get taxi types from pipeline variables
    bruin_vars = json.loads(os.getenv("BRUIN_VARS", '{}'))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])
    
    print(f"\nIngestion Parameters:")
    print(f"  Date Range: {start_date} to {end_date}")
    print(f"  Taxi Types: {taxi_types}")
    
    # Generate months to ingest
    months_to_ingest = generate_months_to_ingest(start_date, end_date)
    print(f"  Months to Ingest: {len(months_to_ingest)}")
    
    dataframes = []
    
    # Iterate through all months and taxi types
    for year, month in months_to_ingest:
        for taxi_type in taxi_types:
            url = build_parquet_url(taxi_type, year, month)
            df = fetch_trip_data(url, taxi_type)
            
            if not df.empty:
                dataframes.append(df)
    
    if not dataframes:
        print("\nWarning: No data was fetched. Returning empty DataFrame.")
        return pd.DataFrame()
    
    # Concatenate all dataframes
    final_df = pd.concat(dataframes, ignore_index=True)
    
    print(f"\nIngestion Summary:")
    print(f"  Total Rows: {len(final_df)}")
    print(f"  Columns: {list(final_df.columns)}")
    
    return final_df


