import os
import time
from pathlib import Path
from typing import Optional
import requests
import polars as pl
from google.cloud import storage
import logging

"""
Pre-reqs: 
1. `pip install polars pyarrow google-cloud-storage requests`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = os.environ.get("GCP_GCS_BUCKET", "mdzki-ny-taxi-bucket")
INIT_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
TEMP_DIR = Path("temp_data")
TEMP_DIR.mkdir(exist_ok=True)

def timed_operation(operation_name: str) -> callable:
    def decorator(func: callable) -> callable:
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            logger.info(f"▶ {operation_name}")
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            logger.info(f"✓ {operation_name} completed in {elapsed:.2f}s\n")
            return result
        return wrapper
    return decorator

def list_gcs_files(prefix=None):
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]

@timed_operation("Download file from web")
def download_file(url: str, local_path: Path) -> Path:
    response = requests.get(url, stream=True)
    response.raise_for_status()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(response.content)
    return local_path

@timed_operation("Transform CSV to Parquet")
def transform_to_parquet(csv_path: Path, parquet_path: Path) -> Path:
    # We use infer_schema_length to handle the messy taxi data types
    df = pl.read_csv(csv_path, infer_schema_length=100000)
    df.write_parquet(parquet_path)
    return parquet_path

@timed_operation("Upload to GCS")
def upload_to_gcs(local_file: Path, bucket_name: str, object_name: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_file))

@timed_operation("Batch process web to GCS")
def web_to_gcs(year: int, service: str, overwrite: bool, file_format: str = "original") -> None:
    """
    file_format: 'parquet' (converts to parquet) or 'original' (keeps .csv.gz)
    """
    existing_files = set() if overwrite else set(list_gcs_files(prefix=service))
    
    for month in range(1, 13):
        month_str = f"{month:02d}"
        csv_file_name = f"{service}_tripdata_{year}-{month_str}.csv.gz"
        
        # Determine target file name and path
        if file_format == "original":
            target_file_name = csv_file_name
        else:
            target_file_name = f"{service}_tripdata_{year}-{month_str}.parquet"

        csv_path = TEMP_DIR / csv_file_name
        target_path = TEMP_DIR / target_file_name
        gcs_object_name = f"{service}/{target_file_name}"
        
        if gcs_object_name in existing_files:
            logger.info(f"Skipping {gcs_object_name}: Already exists.")
            continue

        request_url = f"{INIT_URL}{service}/{csv_file_name}"
        
        try:
            # Step 1: Download
            download_file(request_url, csv_path)
            
            # Step 2: Handle Transformation
            if file_format == "parquet":
                transform_to_parquet(csv_path, target_path)
                # Cleanup the raw CSV if we converted it
                csv_path.unlink()
            else:
                # If 'original', target_path is just the csv_path
                target_path = csv_path
            
            # Step 3: Upload
            upload_to_gcs(target_path, BUCKET, gcs_object_name)
            
            # Final Cleanup
            target_path.unlink()
            logger.info(f"Successfully uploaded: {gcs_object_name}")
            
        except Exception as e:
            logger.error(f"Error processing {csv_file_name}: {e}")
            csv_path.unlink(missing_ok=True)
            if file_format == "parquet":
                target_path.unlink(missing_ok=True)

if __name__ == "__main__":
    # web_to_gcs(2019, "green", overwrite=True, file_format="original")
    # web_to_gcs(2020, "green", overwrite=True, file_format="original")
    # web_to_gcs(2019, "yellow", overwrite=True, file_format="original")
    # web_to_gcs(2020, "yellow", overwrite=True, file_format="original")
    web_to_gcs(2019, "fhv", overwrite=True, file_format="original")