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
    """Decorator to measure execution time of operations."""
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
    """Lists all files in the GCS bucket. Optional prefix filters by 'folder'."""
    bucket_name = os.getenv("GCP_GCS_BUCKET", 'mdzki-ny-taxi-bucket')
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # list_blobs returns an iterator
    blobs = bucket.list_blobs(prefix=prefix)

    file_list = []
    for blob in blobs:
        file_list.append(blob.name)
    return file_list

@timed_operation("Download file from web")
def download_file(url: str, local_path: Path) -> Path:
    """Download file from URL to local path."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(response.content)
    
    return local_path


@timed_operation("Transform CSV to Parquet")
def transform_to_parquet(csv_path: Path, parquet_path: Path) -> Path:
    """Read gzipped CSV with Polars and save as Parquet."""
    df = pl.read_csv(csv_path, infer_schema_length=20000)
    df.write_parquet(parquet_path)
    return parquet_path


@timed_operation("Upload to GCS")
def upload_to_gcs(local_file: Path, bucket_name: str, object_name: str) -> None:
    """Upload file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_file))


@timed_operation("Download whole dataset")
def web_to_gcs(year: int, service: str, overwrite: bool) -> None:
    """Download, transform, and upload taxi data to GCS for specified year and service."""
    
    existing_files = set () if overwrite else set(list_gcs_files(prefix=service))
    for month in range(1, 13):
        month_str = f"{month:02d}"
        csv_file_name = f"{service}_tripdata_{year}-{month_str}.csv.gz"
        parquet_file_name = f"{service}_tripdata_{year}-{month_str}.parquet"


        csv_path = TEMP_DIR / csv_file_name
        parquet_path = TEMP_DIR / parquet_file_name
        gcs_object_name = f"{service}/{parquet_file_name}"
        
        # checking if file exists in bucket to not overwrite
        if gcs_object_name in existing_files:
            logger.info(f"Skipping {gcs_object_name}: Already exists in GCS.")
            continue

        request_url = f"{INIT_URL}{service}/{csv_file_name}"
        
        try:
            # Step 1: Download
            download_file(request_url, csv_path)
            
            # Step 2: Transform to Parquet
            transform_to_parquet(csv_path, parquet_path)
            
            # Step 3: Upload to GCS
            upload_to_gcs(parquet_path, BUCKET, gcs_object_name)
            
            # Cleanup
            csv_path.unlink()
            parquet_path.unlink()
            
            logger.info(f"Processed: {service}/{parquet_file_name}\n")
            
        except Exception as e:
            logger.error(f"Error processing {csv_file_name}: {e}")
            # Cleanup on error
            csv_path.unlink(missing_ok=True)
            parquet_path.unlink(missing_ok=True)


if __name__ == "__main__":
    web_to_gcs(2019, "green", overwrite=False)
    web_to_gcs(2020, "green", overwrite=False)
    web_to_gcs(2019, "yellow", overwrite=False)
    web_to_gcs(2020, "yellow", overwrite=False)