"""
REST API source for NYC taxi data using dlt.

This pipeline fetches NYC taxi data from a paginated REST API and loads it into DuckDB.
The API returns 1,000 records per page and pagination stops when an empty page is returned.
"""

import dlt
from dlt.sources.rest_api import rest_api_source


@dlt.source
def taxi_pipeline():
    """
    Define a REST API source for NYC taxi data.
    
    The source uses page-based pagination with 1,000 records per page.
    Pagination automatically stops when an empty page is received.
    """
    config = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            "paginator": {
                "type": "page_number",
                "base_page": 1,           # API Zoomcamp startuje od strony 1
                "page_param": "page",
                "total_path": "",         # Opcjonalne: jeśli API nie zwraca total, zostaw puste
                "stop_after_empty_page": True,
            }
        },
        "resources": [
            {
                "name": "nyc_taxi_data",
                "endpoint": {
                    "path": "/",
                    "params": {
                        "limit": 1000,  # Records per page
                    },
                },
            },
        ],
    }

    return rest_api_source(config)


# Create the pipeline
pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    # Run the pipeline
    load_info = pipeline.run(taxi_pipeline())
    print(load_info)  # noqa: T201
    print(len(list(load_info)))
    

