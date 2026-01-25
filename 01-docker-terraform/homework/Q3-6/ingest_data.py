import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db):

    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    url_lookup = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    df_lookup = pd.read_csv(url_lookup)
    df = pd.read_parquet(url)

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    df_lookup.to_sql(name='taxi_zones', con=engine, if_exists='replace')
    df.to_sql(name='green_taxi_trips_2025_11', con=engine, if_exists='replace')


if __name__ == '__main__':
    pass
    run()