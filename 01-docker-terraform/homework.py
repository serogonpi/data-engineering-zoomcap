import pandas as pd
import pyarrow.parquet as pq
import click
from sqlalchemy import create_engine

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='pgdatabase', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
def main(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    data_trips = pd.read_parquet(
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
        )

    data_zones_dtypes = {
        'LocationID': 'Int64',
        'Borough': 'string',
        'Zone': 'string',
        'service_zone': 'string'
    }

    data_zones = pd.read_csv(
        'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv',
        dtype=data_zones_dtypes)

    data_zones.to_sql(
        name='taxi_zones',
        con=engine,
        if_exists='replace'
    )

    first = True
    chuncksize = 100000
    for i in range(0, len(data_trips), chuncksize):
        if first:
            chunck = data_trips.iloc[i:i+chuncksize]
            chunck.to_sql(
                'green_taxi_trips',
                con=engine,
                if_exists='replace'
            )
            first = False
        else:
            chunck = data_trips.iloc[i:i+chuncksize]
            chunck.to_sql(
                'green_taxi_trips',
                con=engine,
                if_exists='append'
            )

if __name__ == '__main__':
    main()