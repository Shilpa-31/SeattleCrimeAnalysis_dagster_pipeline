from dagster import define_asset_job

from .assets import (
    # Daily ingestion assets
    fetch_crime_data,
    load_and_process_crime_data_from_mongodb,
    clean_and_analyze_crime_data,
    store_crime_data_postgres,

    # Weekly Geo/spatial assets
    store_seattle_neighborhoods_geojson,
    load_seattle_neighborhoods_from_mongo,
    crime_neighborhood_spatial_join,
    insert_spatial_join_into_postgres,
)

# DAILY JOB — Crime Data Ingestion & Processing
daily_crime_ingestion_job = define_asset_job(
    name="daily_crime_ingestion_job",
    selection=[
        fetch_crime_data,
        load_and_process_crime_data_from_mongodb,
        clean_and_analyze_crime_data,
        store_crime_data_postgres,
    ],
)

# WEEKLY JOB — Geo Data + Spatial Join
weekly_geo_spatial_job = define_asset_job(
    name="weekly_geo_spatial_job",
    selection=[
        store_seattle_neighborhoods_geojson,
        load_seattle_neighborhoods_from_mongo,
        crime_neighborhood_spatial_join,
        insert_spatial_join_into_postgres,
    ],
)
