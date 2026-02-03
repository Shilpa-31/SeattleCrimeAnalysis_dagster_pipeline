from dagster import Definitions

from .assets import (
    fetch_crime_data,
    load_and_process_crime_data_from_mongodb,
    clean_and_analyze_crime_data,
    store_crime_data_postgres,
    store_seattle_neighborhoods_geojson,
    load_seattle_neighborhoods_from_mongo,
    crime_neighborhood_spatial_join,
    insert_spatial_join_into_postgres,
)

from .jobs import (
    daily_crime_ingestion_job,
    weekly_geo_spatial_job,
)

from .schedules import (
    daily_crime_schedule,
    weekly_geo_schedule,
)

defs = Definitions(
    assets=[
        fetch_crime_data,
        load_and_process_crime_data_from_mongodb,
        clean_and_analyze_crime_data,
        store_crime_data_postgres,
        store_seattle_neighborhoods_geojson,
        load_seattle_neighborhoods_from_mongo,
        crime_neighborhood_spatial_join,
        insert_spatial_join_into_postgres,
    ],
    jobs=[
        daily_crime_ingestion_job,
        weekly_geo_spatial_job,
    ],
    schedules=[
        daily_crime_schedule,
        weekly_geo_schedule,
    ],
)
