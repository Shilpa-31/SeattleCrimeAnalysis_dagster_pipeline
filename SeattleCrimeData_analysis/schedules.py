from dagster import ScheduleDefinition

from .jobs import (
    daily_crime_ingestion_job,
    weekly_geo_spatial_job,
)

# DAILY — Run crime ingestion every day at 7 PM
daily_crime_schedule = ScheduleDefinition(
    name="daily_crime_schedule",
    cron_schedule="0 19 * * *",
    job=daily_crime_ingestion_job,
)

# WEEKLY — Run spatial join Sunday at 6 PM
weekly_geo_schedule = ScheduleDefinition(
    name="weekly_geo_schedule",
    cron_schedule="0 6 * * *",
    job=weekly_geo_spatial_job,
)
