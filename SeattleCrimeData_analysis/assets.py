from dagster import asset, Output, MetadataValue
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
import time
import requests
import pandas as pd
import warnings
import re
from sqlalchemy import create_engine
import geopandas as gpd
from shapely.geometry import shape

# -----------------------------
# Global database configuration
# -----------------------------
# MongoDB Connection
MONGO_URI = "mongodb+srv://myMongoDB:myMongoDB@developerclusterone.svs3c.mongodb.net/"
mongo_client = MongoClient(MONGO_URI)

# PostgreSQL connection
POSTGRES_URI = "postgresql://postgres:admin@localhost:5433/myDatabase"
postgres_engine = create_engine(POSTGRES_URI)

# Helper functions
def get_mongo_collection(db_name: str, collection_name: str):
    """Return a MongoDB collection object."""
    return mongo_client[db_name][collection_name]

# ----------------------------------------------
# Asset 1: Fetching Seattle Crime data from API
# ---------------------------------------------
@asset(group_name="crime_ingestion_into_mongoDB")
def fetch_crime_data() -> Output[str]:
    """
    Incrementally fetch Seattle crime data and store in MongoDB.
    - Only downloads NEW data since last ingestion
    - Safe for daily Dagster schedule (7:00 PM)
    - Idempotent (no duplicates)
    """

    db_name = "SeattleCrimeDataset"
    collection_name = "crime_seattle_data"
    collection = get_mongo_collection(db_name, collection_name)

    API_URL = "https://data.seattle.gov/resource/tazs-3rd5.json"

    # Determine start date (incremental checkpoint)
    latest_doc = collection.find_one(
        {"offense_date": {"$exists": True}},
        sort=[("offense_date", -1)],
        projection={"offense_date": 1}
    )

    if latest_doc and "offense_date" in latest_doc:
        start_date = pd.to_datetime(latest_doc["offense_date"]).date() + timedelta(days=1)
    else:
        # First-ever run fallback
        start_date = datetime.strptime("2025-01-01", "%Y-%m-%d").date()

    today = datetime.utcnow().date()

    if start_date > today:
        return Output(
            str(today),
            metadata={
                "Message": "No new data available",
                "Last Ingested Date": str(start_date - timedelta(days=1))
            }
        )
    # Fetch new data day-by-day
    inserted_total = 0
    current_date = start_date

    while current_date <= today:
        start_datetime = f"{current_date}T00:00:00"
        end_datetime = f"{current_date}T23:59:59"

        offset = 0
        batch_size = 1000
        inserted_day = 0

        while True:
            params = {
                "$where": (
                    f"offense_date >= '{start_datetime}' "
                    f"AND offense_date <= '{end_datetime}'"
                ),
                "$limit": batch_size,
                "$offset": offset,
                "$order": "offense_date ASC",
            }

            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            batch = response.json()

            if not batch:
                break

            operations = [
                UpdateOne(
                    {"report_number": record["report_number"]},
                    {"$set": record},
                    upsert=True
                )
                for record in batch if "report_number" in record
            ]

            if operations:
                result = collection.bulk_write(operations, ordered=False)
                inserted_day += result.upserted_count

            offset += batch_size
            time.sleep(1)  # avoid API throttling

        inserted_total += inserted_day
        current_date += timedelta(days=1)

    # Dagster metadata
    return Output(
        value=str(today),
        metadata={
            "New Records Inserted": MetadataValue.int(int(inserted_total)),
            "Start Date": MetadataValue.text(str(start_date)),
            "End Date": MetadataValue.text(str(today)),
            "Scheduler": MetadataValue.text("Daily at 7:00 PM"),
            "Status": MetadataValue.text("Incremental ingestion completed"),
            "Message": "Crime data ingestion completed successfully..!"
        }
    )

# -------------------------------------
# Asset 2: Load Crime Data from MongoDB
# -------------------------------------
@asset(group_name="crime_ingestion")
def load_and_process_crime_data_from_mongodb(
    fetch_crime_data: str  
) -> Output[pd.DataFrame]:
    """
    Load crime data from MongoDB and return cleaned DataFrame.
    Depends on `fetch_crime_data` to ensure data has been ingested first.
    """
    collection = get_mongo_collection("SeattleCrimeDataset", "crime_seattle_data")
    data_list = list(collection.find())
    if not data_list:
        return Output(pd.DataFrame(), metadata={"warning": "MongoDB returned no records"})

    df = pd.DataFrame(data_list)
    df.columns = df.columns.str.lower()

    required_columns = [
        "report_number", "report_date_time", "offense_date", "nibrs_crime_against_category",
        "offense_sub_category", "offense_category", "nibrs_offense_code",
        "nibrs_offense_code_description", "precinct", "sector", "beat",
        "neighborhood", "longitude", "latitude"
    ]
    available_columns = [c for c in required_columns if c in df.columns]
    crime_df = df[available_columns]

    for col in ["report_date_time", "offense_date"]:
        if col in crime_df.columns:
            crime_df[col] = pd.to_datetime(crime_df[col], errors="coerce")

    return Output(
        crime_df,
        metadata={
            "Total number of Rows": MetadataValue.int(len(crime_df)),
            "Columns Count": MetadataValue.int(len(crime_df.columns)),
            "Selected Columns": MetadataValue.md(", ".join(available_columns)),
            "Top 10 Rows": MetadataValue.md(crime_df.head(10).to_markdown()),
        }
    )

# ---------------------------------------------------
# Asset 3: Clean & Analyze Crime Data
# ---------------------------------------------------
@asset(group_name="crime_processing")
def clean_and_analyze_crime_data(
    load_and_process_crime_data_from_mongodb: pd.DataFrame,
) -> Output[pd.DataFrame]:
    warnings.filterwarnings("ignore")
    crime_df = load_and_process_crime_data_from_mongodb.copy()

    # Normalize column names
    crime_df.columns = (
        crime_df.columns.str.lower()
        .str.replace("nibrs", "", regex=False)
        .str.replace(" ", "_")
        .str.replace("__", "_")
        .str.strip("_")
    )

    had_duplicates = crime_df.duplicated().any()
    if had_duplicates:
        crime_df = crime_df.drop_duplicates()

    # Check for duplicates
    had_duplicates = bool(crime_df.duplicated().any())
    if had_duplicates:
        crime_df = crime_df.drop_duplicates()    

    # Remove invalid categories
    crime_df = crime_df[crime_df["crime_against_category"] != "-"]
    crime_df = crime_df[crime_df["precinct"] != "-"]

    crime_category_counts_before = crime_df["crime_against_category"].value_counts().to_dict()

    # Latitude/Longitude cleanup
    crime_df = crime_df[
        (crime_df["latitude"] != "REDACTED") & (crime_df["latitude"] != "-1.0")
    ]
    crime_df["latitude"] = crime_df["latitude"].astype(float)
    crime_df["longitude"] = crime_df["longitude"].astype(float)

    # Drop rows with NaNs
    crime_df = crime_df.dropna()
    crime_df = crime_df[crime_df["crime_against_category"] != "NOT_A_CRIME"]
    crime_df = crime_df[~((crime_df["longitude"] == 0.0) & (crime_df["latitude"] == 0.0))]

    # Validate datetime
    iso_8601_pattern = re.compile(
        r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T"
        r"([01]\d|2[0-3]):([0-5]\d):([0-5]\d)(\.\d{1,3})?$"
    )

    def correct_datetime(date_str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%dT%H:%M:%S.000")
            except Exception:
                continue
        return None

    valid_dt = 0
    invalid_dt = 0
    if "offense_date" in crime_df.columns:
        corrected_dates = []
        for val in crime_df["offense_date"].astype(str):
            if re.match(iso_8601_pattern, val):
                valid_dt += 1
                corrected_dates.append(val)
            else:
                fixed = correct_datetime(val)
                if fixed:
                    valid_dt += 1
                    corrected_dates.append(fixed)
                else:
                    invalid_dt += 1
                    corrected_dates.append(None)
        crime_df["offense_date"] = corrected_dates
        crime_df = crime_df.dropna(subset=["offense_date"])

    crime_category_counts_after = crime_df["crime_against_category"].value_counts().to_dict()

    return Output(
        crime_df,
        metadata={
            "Rows After Cleaning": MetadataValue.int(len(crime_df)),
            "Duplicates Removed": MetadataValue.bool(had_duplicates),
            "Valid Offense DateTime Records": MetadataValue.int(valid_dt),
            "Invalid Offense DateTime Records": MetadataValue.int(invalid_dt),
            "Crime Category Distribution (Before)": MetadataValue.json(crime_category_counts_before),
            "Crime Category Distribution (After)": MetadataValue.json(crime_category_counts_after),
            "Top 10 Rows": MetadataValue.md(crime_df.head(10).to_markdown()),
        }
    )

# ---------------------------------------------
# Asset 4: Store Clean Crime Data in PostgreSQL
# ---------------------------------------------
@asset(group_name="crime_warehouse")
def store_crime_data_postgres(clean_and_analyze_crime_data: pd.DataFrame) -> Output[pd.DataFrame]:
    table = "CleanSeattleCrimeData"
    clean_and_analyze_crime_data.to_sql(
        table,
        postgres_engine,
        if_exists="replace",
        index=False,
    )
    return Output(
        value=clean_and_analyze_crime_data,
        metadata={
            "Table Name": MetadataValue.text(table),
            "Rows Stored": MetadataValue.int(len(clean_and_analyze_crime_data)),
            "Columns": MetadataValue.md(", ".join(clean_and_analyze_crime_data.columns)),
            "Top 5 Rows": MetadataValue.md(clean_and_analyze_crime_data.head().to_markdown())
        }
    )

# --------------------------------------------
# Asset 5: Store Seattle Neighborhoods GeoJSON
# --------------------------------------------
@asset(group_name="geo_data_loading")
def store_seattle_neighborhoods_geojson() -> gpd.GeoDataFrame:

    geojson_path = "2016_seattle_neighborhoods.geojson"
    db_name = "SeattleGeoData"
    collection_name = "Seattle_Neighborhoods"

    # Read the GeoJSON as a GeoDataFrame
    gdf = gpd.read_file(geojson_path)

    # Convert geometries to GeoJSON-like dicts for MongoDB without breaking the GeoDataFrame
    records = gdf.copy()
    records["geometry"] = records["geometry"].apply(lambda geom: geom.__geo_interface__ if geom else None)
    records = records.to_dict(orient="records")

    # Store in MongoDB
    collection = get_mongo_collection(db_name, collection_name)
    collection.delete_many({})
    collection.insert_many(records)

    # Set CRS
    gdf = gdf.to_crs("EPSG:4326")
    return gdf


# -------------------------------------------------
# Asset 6: Load Seattle Neighborhoods from MongoDB
# -------------------------------------------------
@asset(group_name="geo_data_loading")
def load_seattle_neighborhoods_from_mongo(
    store_seattle_neighborhoods_geojson: object
) -> Output[gpd.GeoDataFrame]:

    db_name = "SeattleGeoData"
    collection_name = "Seattle_Neighborhoods"
    collection = get_mongo_collection(db_name, collection_name)

    docs = list(collection.find({}, {"_id": 0, "NEIGHBO": 1, "CRA_NAM": 1, "geometry": 1}))
    if not docs:
        raise ValueError("No neighborhoods data found in MongoDB")
    
    gdf = gpd.GeoDataFrame(docs)

    # Convert dicts to Shapely geometries
    gdf["geometry"] = gdf["geometry"].apply(lambda g: shape(g) if g else None)
    gdf = gdf.set_geometry("geometry")

    # Assign CRS (GeoJSON is likely EPSG:4326)
    gdf.set_crs("EPSG:4326", inplace=True)

    return Output(
        value=gdf,
        metadata={
            "Total Rows": MetadataValue.int(len(gdf)),
            "Columns Name": MetadataValue.md(", ".join(gdf.columns)),
            "CRS (Coordinate reference system)": MetadataValue.text(str(gdf.crs)),
            "Top 5 Rows": MetadataValue.md(gdf.head().to_markdown())
        }
    )

# ---------------------------------------------------
# Asset 7: Spatial Join Crime + GeoJson Neighborhoods
# ---------------------------------------------------
@asset(group_name="geo_spatial_join")
def crime_neighborhood_spatial_join(
    store_crime_data_postgres: pd.DataFrame,
    load_seattle_neighborhoods_from_mongo: gpd.GeoDataFrame
) -> Output[gpd.GeoDataFrame]:

    crime_gdf = gpd.GeoDataFrame(
        store_crime_data_postgres,
        geometry=gpd.points_from_xy(
            store_crime_data_postgres.longitude,
            store_crime_data_postgres.latitude,
        ),
        crs="EPSG:4326"
    )

    neighborhoods = load_seattle_neighborhoods_from_mongo
    if crime_gdf.crs != neighborhoods.crs:
        crime_gdf = crime_gdf.to_crs(neighborhoods.crs)

    joined = gpd.sjoin(crime_gdf, neighborhoods, how="left", predicate="within")
    joined.to_csv("CrimeData_Geojson_Neighbor_Spatial_Join.csv", index=False)

    return Output(
        value=joined,
        metadata={
            "Total Records": MetadataValue.int(int(len(joined))),
            "Matched Neighborhoods": MetadataValue.int(int(joined["NEIGHBO"].notna().sum())),
            "CRS": MetadataValue.text(str(joined.crs)),
            "Columns Name": MetadataValue.md(", ".join(joined.columns)),
            "Top 10 Rows": MetadataValue.md(joined.head(10).to_markdown())
        }
    )

# --------------------------------------------
# Asset 8: Insert Spatial Join into PostgreSQL
# --------------------------------------------
@asset(group_name="geo_spatial_join")
def insert_spatial_join_into_postgres(
    crime_neighborhood_spatial_join: gpd.GeoDataFrame
) -> Output[int]:

    df_to_insert = crime_neighborhood_spatial_join.copy()
    if "geometry" in df_to_insert.columns:
        df_to_insert["geometry"] = df_to_insert["geometry"].apply(lambda geom: geom.wkt if geom else None)

    df_to_insert.to_sql(
        name="CleanSeattleCrimeWith_NEIGHBOR",
        con=postgres_engine,
        if_exists="replace",
        index=False
    )

    total_rows = len(df_to_insert)

    return Output(
        value=total_rows,
        metadata={
            "Total Rows Inserted": MetadataValue.int(total_rows),
            "Table Name": MetadataValue.text("crime_neighborhood_joined"),
            "Columns Count": MetadataValue.int(len(df_to_insert.columns)),
            "Columns Name": MetadataValue.text(", ".join(df_to_insert.columns))
        }
    )