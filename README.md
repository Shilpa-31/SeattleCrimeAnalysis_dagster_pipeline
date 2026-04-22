# Seattle Crime Data & Geo-Spatial Analytics Pipeline using DAGSTER

An end-to-end **data analytics project** that builds a scalable pipeline to ingest, clean, transform, and analyze Seattle crime data, enriched with **geo-spatial intelligence** and visualized through an interactive dashboard.

---

## ❓ What Problem Does This Solve?

Raw crime datasets are often incomplete, inconsistent, and difficult to analyze due to missing values, invalid location data, and unstructured formats. Additionally, without geographic context, it is challenging to derive meaningful insights such as identifying high-crime neighborhoods or spatial patterns.

This project addresses these challenges by:

- 🧹 Cleaning and standardizing raw crime data to ensure reliability  
- 📍 Enriching crime records with geo-spatial context using neighborhood boundaries  
- 🔄 Automating data pipelines using Dagster for consistent updates  
- 📊 Enabling actionable insights through structured storage and visualization  

---

## 🌟 Key Highlights

- 🔄 Automated data pipelines using **Dagster**
- 🧹 Robust data cleaning & validation workflows
- 🗺️ Geo-spatial enrichment using neighborhood boundaries
- 🗄️ Hybrid storage: **MongoDB (raw)** + **PostgreSQL (warehouse)**
- 📊 Interactive analytics dashboard using **Streamlit**
- ⏰ Scheduled pipelines (daily + weekly automation)

---

## 🏗️ Architecture Overview

```text
            ┌────────────────────┐
            │  Crime Data Source │
            └─────────┬──────────┘
                      │
           ┌────────────────────┐
           │   MongoDB (Raw)    │
           └─────────┬──────────┘
                     │
    ┌──────────────────────────────────┐
    │ Dagster Data Pipeline            │
    │----------------------------------│
    │ • Load & Normalize Data          │
    │ • Clean & Validate Data          │
    │ • Remove Invalid Records         │
    │ • Fix Datetime Issues            │
    └─────────┬────────────────────────┘
              │
    ┌──────────────────────────────┐
    │ PostgreSQL (Cleaned Data)    │
    └─────────┬────────────────────┘
              │
    ┌──────────────────────────────┐
    │ Geo-Spatial Processing       │
    │ (GeoPandas + Shapely)        │
    └─────────┬────────────────────┘
              │
    ┌──────────────────────────────┐
    │ Streamlit Dashboard          │
    └──────────────────────────────┘
```
---

## Tech Stack

| Layer           | Tools                        |
|----------------|-----------------------------|
| Orchestration  | Dagster                     |
| Data Storage   | MongoDB, PostgreSQL         |
| Data Processing| Pandas, NumPy               |
| Geo-Spatial    | GeoPandas, Shapely          |
| Visualization  | Plotly, Matplotlib          |
| Dashboard      | Streamlit                   |

---

## 📂 Dataset Information
This project uses publicly available **Seattle Crime Data**, which contains detailed records of reported crimes across Seattle.

### 🔗 Dataset Link
https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5/about_data

## 🔄 Pipeline Workflow
<p align="center"> <img src="Global_Asset_Lineage.svg" width="800"/> </p>

### Daily Data Pipeline (Ingestion & Processing)
Runs every day to ensure fresh and clean crime data is available for analysis.

**Workflow:**
- Load raw crime data from MongoDB  
- Normalize and clean dataset (remove duplicates, invalid records, fix datetimes)  
- Store processed data into PostgreSQL (data warehouse)  
---

### Weekly Geo-Spatial Pipeline (Enrichment)
Runs weekly to enrich crime data with geographic context.

**Workflow:**
- Load Seattle neighborhood GeoJSON data  
- Retrieve cleaned crime data  
- Perform Spatial joins to map crime locations (lat/long) to neighborhoods 
- Store enriched geo-spatial crime dataset, enable downstream analytics and visualization
---

### Scheduling
- **Daily Crime Data Pipeline:** Runs every day at **7 PM** (`0 19 * * *`)  
- **Geo-Spatial Pipeline:** Runs every **Sunday at 6 PM** (`0 18 * * 0`)  

## Installation
- Clone Repository: git clone https://github.com/Shilpa-31/SeattleCrimeAnalysis_dagster_pipeline.git
- Go to root directory of project: cd SeattleCrimeAnalysis_dagster_pipeline
- Create Virtual Environment
- Install all dependencies: pip install -r `requirements.txt`
- Start Dagster: `dagster dev` and Open UI - http://localhost:3000
