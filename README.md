# Kommineni Automotive — Data Pipeline & Analytics Platform

A production grade data engineering portfolio project simulating a 5 location automotive dealership. Built to demonstrate real world skills in data ingestion, transformation, warehousing, and analytics.

---

## What This Project Does

Raw transactional data flows through a full medallion architecture bronze ingestion, silver cleaning, gold aggregation and surfaces in a live executive dashboard with role based access control.

**Live Demo** → [View Dashboard](https://kommineni-automotive.streamlit.app)

Login credentials:
- Executive: `exec001` / `exec001`
- Branch Manager: `mgr001` through `mgr005` / same as username
- Salesperson: `EMP001` through `EMP020` / same as username

---

## Architecture
```
Raw CSV Data
    ↓
Bronze Layer (DuckDB)
Raw ingestion with metadata — _ingested_at, _source_file
    ↓
Silver Layer (dbt)
Cleaned, typed, standardized staging models
17 automated data quality tests
    ↓
Gold Layer (dbt)
Business KPI aggregations
Revenue vs target, leaderboard, inventory, service utilization
    ↓
Streamlit Dashboard
Role based access: Executive, Branch Manager, Salesperson
5 real time filters: date range, location, make, sale type, salesperson
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Generation | Python, Faker |
| Data Warehouse | DuckDB |
| Transformations | dbt core, dbt duckdb |
| Dashboard | Streamlit, Plotly |
| Deployment | Streamlit Cloud |
| Version Control | Git, GitHub |

---

## Data Model

**5 source tables** — locations, employees, vehicles, sales_transactions, service_jobs

**5 silver models** — fully typed and tested staging layers

**5 gold models:**
- `daily_sales_by_location` — revenue and unit trends per branch
- `salesperson_leaderboard` — ranked performance with commission tracking
- `revenue_vs_target` — monthly pacing against branch targets
- `inventory_status` — available stock by make and location
- `service_center_utilization` — technician efficiency and revenue

---

## Data Quality

23 automated dbt tests covering:
- Primary key uniqueness
- Not null constraints on critical fields
- Accepted value validation
- Referential integrity across tables

---

## Dashboard Features

- 3 role based views with query level RBAC filtering
- Executive sees all 5 locations with company wide KPIs
- Branch managers see their location, team, inventory, and service data
- Salespeople see personal performance, commission, and company ranking
- All views respond to 5 live filters applied at the SQL level

---

## Running Locally
```bash
git clone https://github.com/kpk111298/kommineni-automotive-pipeline
cd kommineni-automotive-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Generate data
python data_generator/generate_data.py

# Ingest to bronze
python ingestion/ingest_bronze.py

# Run dbt transformations
cd dbt_project/kommineni_automotive
dbt run
dbt test

# Launch dashboard
cd ../../dashboard
streamlit run app.py
```

---

## Author

**Prameel Kommineni**
Data Engineer — Snowflake, AWS, Azure, Databricks, dbt
[LinkedIn](https://linkedin.com/in/pkomm) · [GitHub](https://github.com/kpk111298)