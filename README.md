# IPL Analytics Pipeline

An end-to-end data engineering pipeline that ingests, transforms, and loads
IPL cricket match data (2007-2018) into a PostgreSQL data warehouse,
orchestrated with Apache Airflow and visualised in Power BI.

## Architecture
CSV Files -> Python ETL Scripts -> PostgreSQL (Star Schema) -> Power BI Dashboard

^

Apache Airflow DAG

(scheduled daily at 6 AM)

## Star Schema
                dim_players
                    |
dim_venues - dim_matches - dim_teams

|

fact_deliveries

| Table | Rows | Description |
|---|---|---|
| dim_teams | 19 | All IPL franchises |
| dim_players | 732 | All batsmen, bowlers, fielders |
| dim_venues | 61 | Stadiums and cities |
| dim_matches | 1,095 | Match-level metadata |
| fact_deliveries | 260,920 | Ball-by-ball delivery data |

> **Note on season values:** The `season` column reflects the year the tournament started (cricket convention) - e.g. `season = 2007` represents the 2007/08 season, where all matches were actually played in April-May 2008. This matches how other sports leagues (e.g. NBA "2023-24 season") label cross-year seasons.

## Pipeline Steps

The Airflow DAG (`dags/ipl_pipeline_dag.py`) runs these tasks in order:

1. **validate_source_files** - checks CSV files exist and are not corrupt
2. **extract_data** - loads CSVs into memory, runs schema assertions
3. **load_dimensions** - populates dim_teams, dim_players, dim_venues, dim_matches
4. **load_facts** - loads 260,920 deliveries into fact_deliveries
5. **validate_load** - confirms row counts meet minimum thresholds

## Tech Stack

- **Python** - Pandas, psycopg2, SQLAlchemy
- **PostgreSQL 15** - Data warehouse
- **Apache Airflow 2.9** - Pipeline orchestration
- **Power BI** - Dashboard and KPI reporting
- **Git / GitHub** - Version control

## Setup Instructions

### 1. Clone the repo
```bash
git clone https://github.com/DineshReddy119/ipl-analytics-pipeline.git
cd ipl-analytics-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up PostgreSQL
```bash
psql -U postgres
CREATE USER ipl_user WITH PASSWORD 'ipl_pass';
CREATE DATABASE ipl_db OWNER ipl_user;
GRANT ALL PRIVILEGES ON DATABASE ipl_db TO ipl_user;
\q

psql -U ipl_user -h localhost -d ipl_db -f sql/create_tables.sql
```

### 4. Download the dataset
Download from Kaggle (search "IPL Complete Dataset 2008-2020")
and place matches.csv and deliveries.csv in the data/ folder.

### 5. Run the pipeline
```bash
python scripts/load.py
```

### 6. Run with Airflow (Linux/Mac)
```bash
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow dags trigger ipl_analytics_pipeline
```

## Power BI KPIs

| KPI | Description |
|---|---|
| Total runs by team | Season-wise run totals per franchise |
| Top 10 batsmen | By total runs and strike rate |
| Top 10 bowlers | By wickets and economy rate |
| Win % by team | Overall and season-wise |
| Toss impact | Win % when batting vs fielding first |
| Venue analysis | Avg scores and win % by venue |
| Season trends | Runs per over across seasons |
| Boundary % | 4s and 6s as % of total runs |
| Powerplay analysis | Runs and wickets in overs 1-6 |
| Death over analysis | Runs and wickets in overs 17-20 |
| Player of match leaders | Most awards by player |
| Super over matches | Matches decided by super over |

## Project Structure
ipl-analytics-pipeline/

|-- dags/

|   -- ipl_pipeline_dag.py    # Airflow DAG definition |-- data/ |   |-- matches.csv            # 1,095 IPL matches |   -- deliveries.csv         # 260,920 ball-by-ball records

|-- scripts/

|   |-- extract.py             # Data extraction and validation

|   -- load.py                # Transform and load to PostgreSQL |-- sql/ |   -- create_tables.sql      # Star schema DDL

|-- requirements.txt

`-- README.md

## Author

**Dinesh Reddy** - Data Engineer
[LinkedIn](https://linkedin.com/in/dinesh-reddy-de) | [GitHub](https://github.com/DineshReddy119)
