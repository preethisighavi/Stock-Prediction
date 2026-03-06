# Data Warehousing Labs (Airflow + Snowflake + dbt)

This single repository contains **two lab projects**:

- **Lab 1 – Airflow ETL + prediction**: pulls stock prices, loads to Snowflake, and writes simple forecasts back to Snowflake.
- **Lab 2 – Airflow-orchestrated dbt ELT**: uses dbt models/tests/snapshots on stock data in Snowflake, triggered by Airflow.
---

## Repository structure

```
.
├── lab1_airflow_stock_etl/
│   ├── ML_ETL_stocks_7_updated.py
│   ├── README.md
│   └── Screenshots/
│       └── ...
├── lab2_dbt_airflow_elt/
│   ├── README.md
│   ├── snowflake.sql
│   ├── dags/
│   │   ├── dbt_dag.py
│   │   └── etl_lab2-2.py
│   └── build_ELT/
│       ├── dbt_project.yml
│       ├── models/
│       │   ├── input/
│       │   │   ├── sources.yml
│       │   │   ├── schema.yml
│       │   │   └── stock_prices.sql
│       │   └── output/
│       │       └── stock_calcs.sql
│       ├── snapshots/
│       │   └── snapshot_stock_calcs.sql
│       └── profiles.yml.example
└── .gitignore
```

---

## What to add (commit) to Git

###  Safe to commit
- All **source code** (`.py`, `.sql`, `.yml` configs that are not secrets)
- Documentation (`README.md`)
- Lab screenshots (optional, but fine)

Specifically, keep:
- `lab1_airflow_stock_etl/**` *(everything inside is OK as provided)*
- `lab2_dbt_airflow_elt/dags/**`
- `lab2_dbt_airflow_elt/build_ELT/dbt_project.yml`
- `lab2_dbt_airflow_elt/build_ELT/models/**`
- `lab2_dbt_airflow_elt/build_ELT/snapshots/**`
- `lab2_dbt_airflow_elt/snowflake.sql`
- `lab2_dbt_airflow_elt/build_ELT/profiles.yml.example`
- `.gitignore`
- This top-level `README.md`

### ❌ Do NOT commit (generated or secret)
- `lab2_dbt_airflow_elt/build_ELT/profiles.yml` (contains credentials)
- `lab2_dbt_airflow_elt/build_ELT/target/` (dbt build artifacts)
- `lab2_dbt_airflow_elt/build_ELT/logs/` (local logs)
- `__pycache__/`, `*.pyc` (Python bytecode)
- Any `.env` file with secrets

The included `.gitignore` already excludes these.

---

## Changes you should make before pushing to GitHub

### 1) Remove Snowflake credentials from the repo (required)
In the original Lab 2 zip, `build_ELT/profiles.yml` contains a real Snowflake **account/user/password**.
Do this instead:

1. Copy the example to a real profile file **locally** (but keep it ignored by git):
   ```bash
   cp lab2_dbt_airflow_elt/build_ELT/profiles.yml.example lab2_dbt_airflow_elt/build_ELT/profiles.yml
   ```
2. Provide credentials via environment variables (example):
   ```bash
   export SNOWFLAKE_ACCOUNT="xxxxxx"
   export SNOWFLAKE_USER="xxxxxx"
   export SNOWFLAKE_PASSWORD="xxxxxx"
   export SNOWFLAKE_ROLE="ACCOUNTADMIN"
   export SNOWFLAKE_DATABASE="stock_db"
   export SNOWFLAKE_WAREHOUSE="compute_wh"
   export SNOWFLAKE_SCHEMA="analytics"
   ```

> Alternative: place your real dbt profile under `~/.dbt/profiles.yml` (dbt default) and don’t store it in the project folder.

### 2) Ensure Airflow uses Connections/Variables (recommended)
- Lab 1 already reads the AlphaVantage key via Airflow Variable: `vantage_api_key`.
- For Snowflake access, store credentials in an **Airflow connection** (e.g., `snowflake_default`) rather than hardcoding.

---

## How to run (high level)

### Lab 1 (Airflow ETL + prediction)
1. Put `lab1_airflow_stock_etl/ML_ETL_stocks_7_updated.py` into your Airflow `dags/` folder (or add a symlink).
2. Create Airflow Variable:
   - `vantage_api_key` = your Alpha Vantage API key
3. Create/verify your Airflow Snowflake connection in the Airflow UI.
4. Trigger the DAG from the Airflow UI.

### Lab 2 (dbt ELT + Airflow orchestration)
1. Create the Snowflake database/schema using `lab2_dbt_airflow_elt/snowflake.sql` (or ensure they exist).
2. Configure dbt credentials (see **Changes you should make** above).
3. From `lab2_dbt_airflow_elt/build_ELT/` run:
   ```bash
   dbt run
   dbt test
   dbt snapshot
   ```
4. Put the Airflow DAG(s) from `lab2_dbt_airflow_elt/dags/` into your Airflow `dags/` folder and trigger them.

---

## Notes
- If you’re deploying to a shared Airflow environment, keep paths in the DAGs (`DBT_PROJECT_DIR`) consistent with where the dbt project is mounted.
- If you want a cleaner separation, you can rename folders (e.g., `lab1/`, `lab2/`)—it won’t affect functionality as long as Airflow paths match.

