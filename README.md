## What This Repo Covers

Included:
- TPC-H generation
- Data cleaning
- Data validation
- Local Docker PostgreSQL load/restore

Not included yet:
- Wander Join algorithm implementation
- AWS Lambda orchestration
- Final report visualization code

## Project Layout

- `scripts/`: all automation scripts
- `sql/`: schema and verification SQL
- `docker-compose.yml`: PostgreSQL service
- `data/raw/.gitkeep`: placeholder for generated raw files
- `data/clean/.gitkeep`: placeholder for cleaned files
- `reports/.gitkeep`: placeholder for validation outputs

Note: generated datasets and local DB artifacts are intentionally ignored by Git. (Very large files)

## Prerequisites

- Python 3.10+
- PowerShell 5.1+
- Docker Desktop

## Choose One Setup Path

### Path A (Fastest): Restore From Shared Dump

Use this if you received `tpch_sf5.dump` from the Data Lead.

Google Drive download link: https://drive.google.com/file/d/1PYgIMOsjSiQiR_UDs3ROvJqQr6oBR0Z5/view?usp=sharing

1. Download `tpch_sf5.dump` from Google Drive.
2. Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_data_lead_pipeline.ps1 -DumpFile ".\tpch_sf5.dump"
```

What this does:
- Starts Docker PostgreSQL
- Restores the dump
- Runs row-count verification

### Path B (Fully Reproducible): Regenerate Everything

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_data_lead_pipeline.ps1 -Scale 5
```

What this does:
- Generates TPC-H SF5 raw files
- Cleans `.tbl` files
- Validates data integrity
- Loads data to Docker PostgreSQL
- Runs row-count verification

## Manual Commands (Fallback)

Use this only if you need step-by-step debugging.

```powershell
# 1) Environment setup
py -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# 2) Generate raw TPC-H SF5 data
python scripts/generate_tpch_duckdb.py --sf 5 --out-dir data/raw/sf5 --force

# 3) Clean raw files
python scripts/clean_tpch.py --input-dir data/raw/sf5 --output-dir data/clean/sf5 --force

# 4) Validate cleaned files
python scripts/validate_tpch.py --input-dir data/clean/sf5 --report reports/validation_sf5.json

# 5) Load into Docker PostgreSQL
powershell -ExecutionPolicy Bypass -File scripts/load_to_postgres.ps1 -Scale 5
```

## Verify Data Loaded Correctly

Run this command after Path A or Path B:

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_reviews -c "SELECT 'region' AS table_name, COUNT(*) AS row_count FROM region UNION ALL SELECT 'nation', COUNT(*) FROM nation UNION ALL SELECT 'supplier', COUNT(*) FROM supplier UNION ALL SELECT 'customer', COUNT(*) FROM customer UNION ALL SELECT 'part', COUNT(*) FROM part UNION ALL SELECT 'partsupp', COUNT(*) FROM partsupp UNION ALL SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'lineitem', COUNT(*) FROM lineitem ORDER BY table_name;"
```

Expected SF5 row counts:
- `region`: 5
- `nation`: 25
- `supplier`: 50,000
- `customer`: 750,000
- `part`: 1,000,000
- `partsupp`: 4,000,000
- `orders`: 7,500,000
- `lineitem`: 29,999,795

## Role-Based

- Data Lead: owns generation, cleaning, validation, and local DB preparation.
- Algorithm Engineer: consumes cleaned/loaded TPC-H tables for random walk logic.
- Cloud Architect: focuses on AWS setup; local Docker path is optional fallback.
- Analytics and Math Lead: uses validated counts and loaded tables for estimator/CI evaluation.
