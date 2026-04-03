## What This Repo Covers

Included:
- TPC-H generation
- Data cleaning
- Data validation
- Local Docker PostgreSQL load/restore
- Wander Join random walk engine (index builder + sampling)

Not included yet:
- Horvitz-Thompson estimator (math lead)
- Multiprocessing / AWS Lambda orchestration (cloud architect)
- Final report visualization code

## Project Layout

- `scripts/build_indexes.py`: loads TPC-H .tbl files and builds in-memory join indexes
- `scripts/wander_join.py`: core random walk engine (Customer -> Orders -> LineItem)
- `scripts/test_walk.py`: sanity-check script that runs walks and prints weighted average
- `scripts/`: all other automation scripts (generation, cleaning, validation, loading)
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
- Validates data integrity (PK/FK and generated-vs-cleaned row count consistency)
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
python scripts/validate_tpch.py --input-dir data/clean/sf5 --report reports/validation_sf5.json --metadata data/raw/sf5/generation_metadata.json

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

## Wander Join Algorithm

The Wander Join implementation samples from the join path **Customer -> Orders -> LineItem** using random walks (Li et al., SIGMOD 2016).

### Quick Start (no Docker needed)

```bash
# Install dependencies
pip install -r requirements.txt

# Generate and clean TPC-H SF1 data (~1GB)
python scripts/generate_tpch_duckdb.py --sf 1 --out-dir data/raw/sf1
python scripts/clean_tpch.py --input-dir data/raw/sf1 --output-dir data/clean/sf1

# Run 10,000 random walks
cd scripts && python test_walk.py --data-dir ../data/clean/sf1
```

### How It Works

1. **`build_indexes.py`** reads the `.tbl` files and builds two `defaultdict(list)` indexes:
   - `orders_by_custkey[custkey]` -> list of order dicts
   - `lineitems_by_orderkey[orderkey]` -> list of lineitem dicts

2. **`wander_join.py`** performs random walks:
   - Pick a random customer -> look up their orders -> pick one -> look up its line items -> pick one
   - If a step has no matches, the walk is a **dead end** (returns `None`)
   - Each successful walk returns `{'value': extendedprice, 'weight': fanout}` where `weight = len(orders) * len(lineitems)` — the product of choices at each join step

3. The **weight** is critical for the Horvitz-Thompson estimator: it corrects for the non-uniform probability of reaching each join tuple. Without it, customers with fewer orders would be overrepresented.

### Importing in Other Modules

```python
from build_indexes import load_and_index
from wander_join import run_walks

customers, orders_idx, lineitems_idx = load_and_index("data/clean/sf1")
results = run_walks(n_walks=50000, customers=customers,
                    orders_idx=orders_idx, lineitems_idx=lineitems_idx)
# results: list of {'value': float, 'weight': int}
```

### Expected Output (SF1, 10k walks)

```
Successful walks : ~6,600
Dead ends        : ~3,400 (33.9%)
Raw weighted avg : ~$38,000
```

The ~34% dead-end rate is expected — SF1 has 150k customers but only ~100k have orders.

## Role-Based

- Data Lead: owns generation, cleaning, validation, and local DB preparation.
- Algorithm Engineer: owns `build_indexes.py`, `wander_join.py`, and `test_walk.py`.
- Cloud Architect: focuses on AWS setup; will wrap `run_walks()` in Lambda handler.
- Analytics and Math Lead: plugs into `run_walks()` output to build Horvitz-Thompson estimator and confidence intervals.
