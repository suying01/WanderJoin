## What This Repo Covers

Included:
- TPC-H generation
- Data cleaning
- Data validation
- Local Docker PostgreSQL load/restore
- Wander Join random walk engine (index builder + sampling)
- **Horvitz-Thompson estimator** (unbiased aggregate estimation with confidence intervals)
- **Adaptive stopping condition** (95% CI with ±1% relative error threshold)
- **Visualizations** (convergence, distributions, accuracy metrics)
- Multiprocessing / AWS Lambda orchestration (cloud architect)

## Project Layout

- `scripts/build_indexes.py`: loads TPC-H .tbl files and builds in-memory join indexes
- `scripts/wander_join.py`: core random walk engine (Customer -> Orders -> LineItem)
- `scripts/test_walk.py`: sanity-check script that runs walks and prints weighted average
- `scripts/horvitz_thompson_estimator.py`: **Horvitz-Thompson ratio estimator with 95% confidence intervals & stopping condition**
- `scripts/visualizations.py`: **Chart generation (convergence, distributions, error decay)**
- `scripts/analyze_wanderjoin.py`: **Complete end-to-end workflow with adaptive stopping** (recommended entry point for local execution)
- `scripts/analyze_wanderjoin_cloud.py`: **Complete end-to-end workflow with adaptive stopping** (recommended entry point for cloud execution)
- `scripts/gather.py`: AWS Lambda orchestration script for scatter walk
- `scripts/`: all other automation scripts (generation, cleaning, validation, loading)
- `scatterworker/`: AWS Lambda function for scatter walk
- `sql/`: schema and verification SQL
- `docker-compose.yml`: PostgreSQL service
- `data/raw/.gitkeep`: placeholder for generated raw files
- `data/clean/.gitkeep`: placeholder for cleaned files
- `reports/.gitkeep`: placeholder for validation outputs

Note: generated datasets and local DB artifacts are intentionally ignored by Git. (Very large files)
Note: the shared SF5 parquet file is available on Google Drive: https://drive.google.com/file/d/1RJuN50Dl_PdfzuyUEoNx-n4YQjN0Mvd6/view?usp=sharing

## Prerequisites

- Python 3.10+
- PowerShell 5.1+
- Docker Desktop

## Setup (SF5 parquet file)

Use this default command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_data_lead_pipeline.ps1 -Scale 5 -SkipDocker -ExportParquet
```

What this does:
- Generates TPC-H SF5 raw files
- Cleans `.tbl` files
- Validates data integrity (PK/FK and generated-vs-cleaned row count consistency)
- Exports parquet files to `data/clean/sf5/parquet`

Typical runtime on SF5: several minutes, depending on CPU/disk speed.

If you only need parquet data (no local regeneration), download the shared SF5 parquet file from Google Drive and place it under `data/clean/sf5/parquet`.

If you also need Docker PostgreSQL loaded, run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_data_lead_pipeline.ps1 -Scale 5
```

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

# 5) Export parquet files
python scripts/export_tpch_parquet.py --input-dir data/clean/sf5 --output-dir data/clean/sf5/parquet --force

# 6) Load into Docker PostgreSQL (optional)
powershell -ExecutionPolicy Bypass -File scripts/load_to_postgres.ps1 -Scale 5
```

## Verify Data Loaded Correctly

Run this command after Docker load:

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

## Horvitz-Thompson Estimator & Analysis

### Overview

The Horvitz-Thompson ratio estimator converts random walk samples into **unbiased aggregate estimates** with **95% confidence intervals**. The stopping condition automatically determines when sufficient accuracy (±1% relative error) is achieved.

**Key insight:** Each sample is weighted by the fanout product, which ensures that tuples reached through high-fanout paths are correctly downweighted in the final estimate.

### Mathematical Foundation

Given samples with values $v_i$ and weights $w_i$:

$$\hat{\mu} = \frac{\sum v_i w_i}{\sum w_i}$$

The variance is estimated via the delta method:

$$\widehat{\text{Var}}(\hat{\mu}) \approx \frac{1}{(\sum w_i)^2} \sum w_i^2 (v_i - \hat{\mu})^2$$

The **stopping condition** checks if the margin of error meets the target:

$$\text{Margin of Error} = 1.96 \cdot \sigma(\hat{\mu}) < \epsilon \cdot |\hat{\mu}|$$

For 95% confidence and ±1% relative error: $\epsilon = 0.01$.

### End-to-End Analysis (Recommended)

Run the complete workflow with adaptive batch sampling and automatic stopping:

```bash
python scripts/analyze_wanderjoin.py \
  --data-dir data/clean/sf1 \
  --batch-size 2000 \
  --max-batches 50 \
  --rel-error-threshold 0.01 \
  --output-dir reports
```

**Output:**
- Console report with convergence statistics
- `reports/estimate_summary.txt`: point estimates and confidence intervals
- `reports/01_value_distribution.png`: histogram of sampled extended prices
- `reports/02_weight_distribution.png`: fanout weight distribution (linear & log scale)

### Programmatic Usage

```python
from horvitz_thompson_estimator import HorvitzThompsonEstimator

# Initialize with estimated population size
pop_size = len(customers) * avg_orders_per_cust * avg_items_per_order
estimator = HorvitzThompsonEstimator(population_size=pop_size)

# Add samples from walks
estimator.add_samples(walk_results)

# Check accuracy
point_estimate = estimator.estimate_mean()
total_estimate = estimator.estimate_total()
ci_lower, ci_upper, moe = estimator.confidence_interval_95()

# Check stopping condition (95% CI, ±1% relative error)
should_stop, reason = estimator.should_stop_sampling(rel_error_threshold=0.01)
print(f"Relative Error: {estimator.coefficient_of_variation()*100:.3f}%")
print(reason)
```

## Report Visualization

Generate publication-quality charts:

```python
from visualizations import WanderJoinVisualizer

visualizer = WanderJoinVisualizer()

# Value distribution (what gets sampled)
visualizer.plot_value_distribution(values, output_file="reports/values.png")

# Weight distribution (fanout variance)
visualizer.plot_weight_distribution(weights, output_file="reports/weights.png")
```

### Expected Output (SF1, 10k walks)

```
Successful walks : ~6,600
Dead ends        : ~3,400 (33.9%)
Raw weighted avg : ~$38,000
```

The ~34% dead-end rate is expected — SF1 has 150k customers but only ~100k have orders.

## Multiprocessing / AWS Lambda Orchestration
This system utilizes a "Scatter-Gather" orchestration pattern.
* **Storage:** Amazon S3 hosting TPC-H Parquet files.
* **Compute Engine:** AWS Lambda running a custom Amazon Linux 2023 Docker container (Python 3.12).
* **Database Engine:** DuckDB using the `httpfs` extension.
* **Orchestrator:** A local Python script utilizing `concurrent.futures` and `boto3`.

To replicate this experiment, you must configure your own AWS environment. 

#### S3 Storage
Create an S3 bucket in your deployment region and upload the 8 TPC-H tables as Parquet files (`customer.parquet`, `orders.parquet`, `lineitem.parquet`, etc.).

#### IAM Execution Role
Create an IAM Role for your Lambda function. Attach the managed `AWSLambdaBasicExecutionRole` policy, and create the following inline policy to grant DuckDB access to your specific bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": "arn:aws:s3:::<YOUR_BUCKET_NAME>"
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": "arn:aws:s3:::<YOUR_BUCKET_NAME>/*"
        }
    ]
}
```

#### Docker Deployment (ECR)
AWS Lambda requires a custom container to ensure `glibc` compatibility with DuckDB. Replace the placeholders below and run these commands to push the container to your Elastic Container Registry (ECR). 

*(Note: If building on an Apple Silicon/M-Series Mac, the `--platform linux/amd64` flag is mandatory).*

```bash
# 1. Authenticate Docker with ECR
aws ecr get-login-password --region <YOUR_AWS_REGION> | docker login --username AWS --password-stdin <YOUR_AWS_ACCOUNT_ID>.dkr.ecr.<YOUR_AWS_REGION>.amazonaws.com

# 2. Build the image
docker build --platform linux/amd64 -t wander-join-worker .

# 3. Tag the image
docker tag wander-join-worker:latest <YOUR_AWS_ACCOUNT_ID>.dkr.ecr.<YOUR_AWS_REGION>[.amazonaws.com/wander-join-worker:latest](https://.amazonaws.com/wander-join-worker:latest)

# 4. Push to ECR
docker push <YOUR_AWS_ACCOUNT_ID>.dkr.ecr.<YOUR_AWS_REGION>[.amazonaws.com/wander-join-worker:latest](https://.amazonaws.com/wander-join-worker:latest)
```

#### Lambda Configuration
Create a new Lambda function from the uploaded Container Image. **You must immediately update the default configuration:**
* **Memory:** `2048 MB` (Critical for S3 network I/O speed).
* **Timeout:** `3 min 0 sec`.

#### Local Execution

Before running the orchestrator, export your AWS IAM User credentials to your local environment so `boto3` can authenticate the Lambda invocations:

```bash
export AWS_ACCESS_KEY_ID="<YOUR_IAM_ACCESS_KEY>"
export AWS_SECRET_ACCESS_KEY="<YOUR_IAM_SECRET_KEY>"
export AWS_DEFAULT_REGION="<YOUR_AWS_REGION>"
```

#### Running the Orchestrator
Inside `scripts/gather.py`, you can tune `NUM_WORKERS` to scale horizontally. Ensure `WALKS_PER_WORKER` stays low enough (e.g., 100) to avoid cloud execution timeouts.

```bash
python gather.py
```


## Role-Based

- Data Lead: owns generation, cleaning, validation, and local DB preparation.
- Algorithm Engineer: owns `build_indexes.py`, `wander_join.py`, and `test_walk.py`.
- Cloud Architect: focuses on AWS setup; will wrap `run_walks()` in Lambda handler.
- Analytics and Math Lead: plugs into `run_walks()` output to build Horvitz-Thompson estimator and confidence intervals.
