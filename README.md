# Serverless Wander Join
## Adapting Approximate Query Processing for Ephemeral Cloud Architectures

This project implements the Wander Join algorithm (Li et al., SIGMOD 2016) across three deployment tiers: local single-core, stateless serverless (V1), and state-hydrated serverless (V2). The goal is to evaluate whether approximate query processing can be made practical in ephemeral cloud environments.

---

## What This Repo Covers

- TPC-H generation, cleaning, and validation
- Local Docker PostgreSQL load/restore
- Wander Join random walk engine (index builder + sampling)
- **Horvitz-Thompson estimator** (unbiased aggregate estimation with confidence intervals)
- **Adaptive stopping condition** (95% CI with ±1% relative error threshold)
- **Visualizations** (convergence, distributions, accuracy metrics)
- **V1 Stateless Worker** — scatter-gather orchestration via AWS Lambda + DuckDB httpfs
- **V2 State-Hydrated Worker** — one-time S3 hydration to local NVMe, high-frequency local sampling

---

## Project Layout

```
scripts/
  build_indexes.py                  # Loads TPC-H .tbl files and builds in-memory join indexes
  wander_join.py                    # Core random walk engine (Customer -> Orders -> LineItem)
  test_walk.py                      # Sanity-check script for local walk execution
  horvitz_thompson_estimator.py     # HT ratio estimator with 95% CI and stopping condition (V1)
  horvitz_thompson_estimator_v2.py  # HT estimator variant used in V2 orchestration
  visualizations.py                 # Chart generation (convergence, distributions, error decay)
  analyze_wanderjoin.py             # End-to-end local workflow with adaptive stopping
  analyze_wanderjoin_cloud.py       # V1 cloud orchestration (stateless, S3 streaming)
  analyze_wanderjoin_cloud_v2.py    # V2 cloud orchestration (state-hydrated, local NVMe)
  gather.py                         # AWS Lambda scatter orchestration (V1)
  run_data_lead_pipeline.ps1        # PowerShell pipeline: generate -> clean -> validate -> export

scatterworker/                      # V1 Lambda function (stateless, DuckDB httpfs)
scatterworker_v2/                   # V2 Lambda function (state-hydrated, local NVMe sampling)

sql/                                # Schema and verification SQL
docker-compose.yml                  # PostgreSQL service
data/raw/.gitkeep                   # Placeholder for generated raw files
data/clean/.gitkeep                 # Placeholder for cleaned files
reports/.gitkeep                    # Placeholder for validation outputs
```

> **Note:** Generated datasets and local DB artifacts are intentionally ignored by Git (very large files).  
> The shared SF5 DuckDB database file is available on Google Drive: https://drive.google.com/file/d/1RJuN50Dl_PdfzuyUEoNx-n4YQjN0Mvd6/view?usp=sharing

---

## Prerequisites

- Python 3.10+
- PowerShell 5.1+
- Docker Desktop

---

## Setup

### Generate SF5 Parquet Files

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_data_lead_pipeline.ps1 -Scale 5 -SkipDocker -ExportParquet
```

This generates TPC-H SF5 raw files, cleans and validates them, and exports Parquet files to `data/clean/sf5/parquet`.

If you only need the Parquet data without local regeneration, download the shared SF5 file from Google Drive and place it under `data/clean/sf5/parquet`.

To also load into Docker PostgreSQL:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_data_lead_pipeline.ps1 -Scale 5
```

### Manual Setup (Fallback)

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

# 5) Export Parquet files
python scripts/export_tpch_parquet.py --input-dir data/clean/sf5 --output-dir data/clean/sf5/parquet --force

# 6) Load into Docker PostgreSQL (optional)
powershell -ExecutionPolicy Bypass -File scripts/load_to_postgres.ps1 -Scale 5
```

### Verify Data Loaded Correctly

```powershell
docker compose exec -T postgres psql -U postgres -d amazon_reviews -c "SELECT 'region' AS table_name, COUNT(*) AS row_count FROM region UNION ALL SELECT 'nation', COUNT(*) FROM nation UNION ALL SELECT 'supplier', COUNT(*) FROM supplier UNION ALL SELECT 'customer', COUNT(*) FROM customer UNION ALL SELECT 'part', COUNT(*) FROM part UNION ALL SELECT 'partsupp', COUNT(*) FROM partsupp UNION ALL SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'lineitem', COUNT(*) FROM lineitem ORDER BY table_name;"
```

Expected SF5 row counts: `region`: 5, `nation`: 25, `supplier`: 50,000, `customer`: 750,000, `part`: 1,000,000, `partsupp`: 4,000,000, `orders`: 7,500,000, `lineitem`: 29,999,795

---

## Wander Join Algorithm

Samples from the join path **Customer → Orders → LineItem** using random walks.

### Quick Start (Local, No Docker)

```bash
pip install -r requirements.txt

python scripts/generate_tpch_duckdb.py --sf 1 --out-dir data/raw/sf1
python scripts/clean_tpch.py --input-dir data/raw/sf1 --output-dir data/clean/sf1

cd scripts && python test_walk.py --data-dir ../data/clean/sf1
```

### How It Works

1. **`build_indexes.py`** reads `.tbl` files and builds two `defaultdict(list)` indexes:
   - `orders_by_custkey[custkey]` → list of order dicts
   - `lineitems_by_orderkey[orderkey]` → list of lineitem dicts

2. **`wander_join.py`** performs random walks:
   - Pick a random customer → look up their orders → pick one → look up its line items → pick one
   - If any step has no matches, the walk is a **dead end** (returns `None`)
   - Each successful walk returns `{'value': extendedprice, 'weight': fanout}` where `weight = len(orders) * len(lineitems)`

3. The **weight** corrects for non-uniform inclusion probability in the Horvitz-Thompson estimator.

### Local End-to-End Analysis

```bash
python scripts/analyze_wanderjoin.py \
  --data-dir data/clean/sf5 \
  --batch-size 2000 \
  --max-batches 50 \
  --rel-error-threshold 0.01 \
  --output-dir reports
```

> Sampling terminates early when the ±1% relative error stopping condition is met. The 50-batch maximum is a safety ceiling and is rarely reached.

**Output:**
- Console convergence report
- `reports/01_value_distribution.png`
- `reports/02_weight_distribution.png`
- `reports/03_convergence.png`
- `reports/04_error_decay.png`
- `reports/05_ci_width.png`
- `reports/estimate_summary.txt`

---

## Horvitz-Thompson Estimator

### Mathematical Foundation

Given samples with values $v_i$ and weights $w_i$:

$$\hat{\mu} = \frac{\sum v_i w_i}{\sum w_i}$$

Variance via the delta method:

$$\widehat{\text{Var}}(\hat{\mu}) \approx \frac{1}{(\sum w_i)^2} \sum w_i^2 (v_i - \hat{\mu})^2$$

Stopping condition:

$$\text{Margin of Error} = 1.96 \cdot \sqrt{\widehat{\text{Var}}(\hat{\mu})} < \epsilon \cdot |\hat{\mu}|$$

For 95% confidence and ±1% relative error: $\epsilon = 0.01$.

---

## Cloud Architecture

### Tier Comparison

| Feature | V1 Stateless Worker | V2 State-Hydrated Worker |
|---|---|---|
| Data Access | DuckDB httpfs (S3 streaming) | One-time S3 download to `/tmp` NVMe |
| Bottleneck | Network latency (10–100ms/lookup) | Local SSD I/O (<1ms/lookup) |
| Throughput | ~20 samples/sec | ~334 samples/sec |
| Convergence | Stalled at 1.25% (did not meet target) | 0.91% in 54 seconds |
| Lambda Script | `analyze_wanderjoin_cloud.py` | `analyze_wanderjoin_cloud_v2.py` |
| Lambda Handler | `scatterworker/` | `scatterworker_v2/` |

### V1 Failure Analysis

V1 routes each random walk lookup through S3 via DuckDB's `httpfs` extension. Because Wander Join requires thousands of random-access lookups per query, each incurring a 10–100ms network round-trip, throughput is structurally capped at ~20 samples/second regardless of runtime. Due to AWS account-level concurrency limits, V1 was constrained to 10 concurrent workers with 100 walks per invocation.

### V2 Engineering Contributions

V2 overcomes the V1 network bottleneck through three systems-level innovations:

1. **State-Hydrated Architecture:** Each Lambda worker downloads the full 2.4 GB DuckDB database to `/tmp` NVMe storage at invocation. All subsequent walk lookups are resolved locally, eliminating network latency from the hot path.

2. **Distributed Entropy and PRNG Independence:** In a distributed fan-out, naive seeding produces correlated walks, invalidating the Horvitz-Thompson estimator's independence assumption. V2 uses a cryptographically-grounded seeding protocol combining `aws_request_id` and nanosecond-precision timestamps to ensure stochastic independence across parallel workers.

3. **Fault-Tolerant Estimator Lifecycle (Circuit Breaker):** AWS Lambda's hard timeout can kill workers mid-walk, introducing selection bias if incomplete results are discarded. V2 monitors remaining execution time via the Lambda context object and packages results before termination, ensuring the final aggregate estimator remains unbiased.

---

## AWS Setup

### S3 Storage

Create an S3 bucket in your deployment region. For V1, upload the 8 TPC-H tables as Parquet files. For V2, upload the compiled DuckDB `.db` file.

### IAM Execution Role

Create an IAM Role for your Lambda function with `AWSLambdaBasicExecutionRole` and the following inline policy:

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

### Docker Deployment (ECR)

> If building on Apple Silicon (M-series Mac), the `--platform linux/amd64` flag is mandatory.

```bash
# 1. Authenticate Docker with ECR
aws ecr get-login-password --region <YOUR_AWS_REGION> | docker login --username AWS --password-stdin <YOUR_AWS_ACCOUNT_ID>.dkr.ecr.<YOUR_AWS_REGION>.amazonaws.com

# 2. Build the image (replace DIR with scatterworker or scatterworker_v2)
docker build --platform linux/amd64 -t wander-join-worker ./<DIR>

# 3. Tag the image
docker tag wander-join-worker:latest <YOUR_AWS_ACCOUNT_ID>.dkr.ecr.<YOUR_AWS_REGION>.amazonaws.com/wander-join-worker:latest

# 4. Push to ECR
docker push <YOUR_AWS_ACCOUNT_ID>.dkr.ecr.<YOUR_AWS_REGION>.amazonaws.com/wander-join-worker:latest
```

### Lambda Configuration

| Setting | V1 | V2 |
|---|---|---|
| Memory | 1,024 MB | 4,096 MB |
| Ephemeral Storage | 512 MB | 6,144 MB |
| Timeout | 3 min | 3 min (accounts for ~50s hydration + sampling) |
| Handler | `scatterworker/` image | `scatterworker_v2/` image |

### Local Credentials

```bash
export AWS_ACCESS_KEY_ID="<YOUR_IAM_ACCESS_KEY>"
export AWS_SECRET_ACCESS_KEY="<YOUR_IAM_SECRET_KEY>"
export AWS_DEFAULT_REGION="<YOUR_AWS_REGION>"
```

### Running the Orchestrators

**V1 (Stateless):**
```bash
python scripts/analyze_wanderjoin_cloud.py --workers 10
```

**V2 (State-Hydrated):**
```bash
python scripts/analyze_wanderjoin_cloud_v2.py --workers 10
```

Both scripts use adaptive stopping and will terminate early once ±1% relative error is achieved. V2 outputs a `reports/v2_stats.json` file with full experiment metrics in addition to the standard plots.

---

## Role-Based Responsibilities

- **Data Lead:** owns generation, cleaning, validation, and local DB preparation
- **Algorithm Engineer:** owns `build_indexes.py`, `wander_join.py`, and `test_walk.py`
- **Cloud Architect:** owns AWS setup, `scatterworker/`, `scatterworker_v2/`, and Lambda configuration
- **Analytics & Math Lead:** owns `horvitz_thompson_estimator.py`, `horvitz_thompson_estimator_v2.py`, `visualizations.py`, and analysis scripts