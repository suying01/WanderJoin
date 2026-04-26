import os
import time
import json
import boto3
import duckdb
import numpy as np

# --- CONFIGURATION ---
S3_BUCKET = "tpch-sf5-data-2026"
DB_FILE_NAME = "tpch_sf5.db"
LOCAL_PATH = f"/tmp/{DB_FILE_NAME}"
SAMPLE_LIMIT = 5000  # NEW: Force return after 5k samples to allow parallelism

s3 = boto3.client('s3')


def handler(event, context):
    start_time = time.time()

    # 1. HYDRATION (Fat Worker Pattern)
    if not os.path.exists(LOCAL_PATH):
        try:
            s3.download_file(S3_BUCKET, DB_FILE_NAME, LOCAL_PATH)
            os.sync()
        except Exception as e:
            return {'statusCode': 500, 'errorMessage': f"Hydration Failed: {str(e)}"}

    # 2. CONNECT
    try:
        con = duckdb.connect(database=LOCAL_PATH, read_only=True)
    except Exception as e:
        return {'statusCode': 500, 'errorMessage': f"DuckDB Error: {str(e)}"}

    # 3. SEEDING
    seed_str = f"{context.aws_request_id}_{time.time_ns()}"
    rng = np.random.default_rng(abs(hash(seed_str)) % (2 ** 32))

    results = []
    total_attempts = 0
    dead_ends = 0

    # 4. MODIFIED SAMPLING LOOP (The "Sprinter" Logic)
    # We stop IF we hit the 5,000 sample limit OR the 30s circuit breaker
    while len(results) < SAMPLE_LIMIT and context.get_remaining_time_in_millis() > 30000:
        total_attempts += 1
        walk_result = run_single_walk(con, rng)

        if walk_result is None:
            dead_ends += 1
        elif walk_result is not False:
            results.append(walk_result)

    con.close()

    return {
        'statusCode': 200,
        'batch_size': len(results),
        'total_attempts': total_attempts,
        'dead_ends': dead_ends,
        'samples': results,
        'hydration_time_s': time.time() - start_time if "start_time" in locals() else 0
    }


def run_single_walk(con, rng):
    # (Keep your existing run_single_walk logic here)
    try:
        cust = con.execute("SELECT c_custkey FROM customer USING SAMPLE 1 ROWS").fetchone()
        if not cust: return None
        c_key = cust[0]

        orders = con.execute("SELECT o_orderkey FROM orders WHERE o_custkey = ?", [c_key]).fetchall()
        if not orders: return None

        o_fanout = len(orders)
        o_idx = rng.integers(0, o_fanout)
        o_key = orders[o_idx][0]

        lineitems = con.execute("SELECT l_extendedprice FROM lineitem WHERE l_orderkey = ?", [o_key]).fetchall()
        if not lineitems: return None

        l_fanout = len(lineitems)
        l_idx = rng.integers(0, l_fanout)
        price = lineitems[l_idx][0]

        return {'value': float(price), 'weight': float(o_fanout * l_fanout)}
    except:
        return False