import duckdb
import json
import random
import logging

# ==========================================
# WANDER JOIN: ALGORITHM ENGINEER'S MATH
# ==========================================
def single_walk(con):
    """
    Executes one random walk using DuckDB.
    Path: Customer -> Orders -> LineItem
    """
    # Step 1: Pick a random customer
    cust_res = con.execute("SELECT c_custkey FROM customer USING SAMPLE 1 ROWS;").fetchone()
    if not cust_res:
        return None
    custkey = cust_res[0]

    # Step 2: Look up orders for this customer
    orders = con.execute(f"SELECT o_orderkey FROM orders WHERE o_custkey = {custkey};").fetchall()
    if not orders:
        return None
    
    # Pick a random order from the list
    orderkey = random.choice(orders)[0]

    # Step 3: Look up line items for this order
    lineitems = con.execute(f"SELECT l_extendedprice FROM lineitem WHERE l_orderkey = {orderkey};").fetchall()
    if not lineitems:
        return None
    
    # Pick a random line item
    extendedprice = random.choice(lineitems)[0]

    # Step 4: Fanout weight calculation
    weight = len(orders) * len(lineitems)

    # Force the Decimal to a float, and the weight to an integer so JSON doesn't crash
    return {"value": float(extendedprice), "weight": int(weight)}

def run_walks(con, n_walks):
    """Runs N walks and returns the valid results."""
    results = []
    for _ in range(n_walks):
        outcome = single_walk(con)
        if outcome is not None:
            results.append(outcome)
    return results

# ==========================================
# AWS WORKER: INFRASTRUCTURE & ROUTING
# ==========================================
def handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    try:
        bucket = "tpch-sf5-data-2026"
        con = duckdb.connect()
        
        # 1. AWS Lambda Environment Fixes
        con.execute("SET home_directory='/tmp';")
        con.execute("SET extension_directory='/tmp';")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET max_memory='800MB';")
        con.execute("SET temp_directory='/tmp';")
        
        # 2. Map S3 Data to Virtual Tables
        tables = ['lineitem', 'orders', 'customer', 'part', 'partsupp', 'supplier', 'nation', 'region']
        for table in tables:
            s3_path = f"s3://{bucket}/{table}.parquet"
            con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{s3_path}');")
        
        # 3. The "Swiss Army Knife" Router
        sql_input = event.get('query')
        
        if sql_input:
            # MODE A: Standard SQL Query (For the Math Lead)
            logger.info(f"Executing Custom SQL: {sql_input}")
            res_df = con.execute(sql_input).df()
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "worker_id": event.get('worker_id', 'aws-worker'),
                    "results": res_df.to_dict('records'),
                    "status": "success"
                }, default=str) # default=str converts any weird datatypes (like dates) safely
            }
            
        else:
            # MODE B: Random Walk Engine (For the Algorithm Engineer)
            n_walks = event.get('num_walks', 100)
            logger.info(f"Starting {n_walks} Wander Join walks...")
            walk_results = run_walks(con, n_walks)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "worker_id": event.get('worker_id', 'aws-worker'),
                    "successful_walks": len(walk_results),
                    "results": walk_results, 
                    "status": "success"
                })
            }

    except Exception as e:
        logger.error(f"Execution Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }