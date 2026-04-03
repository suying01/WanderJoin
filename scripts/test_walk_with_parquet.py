#!/usr/bin/env python3
"""
Wander Join integration test using PARQUET files.

Demonstrates that the Wander Join algorithm works with parquet-formatted data.
This wrapper loads parquet files and converts them to the format expected by
the core Wander Join samplers, without modifying the original algorithm code.

Usage:
    python scripts/test_walk_with_parquet.py --data-dir data/clean/sf1/parquet
    python scripts/test_walk_with_parquet.py --data-dir data/clean/sf5/parquet
"""

import argparse
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

from wander_join import run_walks


def load_and_index_from_parquet(parquet_dir):
    """Load TPC-H parquet files and build in-memory indexes for random walks.
    
    Parquet loader variant that reads from .parquet files instead of .tbl,
    then builds the same index structures as the original build_indexes.py.
    """
    parquet_dir = Path(parquet_dir)
    
    # Load parquet files
    print(f"Loading customer.parquet ...")
    customers = pd.read_parquet(parquet_dir / "customer.parquet")
    cust_count = len(customers)
    print(f"  {cust_count:,} customers loaded")
    
    print(f"Loading orders.parquet ...")
    orders = pd.read_parquet(parquet_dir / "orders.parquet")
    ord_count = len(orders)
    print(f"  {ord_count:,} orders loaded")
    
    print(f"Loading lineitem.parquet ...")
    lineitems = pd.read_parquet(parquet_dir / "lineitem.parquet")
    li_count = len(lineitems)
    print(f"  {li_count:,} lineitems loaded")
    
    # Build index: orders_by_custkey
    print(f"Building orders index...")
    orders_by_custkey = defaultdict(list)
    for _, row in orders.iterrows():
        orders_by_custkey[row["o_custkey"]].append({
            "o_orderkey": row["o_orderkey"],
            "o_totalprice": row["o_totalprice"],
        })
    distinct_custs = len(orders_by_custkey)
    print(f"  {distinct_custs:,} distinct customers with orders")
    
    # Build index: lineitems_by_orderkey
    print(f"Building lineitem index...")
    lineitems_by_orderkey = defaultdict(list)
    for _, row in lineitems.iterrows():
        lineitems_by_orderkey[row["l_orderkey"]].append({
            "l_extendedprice": row["l_extendedprice"],
            "l_discount": row["l_discount"],
        })
    distinct_orders = len(lineitems_by_orderkey)
    print(f"  {distinct_orders:,} distinct orders with lineitems")
    
    # Convert customers to list of dicts with c_custkey for uniform sampling
    customers_list = customers[["c_custkey"]].to_dict(orient="records")
    
    return customers_list, orders_by_custkey, lineitems_by_orderkey


def main():
    parser = argparse.ArgumentParser(description="Wander Join with Parquet files")
    parser.add_argument(
        "--data-dir",
        default="data/clean/sf1/parquet",
        help="Path to directory with customer.parquet, orders.parquet, lineitem.parquet",
    )
    parser.add_argument(
        "--n-walks",
        type=int,
        default=10_000,
        help="Number of random walks to run (default: 10000)",
    )
    args = parser.parse_args()

    # Build indexes from parquet
    print(f"\n{'='*60}")
    print("Building indexes from PARQUET files...")
    print(f"{'='*60}")
    t0 = time.perf_counter()
    customers, orders_idx, lineitems_idx = load_and_index_from_parquet(args.data_dir)
    t_index = time.perf_counter() - t0
    print(f"Index build time: {t_index:.2f}s\n")

    # Run walks
    print(f"{'='*60}")
    print(f"Running {args.n_walks:,} random walks on parquet-loaded data ...")
    print(f"{'='*60}")
    t0 = time.perf_counter()
    results = run_walks(args.n_walks, customers, orders_idx, lineitems_idx)
    t_walk = time.perf_counter() - t0

    # Report
    n_success = len(results)
    n_dead = args.n_walks - n_success
    dead_pct = 100 * n_dead / args.n_walks if args.n_walks > 0 else 0

    print(f"\nCompleted in {t_walk:.2f}s")
    print(f"  Successful walks : {n_success:,}")
    print(f"  Dead ends        : {n_dead:,} ({dead_pct:.1f}%)")

    if results:
        total_weighted = sum(r["value"] * r["weight"] for r in results)
        total_weight = sum(r["weight"] for r in results)
        weighted_avg = total_weighted / total_weight

        print(f"\n  Raw weighted avg of extendedprice: ${weighted_avg:,.2f}")
        print(f"  (sum(v*w) / sum(w) over {n_success:,} samples)")
    else:
        print("\n  No successful walks — check that the data files exist and are non-empty.")


if __name__ == "__main__":
    main()
