#!/usr/bin/env python3
"""
Sanity-check script for the Wander Join random walk engine.

Loads TPC-H data, builds indexes, runs 10 000 walks, and prints a raw
weighted average of l_extendedprice so we can eyeball results before
the proper Horvitz-Thompson estimator is wired up.

Usage:
    python scripts/test_walk.py --data-dir data/clean/sf1
"""

import argparse
import time

from build_indexes import load_and_index
from wander_join import run_walks


def main():
    parser = argparse.ArgumentParser(description="Wander Join sanity check")
    parser.add_argument(
        "--data-dir",
        default="data/clean/sf1",
        help="Path to directory with customer.tbl, orders.tbl, lineitem.tbl",
    )
    parser.add_argument(
        "--n-walks",
        type=int,
        default=10_000,
        help="Number of random walks to run (default: 10000)",
    )
    args = parser.parse_args()

    # Build indexes
    print(f"\n{'='*60}")
    print("Building indexes ...")
    print(f"{'='*60}")
    t0 = time.perf_counter()
    customers, orders_idx, lineitems_idx = load_and_index(args.data_dir)
    t_index = time.perf_counter() - t0
    print(f"Index build time: {t_index:.2f}s\n")

    # Run walks
    print(f"{'='*60}")
    print(f"Running {args.n_walks:,} random walks ...")
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
