#!/usr/bin/env python3
"""
Complete Wander Join Analysis with Horvitz-Thompson Estimator
==============================================================
Demonstrates end-to-end workflow:
1. Load data and build indexes
2. Run random walks in batches
3. Apply Horvitz-Thompson estimator with stopping condition
4. Generate visualizations

Usage:
    python scripts/analyze_wanderjoin.py --data-dir data/clean/sf1 --batch-size 1000 --output-dir reports
"""

import argparse
import time
from pathlib import Path

from build_indexes import load_and_index
from wander_join import run_walks
from horvitz_thompson_estimator import HorvitzThompsonEstimator
from visualizations import WanderJoinVisualizer


def estimate_population_size(customers, orders_idx, lineitems_idx) -> int:
    """
    Quick estimate of total rows in Customer->Orders->LineItem join.
    
    A more precise calculation would come from the actual database.
    For now, we use the product of average fanouts.
    """
    avg_orders_per_cust = sum(len(orders) for orders in orders_idx.values()) / len(customers)
    avg_items_per_order = sum(len(items) for items in lineitems_idx.values()) / len(orders_idx)
    
    population_size = int(len(customers) * avg_orders_per_cust * avg_items_per_order)
    return population_size


def main():
    parser = argparse.ArgumentParser(
        description="Wander Join Analysis with Horvitz-Thompson Estimator"
    )
    parser.add_argument(
        "--data-dir",
        default="data/clean/sf1",
        help="Path to cleaned TPC-H data directory",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of walks per batch (default: 1000)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=20,
        help="Maximum number of batches to run (default: 20)",
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Directory to save visualizations",
    )
    parser.add_argument(
        "--rel-error-threshold",
        type=float,
        default=0.01,
        help="Target relative error for stopping condition (default: 0.01 = ±1%%)",
    )
    args = parser.parse_args()

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(" WANDER JOIN: HORVITZ-THOMPSON ANALYSIS")
    print(f"{'='*70}\n")

    # === STEP 1: Load and Index ===
    print(f"{'='*70}")
    print("STEP 1: Loading data and building indexes...")
    print(f"{'='*70}")
    t0 = time.perf_counter()
    customers, orders_idx, lineitems_idx = load_and_index(args.data_dir)
    t_load = time.perf_counter() - t0
    print(f"✓ Index build time: {t_load:.2f}s\n")

    population_size = estimate_population_size(customers, orders_idx, lineitems_idx)
    print(f"  Estimated population size: {population_size:,}")
    print(f"  (Customer→Order→LineItem join result rows)\n")

    # === STEP 2: Initialize Estimator ===
    print(f"{'='*70}")
    print("STEP 2: Initializing Horvitz-Thompson Estimator...")
    print(f"{'='*70}\n")

    estimator = HorvitzThompsonEstimator(population_size=population_size)
    visualizer = WanderJoinVisualizer()

    all_samples = []
    batch_number = 0

    # === STEP 3: Run Batches with Stopping Condition ===
    print(f"{'='*70}")
    print("STEP 3: Running random walks with adaptive stopping...")
    print(f"{'='*70}\n")

    t_start_walks = time.perf_counter()

    while batch_number < args.max_batches:
        batch_number += 1
        print(f"\n--- Batch {batch_number} ---")
        print(f"Running {args.batch_size:,} walks...")

        t_batch = time.perf_counter()
        batch_results = run_walks(
            args.batch_size,
            customers,
            orders_idx,
            lineitems_idx,
        )
        t_batch = time.perf_counter() - t_batch

        # Add to estimator
        estimator.add_samples(batch_results)
        all_samples.extend(batch_results)

        n_succ = len(batch_results)
        n_fail = args.batch_size - n_succ
        success_rate = 100 * n_succ / args.batch_size if args.batch_size > 0 else 0

        print(f"  ✓ {n_succ:,} successful walks, {n_fail:,} dead ends ({success_rate:.1f}% success)")
        print(f"  Time: {t_batch:.2f}s, Rate: {n_succ / t_batch:.0f} walks/sec")

        # === Check Stopping Condition ===
        mu = estimator.estimate_mean()
        if mu is not None and estimator.n_samples >= 10:
            ci = estimator.confidence_interval_95()
            cv = estimator.coefficient_of_variation()

            print(f"\n  Current estimate (@{estimator.n_samples:,} samples):")
            print(f"    Mean: ${mu:,.2f}")
            if ci:
                print(f"    95% CI: [${ci[0]:,.2f}, ${ci[1]:,.2f}]")
                print(f"    Margin of error: ±${ci[2]:,.2f}")

            if cv is not None:
                print(f"    Relative error: {cv*100:.3f}%")

            should_stop, reason = estimator.should_stop_sampling(args.rel_error_threshold)
            print(f"\n  Stopping criterion: {reason}")

            if should_stop:
                print(f"\n  ✓✓✓ TARGET ACCURACY ACHIEVED ✓✓✓")
                break
        else:
            print(f"  (Need ≥10 samples before checking stopping condition)")

    t_walks = time.perf_counter() - t_start_walks

    # === STEP 4: Final Report ===
    print(f"\n{'='*70}")
    print("STEP 4: Final Results")
    print(f"{'='*70}")

    print(f"\nExecution Summary:")
    print(f"  Total batches run: {batch_number}")
    print(f"  Total walks: {estimator.n_samples:,} samples")
    print(f"  Walk execution time: {t_walks:.2f}s ({estimator.n_samples / t_walks:.0f} samples/sec)")
    print(f"  Total time (including data load): {t_load + t_walks:.2f}s")

    estimator.print_report()

    # === STEP 5: Generate Visualizations ===
    print(f"\n{'='*70}")
    print("STEP 5: Generating Visualizations...")
    print(f"{'='*70}\n")

    values = [s["value"] for s in all_samples]
    weights = [s["weight"] for s in all_samples]

    visualizer.plot_value_distribution(
        values,
        output_file=str(output_path / "01_value_distribution.png")
    )

    visualizer.plot_weight_distribution(
        weights,
        output_file=str(output_path / "02_weight_distribution.png")
    )

    # === Save Summary ===
    summary = estimator.summary()
    summary_file = output_path / "estimate_summary.txt"
    
    with open(summary_file, "w") as f:
        f.write("WANDER JOIN - HORVITZ-THOMPSON ESTIMATOR SUMMARY\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Samples collected: {summary['n_samples']:,}\n\n")
        
        if summary['estimate_mean'] is not None:
            f.write(f"Point Estimates:\n")
            f.write(f"  Mean per row:        ${summary['estimate_mean']:,.2f}\n")
            f.write(f"  Total (estimated):   ${summary['estimate_total']:,.2f}\n\n")
            
            f.write(f"Accuracy Metrics:\n")
            f.write(f"  Standard error:      ${summary['standard_error']:,.2f}\n")
            f.write(f"  95% Confidence interval: [${summary['confidence_interval_95'][0]:,.2f}, "
                   f"${summary['confidence_interval_95'][1]:,.2f}]\n")
            f.write(f"  Margin of error (±): ${summary['margin_of_error']:,.2f}\n")
            f.write(f"  Relative error:      {summary['coefficient_of_variation']*100:.3f}%\n\n")
            
            f.write(f"Stopping Condition (95% CI, ±{args.rel_error_threshold*100:.0f}% rel. error):\n")
            f.write(f"  {summary['stopping_condition_reason']}\n")
            f.write(f"  Status: {'✓ MET' if summary['should_stop_sampling'] else '✗ NOT MET'}\n")

    print(f"✓ Summary saved to: {summary_file}")

    print(f"\n{'='*70}")
    print(" ANALYSIS COMPLETE")
    print(f"{'='*70}\n")
    print(f"Reports saved to: {output_path}/")


if __name__ == "__main__":
    main()
