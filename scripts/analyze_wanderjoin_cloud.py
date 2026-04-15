#!/usr/bin/env python3
"""
Serverless Wander Join Analysis
==============================================================
Demonstrates end-to-end cloud workflow:
1. Orchestrate AWS Lambda workers using boto3
2. Run random walks across S3 data lake in batches
3. Apply Horvitz-Thompson estimator with stopping condition
4. Generate publication-quality visualizations

Usage:
    python analyze_wanderjoin_cloud.py --workers 50 --walks-per-worker 100 --output-dir reports
"""

import argparse
import time
import json
import boto3
import concurrent.futures
from pathlib import Path
from botocore.config import Config

# Import the Math Lead's specific math and visualization classes
from horvitz_thompson_estimator import HorvitzThompsonEstimator
from visualizations import WanderJoinVisualizer

# --- AWS CLOUD CONFIGURATION ---
LAMBDA_NAME = 'ScatterWorker'
REGION = 'ap-southeast-1'

# Adaptive retries to bypass the AWS API Rate Limits
retry_config = Config(retries={'max_attempts': 15, 'mode': 'adaptive'})
lambda_client = boto3.client('lambda', region_name=REGION, config=retry_config)


def trigger_worker(worker_id: int, num_walks: int) -> list:
    """Invokes a single AWS Lambda function and returns its list of walks."""
    payload = {
        "worker_id": f"worker-{worker_id}", 
        "num_walks": num_walks
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        response_payload = json.loads(response['Payload'].read())
        body = json.loads(response_payload['body'])
        return body.get('results', [])
        
    except Exception as e:
        print(f"    [!] Worker {worker_id} failed: {e}")
        return []


def run_cloud_batch(num_workers: int, walks_per_worker: int) -> list:
    """Scatters the workload across N serverless workers concurrently."""
    all_walk_results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            futures.append(executor.submit(trigger_worker, i, walks_per_worker))
            # Stagger launches to respect AWS API Gateways
            time.sleep(0.2) 
            
        for future in concurrent.futures.as_completed(futures):
            all_walk_results.extend(future.result())
            
    return all_walk_results


def main():
    parser = argparse.ArgumentParser(description="Serverless Wander Join HT Analysis")
    parser.add_argument("--workers", type=int, default=50, help="Number of concurrent Lambda workers per batch")
    parser.add_argument("--walks-per-worker", type=int, default=100, help="Number of walks each Lambda performs")
    parser.add_argument("--max-batches", type=int, default=20, help="Maximum number of batches to run")
    parser.add_argument("--output-dir", default="reports", help="Directory to save visualizations")
    parser.add_argument("--rel-error-threshold", type=float, default=0.01, help="Target relative error (0.01 = 1%)")
    
    # For TPC-H SF5, the join size of Customer->Orders->Lineitem is exactly the size of the lineitem table (~30M)
    parser.add_argument("--population-size", type=int, default=29999795, help="Total rows in the exact DB join")
    
    args = parser.parse_args()

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(" SERVERLESS WANDER JOIN: HORVITZ-THOMPSON ANALYSIS")
    print(f"{'='*70}\n")

    # === STEP 1: Initialize Estimator ===
    print(f"Initializing Horvitz-Thompson Estimator...")
    print(f"Assumed Population Size (SF5): {args.population_size:,} tuples\n")
    
    estimator = HorvitzThompsonEstimator(population_size=args.population_size)
    visualizer = WanderJoinVisualizer()

    all_samples = []
    batch_number = 0
    t_start_walks = time.perf_counter()

    # === STEP 2: Run Cloud Batches with Stopping Condition ===
    print(f"{'='*70}")
    print("STEP 2: Scattering workers to AWS with adaptive stopping...")
    print(f"{'='*70}\n")

    while batch_number < args.max_batches:
        batch_number += 1
        total_requested_walks = args.workers * args.walks_per_worker
        
        print(f"\n--- Batch {batch_number} ---")
        print(f"Scattering {args.workers} workers ({total_requested_walks:,} total requested walks)...")

        t_batch = time.perf_counter()
        
        # ---> THE CLOUD TRIGGER <---
        batch_results = run_cloud_batch(args.workers, args.walks_per_worker)
        
        t_batch = time.perf_counter() - t_batch

        # Add successful cloud walks to the Math Lead's estimator
        estimator.add_samples(batch_results)
        all_samples.extend(batch_results)
        
        # Track for the convergence visualization
        if estimator.n_samples >= 2:
            visualizer.track_convergence(
                samples=all_samples,
                estimate_mean=estimator.estimate_mean(),
                ci_lower=estimator.confidence_interval_95()[0] if estimator.confidence_interval_95() else 0,
                ci_upper=estimator.confidence_interval_95()[1] if estimator.confidence_interval_95() else 0
            )

        n_succ = len(batch_results)
        n_fail = total_requested_walks - n_succ
        success_rate = 100 * n_succ / total_requested_walks if total_requested_walks > 0 else 0

        print(f"  ✓ {n_succ:,} successful walks, {n_fail:,} dead ends ({success_rate:.1f}% success)")
        print(f"  Time: {t_batch:.2f}s, Cloud Processing Rate: {n_succ / t_batch:.0f} walks/sec")

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

    # === STEP 3: Final Report ===
    print(f"\n{'='*70}")
    print("STEP 3: Final Results")
    print(f"{'='*70}")

    print(f"\nExecution Summary:")
    print(f"  Total batches run: {batch_number}")
    print(f"  Total walks: {estimator.n_samples:,} samples")
    print(f"  Total cloud execution time: {t_walks:.2f}s ({estimator.n_samples / t_walks:.0f} samples/sec)")

    estimator.print_report()

    # === STEP 4: Generate Visualizations ===
    print(f"\n{'='*70}")
    print("STEP 4: Generating Visualizations...")
    print(f"{'='*70}\n")

    values = [s["value"] for s in all_samples]
    weights = [s["weight"] for s in all_samples]

    visualizer.plot_value_distribution(values, output_file=str(output_path / "01_value_distribution.png"))
    visualizer.plot_weight_distribution(weights, output_file=str(output_path / "02_weight_distribution.png"))
    
    if len(visualizer.sample_history) > 1:
        visualizer.plot_convergence(
            ground_truth=38250.0, # Approximate SF5 true mean for reference
            output_file=str(output_path / "03_convergence.png")
        )
        visualizer.plot_relative_error_decay(output_file=str(output_path / "04_error_decay.png"))
        visualizer.plot_confidence_interval_width(output_file=str(output_path / "05_ci_width.png"))

    # === Save Summary ===
    summary = estimator.summary()
    summary_file = output_path / "estimate_summary.txt"
    
    with open(summary_file, "w") as f:
        f.write("SERVERLESS WANDER JOIN - HORVITZ-THOMPSON ESTIMATOR SUMMARY\n")
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
    print(" PIPELINE COMPLETE")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
