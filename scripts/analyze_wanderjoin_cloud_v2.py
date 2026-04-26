#!/usr/bin/env python3
import argparse
import time
import json
import boto3
import concurrent.futures
from pathlib import Path
from botocore.config import Config
from collections import defaultdict

from horvitz_thompson_estimator_v2 import HorvitzThompsonEstimator
from visualizations import WanderJoinVisualizer

LAMBDA_NAME = 'ScatterWorker_v2'
REGION = 'ap-southeast-1'

retry_config = Config(
    region_name='ap-southeast-1',
    signature_version='v4',
    read_timeout=900,
    connect_timeout=900,
    retries={'max_attempts': 15, 'mode': 'adaptive'}
)
lambda_client = boto3.client('lambda', region_name=REGION, config=retry_config)


def trigger_fat_worker(worker_id):
    """Invokes a Fat Worker that runs until its circuit breaker trips."""
    t_invoke_start = time.perf_counter()
    try:
        response = lambda_client.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType='RequestResponse'
        )
        res = json.loads(response['Payload'].read())
        res['_invoke_latency'] = time.perf_counter() - t_invoke_start  # wall time for this worker
        return res
    except Exception as e:
        print(f"    [!] Worker {worker_id} failed: {e}")
        return {'_error': str(e), '_invoke_latency': time.perf_counter() - t_invoke_start}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--output-dir", default="reports_v2")
    parser.add_argument("--rel-error-threshold", type=float, default=0.01)
    parser.add_argument("--population-size", type=int, default=29999795)
    args = parser.parse_args()

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    estimator = HorvitzThompsonEstimator(population_size=args.population_size)
    visualizer = WanderJoinVisualizer()

    all_samples = []
    t_start = time.perf_counter()

    # ── STATS COUNTERS ──────────────────────────────────────────────
    stats = {
        'workers_succeeded': 0,
        'workers_failed': 0,
        'total_dead_ends': 0,       # from Lambda's dead_ends field
        'total_walk_attempts': 0,   # successful samples + dead ends
        'per_worker_samples': [],   # list of (worker_id, n_samples, latency)
        'convergence_log': [],      # (elapsed_s, n_samples, rel_error, estimate)
        't_convergence': None,      # wall time when ±1% was first hit
    }
    # ────────────────────────────────────────────────────────────────

    print(f"\n🚀 STARTING V2 ABLATION STUDY: STATE-HYDRATED WORKER ENGINE")
    print(f"Targeting {args.rel_error_threshold * 100}% Relative Error with {args.workers} workers...\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(trigger_fat_worker, i): i for i in range(args.workers)}

        while futures:
            done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)

            for future in done:
                worker_id = futures.pop(future)
                result = future.result()
                elapsed = time.perf_counter() - t_start

                if result and 'samples' in result and len(result['samples']) > 0:
                    batch = result['samples']
                    dead_ends = result.get('dead_ends', 0)
                    invoke_latency = result.get('_invoke_latency', 0)

                    estimator.add_samples(batch)
                    all_samples.extend(batch)

                    # ── UPDATE STATS ────────────────────────────────
                    stats['workers_succeeded'] += 1
                    stats['total_dead_ends'] += dead_ends
                    stats['total_walk_attempts'] += len(batch) + dead_ends
                    stats['per_worker_samples'].append((worker_id, len(batch), invoke_latency))
                    # ────────────────────────────────────────────────

                    visualizer.track_convergence(
                        samples=all_samples,
                        estimate_mean=estimator.estimate_mean(),
                        ci_lower=estimator.confidence_interval_95()[0],
                        ci_upper=estimator.confidence_interval_95()[1]
                    )

                    rel_err = estimator.relative_error()
                    stats['convergence_log'].append((elapsed, estimator.n_samples, rel_err, estimator.estimate_mean()))

                    # Record first time we hit ±1%
                    if rel_err is not None and rel_err <= args.rel_error_threshold and stats['t_convergence'] is None:
                        stats['t_convergence'] = elapsed

                    print(f"  ✅ [Worker {worker_id}] {len(batch):,} samples | "
                          f"dead_ends={dead_ends} | latency={invoke_latency:.1f}s | "
                          f"total={estimator.n_samples:,} | rel_err={rel_err:.4%}")

                else:
                    error_msg = result.get('errorMessage', result.get('_error', 'Unknown')) if result else "Connection Failed"
                    stats['workers_failed'] += 1
                    print(f"  ❌ [Worker {worker_id}] FAILED: {error_msg}")

                should_stop, reason = estimator.should_stop_sampling(args.rel_error_threshold)
                if not should_stop:
                    futures[executor.submit(trigger_fat_worker, worker_id)] = worker_id
                else:
                    print(f"\n🎯 TARGET REACHED: {reason}")
                    for f in futures:
                        f.cancel()
                    futures = {}
                    break

    t_total = time.perf_counter() - t_start

    # ── FINAL STATS REPORT ──────────────────────────────────────────
    walk_success_rate = (
        (stats['total_walk_attempts'] - stats['total_dead_ends']) / stats['total_walk_attempts']
        if stats['total_walk_attempts'] > 0 else 0
    )
    throughput = estimator.n_samples / t_total

    print(f"\n{'='*60}")
    print(f"  V2 EXPERIMENT SUMMARY")
    print(f"{'='*60}")
    print(f"  Total wall time          : {t_total:.2f}s")
    print(f"  Time to ±1% convergence  : {stats['t_convergence']:.2f}s" if stats['t_convergence'] else "  Convergence              : NOT REACHED")
    print(f"  Total successful samples : {estimator.n_samples:,}")
    print(f"  Total walk attempts      : {stats['total_walk_attempts']:,}")
    print(f"  Total dead ends          : {stats['total_dead_ends']:,}")
    print(f"  Walk success rate        : {walk_success_rate:.2%}")
    print(f"  Throughput               : {throughput:.1f} samples/sec")
    print(f"  Workers succeeded        : {stats['workers_succeeded']}")
    print(f"  Workers failed           : {stats['workers_failed']}")
    print(f"  Final point estimate     : ${estimator.estimate_mean():,.2f}")
    print(f"  Final relative error     : {estimator.relative_error():.4%}")
    ci = estimator.confidence_interval_95()
    print(f"  95% CI                   : [${ci[0]:,.2f}, ${ci[1]:,.2f}]")
    print(f"{'='*60}\n")

    # Save stats to JSON for the report
    stats_out = {
        'wall_time_s': round(t_total, 2),
        't_convergence_s': round(stats['t_convergence'], 2) if stats['t_convergence'] else None,
        'total_samples': estimator.n_samples,
        'total_walk_attempts': stats['total_walk_attempts'],
        'total_dead_ends': stats['total_dead_ends'],
        'walk_success_rate': round(walk_success_rate, 4),
        'throughput_samples_per_sec': round(throughput, 1),
        'workers_succeeded': stats['workers_succeeded'],
        'workers_failed': stats['workers_failed'],
        'point_estimate': round(estimator.estimate_mean(), 2),
        'relative_error': round(estimator.relative_error(), 6) if estimator.relative_error() else None,
        'ci_lower': round(ci[0], 2),
        'ci_upper': round(ci[1], 2),
        'convergence_log': stats['convergence_log'],
        'per_worker_samples': stats['per_worker_samples'],
    }
    stats_file = output_path / "v2_stats.json"
    with open(stats_file, 'w') as f:
        json.dump(stats_out, f, indent=2)
    print(f"  Stats saved to {stats_file}")
    # ────────────────────────────────────────────────────────────────

    values = [s["value"] for s in all_samples]
    weights = [s["weight"] for s in all_samples]
    visualizer.plot_value_distribution(values, output_file=str(output_path / "01_v2_values.png"))
    visualizer.plot_convergence(ground_truth=38239.96, output_file=str(output_path / "03_v2_convergence.png"))
    visualizer.plot_weight_distribution(weights, output_file=str(output_path / "02_v2_weights.png"))
    visualizer.plot_relative_error_decay(output_file=str(output_path / "04_v2_error_decay.png"))
    visualizer.plot_confidence_interval_width(output_file=str(output_path / "05_v2_ci_width.png"))


if __name__ == "__main__":
    main()
