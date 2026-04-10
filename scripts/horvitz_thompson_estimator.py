"""
Horvitz-Thompson Estimator for Wander Join
============================================
Implements the Horvitz-Thompson (HT) ratio estimator to convert random walk samples
into unbiased aggregate estimates with confidence intervals.

Mathematical Foundation
-----------------------
Given samples from a random walk where each sample i has:
  - value: v_i (e.g., l_extendedprice)
  - weight: w_i (fanout product from join steps)

The Horvitz-Thompson ratio estimator computes:
  
  μ̂ = Σ(v_i * w_i) / Σ(w_i)

This is an unbiased estimator of the population mean. For aggregate estimates
(like total SUM), multiply by the population size N.

Confidence Intervals
--------------------
We use the delta method to approximate the variance of the ratio estimator:

  Var(μ̂) ≈ (1 / (Σw_i)²) * Σ w_i² * (v_i - μ̂)²

The 95% confidence interval is:
  
  [μ̂ - 1.96 * σ(μ̂), μ̂ + 1.96 * σ(μ̂)]

Stopping Condition
------------------
Continue sampling until the margin of error (MoE) falls within the desired threshold:
  
  MoE = 1.96 * σ(μ̂) < ε * μ̂
  
where ε is the relative error tolerance (e.g., 0.01 for ±1%).

For 95% confidence interval with ±1% margin of error:
  - Coefficient of Variation (CV) = σ(μ̂) / μ̂ must be < 0.01 / 1.96 ≈ 0.0051
"""

import math
from typing import List, Dict, Tuple, Optional


class HorvitzThompsonEstimator:
    """
    Accumulates Wander Join samples and provides:
    - Unbiased point estimate
    - Variance calculation
    - Confidence intervals
    - Stopping condition check
    """

    def __init__(self, population_size: int):
        """
        Parameters
        ----------
        population_size : int
            Total number of rows in the join result (for scaling aggregates).
            For TPC-H, this is the total number of (customer, order, lineitem) tuples.
        """
        self.population_size = population_size
        self.samples: List[Dict[str, float]] = []

    def add_samples(self, walk_results: List[Dict[str, float]]) -> None:
        """
        Add new samples from random walks.

        Parameters
        ----------
        walk_results : list[dict]
            Each element has {'value': float, 'weight': int}.
        """
        self.samples.extend(walk_results)

    @property
    def n_samples(self) -> int:
        """Total number of successful samples accumulated."""
        return len(self.samples)

    def estimate_mean(self) -> Optional[float]:
        """
        Compute the Horvitz-Thompson point estimate of the mean.

        Returns
        -------
        float or None
            Unbiased estimate of E[value], or None if no samples.
        """
        if not self.samples:
            return None

        total_weighted = sum(s["value"] * s["weight"] for s in self.samples)
        total_weight = sum(s["weight"] for s in self.samples)

        if total_weight == 0:
            return None

        return total_weighted / total_weight

    def estimate_total(self) -> Optional[float]:
        """
        Compute the total aggregate (sum of all join results).

        Returns
        -------
        float or None
            Estimated SUM(value) over entire join result.
        """
        mean = self.estimate_mean()
        if mean is None:
            return None
        return mean * self.population_size

    def variance(self) -> Optional[float]:
        """
        Compute the variance of the HT mean estimator using delta method.

        Var(μ̂) ≈ (1 / (Σw_i)²) * Σ w_i² * (v_i - μ̂)²

        Returns
        -------
        float or None
            Variance of the point estimate, or None if < 2 samples.
        """
        if len(self.samples) < 2:
            return None

        mu = self.estimate_mean()
        if mu is None:
            return None

        total_weight = sum(s["weight"] for s in self.samples)
        if total_weight == 0:
            return None

        # Compute weighted sum of squared deviations
        sum_w_sq_dev = sum(
            s["weight"] ** 2 * (s["value"] - mu) ** 2
            for s in self.samples
        )

        var = sum_w_sq_dev / (total_weight ** 2)
        return var

    def standard_error(self) -> Optional[float]:
        """Standard deviation of the mean estimator."""
        var = self.variance()
        if var is None or var < 0:
            return None
        return math.sqrt(var)

    def confidence_interval_95(self) -> Optional[Tuple[float, float, float]]:
        """
        Compute 95% confidence interval [lower, upper] and margin of error.

        Returns
        -------
        tuple or None
            (lower_bound, upper_bound, margin_of_error) or None if insufficient samples.
        """
        mu = self.estimate_mean()
        se = self.standard_error()

        if mu is None or se is None:
            return None

        z_critical = 1.96  # 95% confidence
        moe = z_critical * se
        lower = mu - moe
        upper = mu + moe

        return (lower, upper, moe)

    def coefficient_of_variation(self) -> Optional[float]:
        """Relative standard error: σ(μ̂) / μ̂."""
        mu = self.estimate_mean()
        se = self.standard_error()

        if mu is None or se is None or mu == 0:
            return None

        return se / abs(mu)

    def should_stop_sampling(self, rel_error_threshold: float = 0.01) -> Tuple[bool, str]:
        """
        Check if we should stop sampling based on 95% CI with relative error.

        For a 95% confidence interval with ±ε% margin of error:
          Margin of error = 1.96 * σ(μ̂) < ε * μ̂

        Parameters
        ----------
        rel_error_threshold : float
            Target relative error (e.g., 0.01 for ±1%).

        Returns
        -------
        tuple[bool, str]
            (should_stop, reason_message)
        """
        mu = self.estimate_mean()
        se = self.standard_error()

        if mu is None or se is None:
            return False, "Insufficient samples (need ≥2)"

        if mu == 0:
            return False, "Mean estimate is zero (cannot compute relative error)"

        z_critical = 1.96
        moe = z_critical * se
        rel_error = moe / abs(mu)

        should_stop = rel_error < rel_error_threshold
        status = (
            f"Relative Error: {rel_error:.4f} (target: {rel_error_threshold:.4f}) — "
            f"{'✓ STOP' if should_stop else '✗ CONTINUE'}"
        )

        return should_stop, status

    def summary(self) -> Dict:
        """
        Return a comprehensive summary of the estimate and accuracy.

        Returns
        -------
        dict
            Contains: n_samples, estimate_mean, estimate_total, ci_95, 
            relative_error, coefficient_of_variation, stop_sampling, etc.
        """
        mu = self.estimate_mean()
        total = self.estimate_total()
        ci = self.confidence_interval_95()
        cv = self.coefficient_of_variation()
        should_stop, stop_reason = self.should_stop_sampling()

        summary = {
            "n_samples": self.n_samples,
            "estimate_mean": mu,
            "estimate_total": total,
            "standard_error": self.standard_error(),
            "confidence_interval_95": ci,
            "margin_of_error": ci[2] if ci else None,
            "coefficient_of_variation": cv,
            "should_stop_sampling": should_stop,
            "stopping_condition_reason": stop_reason,
        }

        return summary

    def print_report(self) -> None:
        """Pretty-print the estimation report."""
        summary = self.summary()

        print(f"\n{'='*70}")
        print(f"HORVITZ-THOMPSON ESTIMATOR REPORT")
        print(f"{'='*70}")
        print(f"\nSamples collected: {summary['n_samples']:,}")

        if summary["estimate_mean"] is not None:
            print(f"\nPoint Estimate:")
            print(f"  Mean value per row:    ${summary['estimate_mean']:,.2f}")
            print(f"  Estimated total (SUM): ${summary['estimate_total']:,.2f}")

            print(f"\nAccuracy Metrics:")
            print(f"  Standard error:        ${summary['standard_error']:,.2f}")
            se = summary["standard_error"]
            print(f"  95% Confidence interval: "
                  f"[${summary['confidence_interval_95'][0]:,.2f}, "
                  f"${summary['confidence_interval_95'][1]:,.2f}]")
            print(f"  Margin of error (±):   ${summary['margin_of_error']:,.2f}")

            print(f"\nStopping Condition (95% CI, ±1% rel. error):")
            print(f"  {summary['stopping_condition_reason']}")

            if summary["should_stop_sampling"]:
                print(f"\n✓ STOPPING CRITERION MET — sufficient accuracy achieved")
            else:
                remaining_samples = max(1, int(summary['coefficient_of_variation']**2 / 0.0051**2))
                print(f"\n✗ Need more samples (~{remaining_samples:,} additional samples recommended)")

        else:
            print(f"\n⚠ Insufficient data for estimate")

        print(f"\n{'='*70}\n")
