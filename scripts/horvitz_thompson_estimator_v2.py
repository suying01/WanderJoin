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
    Accumulates Wander Join samples and provides unbiased point estimates,
    variance, and confidence interval metrics.
    """

    def __init__(self, population_size: int):
        self.population_size = population_size
        self.samples: List[Dict[str, float]] = []

    def add_samples(self, walk_results: List[Dict[str, float]]) -> None:
        self.samples.extend(walk_results)

    @property
    def n_samples(self) -> int:
        return len(self.samples)

    def estimate_mean(self) -> Optional[float]:
        if not self.samples: return None
        total_weighted = sum(s["value"] * s["weight"] for s in self.samples)
        total_weight = sum(s["weight"] for s in self.samples)
        return total_weighted / total_weight if total_weight != 0 else None

    def estimate_total(self) -> Optional[float]:
        mean = self.estimate_mean()
        return mean * self.population_size if mean is not None else None

    def variance(self) -> Optional[float]:
        if len(self.samples) < 2: return None
        mu = self.estimate_mean()
        total_weight = sum(s["weight"] for s in self.samples)
        if mu is None or total_weight == 0: return None

        sum_w_sq_dev = sum(
            s["weight"] ** 2 * (s["value"] - mu) ** 2
            for s in self.samples
        )
        return sum_w_sq_dev / (total_weight ** 2)

    def standard_error(self) -> Optional[float]:
        var = self.variance()
        return math.sqrt(var) if var is not None and var >= 0 else None

    def relative_error(self) -> Optional[float]:
        """
        Calculates the margin of error at 95% confidence relative to the mean.
        Used by the orchestrator to check the 1% target threshold.
        """
        mu = self.estimate_mean()
        se = self.standard_error()
        if mu is None or se is None or mu == 0: return None
        return (1.96 * se) / abs(mu)

    def confidence_interval_95(self) -> Optional[Tuple[float, float, float]]:
        mu = self.estimate_mean()
        se = self.standard_error()
        if mu is None or se is None: return None
        moe = 1.96 * se
        return (mu - moe, mu + moe, moe)

    def should_stop_sampling(self, rel_error_threshold: float = 0.01) -> Tuple[bool, str]:
        rel_err = self.relative_error()
        if rel_err is None:
            return False, "Insufficient samples"

        should_stop = rel_err < rel_error_threshold
        status = (
            f"Relative Error: {rel_err:.4%}"
            f" (target: {rel_error_threshold:.2%}) — "
            f"{'✓ STOP' if should_stop else '✗ CONTINUE'}"
        )
        return should_stop, status

    def summary(self) -> Dict:
        ci = self.confidence_interval_95()
        return {
            "n_samples": self.n_samples,
            "estimate_mean": self.estimate_mean(),
            "estimate_total": self.estimate_total(),
            "relative_error": self.relative_error(),
            "confidence_interval_95": ci,
            "margin_of_error": ci[2] if ci else None,
        }