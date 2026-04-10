"""
Visualization Module for Wander Join Report
=============================================
Generates publication-quality charts for evaluating the accuracy and 
convergence of the Horvitz-Thompson estimator.

Includes:
- Convergence plot (point estimate ± confidence interval vs. sample count)
- Confidence interval width over time
- Value distribution (histogram of sampled l_extendedprice)
- Weight distribution (fanout multipliers)
- Relative error decay
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from typing import List, Dict, Tuple, Optional
from pathlib import Path


class WanderJoinVisualizer:
    """Generate publication-quality visualizations for Wander Join experiments."""

    def __init__(self, figsize: Tuple[int, int] = (12, 8)):
        """
        Parameters
        ----------
        figsize : tuple
            Default figure size for plots.
        """
        self.figsize = figsize
        self.sample_history: List[Dict] = []

    def track_convergence(self, samples: List[Dict], estimate_mean: float,
                         ci_lower: float, ci_upper: float) -> Dict:
        """
        Record a snapshot of estimate state for convergence tracking.

        Parameters
        ----------
        samples : list[dict]
            Accumulated samples so far.
        estimate_mean : float
            Current point estimate.
        ci_lower : float
            Lower bound of 95% CI.
        ci_upper : float
            Upper bound of 95% CI.

        Returns
        -------
        dict
            Snapshot with n_samples, estimate, ci bounds, margins, etc.
        """
        n = len(samples)
        se = ci_upper - estimate_mean  # margin of error
        rel_error = se / abs(estimate_mean) if estimate_mean != 0 else float('inf')

        snapshot = {
            "n_samples": n,
            "estimate": estimate_mean,
            "ci_lower": ci_lower,
            "ci_upper": ci_upper,
            "margin_of_error": se,
            "relative_error": rel_error,
        }

        self.sample_history.append(snapshot)
        return snapshot

    def plot_convergence(self, ground_truth: Optional[float] = None,
                        output_file: Optional[str] = None) -> None:
        """
        Plot point estimate ± 95% confidence interval vs. sample count.

        Shows how the estimate converges and confidence intervals shrink as more
        samples accumulate. Useful for evaluating accuracy-cost tradeoffs.

        Parameters
        ----------
        ground_truth : float, optional
            True population mean (from DB query) for reference line.
        output_file : str, optional
            If provided, save to this path instead of showing.
        """
        if not self.sample_history:
            print("⚠ No convergence history recorded. Call track_convergence() first.")
            return

        fig, ax = plt.subplots(figsize=self.figsize)

        samples = [h["n_samples"] for h in self.sample_history]
        estimates = [h["estimate"] for h in self.sample_history]
        lower = [h["ci_lower"] for h in self.sample_history]
        upper = [h["ci_upper"] for h in self.sample_history]

        # Plot 95% CI as shaded band
        ax.fill_between(samples, lower, upper, alpha=0.3, color="blue",
                        label="95% Confidence Interval")

        # Plot point estimate
        ax.plot(samples, estimates, "b-", linewidth=2, label="HT Estimate")

        # Plot ground truth reference (if provided)
        if ground_truth is not None:
            ax.axhline(ground_truth, color="green", linestyle="--", linewidth=2,
                       label="Ground Truth (Full Query)")

        ax.set_xlabel("Number of Samples", fontsize=12, fontweight="bold")
        ax.set_ylabel("Estimated Mean Value ($)", fontsize=12, fontweight="bold")
        ax.set_title("Wander Join Convergence: Horvitz-Thompson Estimate vs. Sample Count",
                    fontsize=13, fontweight="bold")
        ax.legend(loc="best", fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x/1000)}K"))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            print(f"✓ Convergence plot saved to: {output_file}")
        else:
            plt.show()

        plt.close()

    def plot_relative_error_decay(self, output_file: Optional[str] = None) -> None:
        """
        Plot relative error (confidence interval / estimate) vs. sample count.

        Used to demonstrate when the ±1% stopping condition is met.

        Parameters
        ----------
        output_file : str, optional
            If provided, save to this path.
        """
        if not self.sample_history:
            print("⚠ No convergence history recorded.")
            return

        fig, ax = plt.subplots(figsize=self.figsize)

        samples = [h["n_samples"] for h in self.sample_history]
        rel_errors = [h["relative_error"] * 100 for h in self.sample_history]  # convert to %

        ax.plot(samples, rel_errors, "r-", linewidth=2.5, marker="o", markersize=4)

        # Stop condition threshold (±1%)
        target_error = 1.0  # %
        ax.axhline(target_error, color="green", linestyle="--", linewidth=2,
                   label=f"Target (±{target_error}% relative error)")

        # Mark when criterion is first met
        for i, rel_err in enumerate(rel_errors):
            if rel_err < target_error:
                ax.axvline(samples[i], color="orange", linestyle=":", alpha=0.7,
                          label=f"Criterion met at {samples[i]:,} samples")
                break

        ax.set_xlabel("Number of Samples", fontsize=12, fontweight="bold")
        ax.set_ylabel("Relative Error (%)", fontsize=12, fontweight="bold")
        ax.set_title("Confidence Interval Relative Error Decay (95% CI)",
                    fontsize=13, fontweight="bold")
        ax.set_yscale("log")
        ax.legend(loc="best", fontsize=10)
        ax.grid(True, alpha=0.3, which="both")
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x/1000)}K"))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            print(f"✓ Relative error plot saved to: {output_file}")
        else:
            plt.show()

        plt.close()

    def plot_value_distribution(self, values: List[float],
                               output_file: Optional[str] = None) -> None:
        """
        Histogram of sampled extended prices (l_extendedprice).

        Helps understand the distribution of values being sampled by the random walk.

        Parameters
        ----------
        values : list[float]
            Sampled values from all walks.
        output_file : str, optional
            If provided, save to this path.
        """
        fig, ax = plt.subplots(figsize=self.figsize)

        ax.hist(values, bins=50, color="steelblue", alpha=0.7, edgecolor="black")

        ax.set_xlabel("Extended Price ($)", fontsize=12, fontweight="bold")
        ax.set_ylabel("Frequency", fontsize=12, fontweight="bold")
        ax.set_title("Distribution of Sampled Line Item Extended Prices",
                    fontsize=13, fontweight="bold")
        ax.grid(True, alpha=0.3, axis="y")

        # Add statistics
        mean_val = np.mean(values)
        median_val = np.median(values)
        std_val = np.std(values)

        stats_text = f"Mean: ${mean_val:,.2f}\nMedian: ${median_val:,.2f}\nStd Dev: ${std_val:,.2f}"
        ax.text(0.98, 0.97, stats_text, transform=ax.transAxes,
               fontsize=10, verticalalignment="top", horizontalalignment="right",
               bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            print(f"✓ Value distribution plot saved to: {output_file}")
        else:
            plt.show()

        plt.close()

    def plot_weight_distribution(self, weights: List[int],
                                output_file: Optional[str] = None) -> None:
        """
        Histogram of fanout weights (product of join multiplicities).

        Illustrates the variance in the importance of samples due to different
        join fanout at each step.

        Parameters
        ----------
        weights : list[int]
            Fanout weights from all samples.
        output_file : str, optional
            If provided, save to this path.
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

        # Linear scale
        ax1.hist(weights, bins=50, color="coral", alpha=0.7, edgecolor="black")
        ax1.set_xlabel("Fanout Weight", fontsize=11, fontweight="bold")
        ax1.set_ylabel("Frequency", fontsize=11, fontweight="bold")
        ax1.set_title("Weight Distribution (Linear Scale)", fontsize=12, fontweight="bold")
        ax1.grid(True, alpha=0.3, axis="y")

        # Log scale
        ax2.hist(weights, bins=50, color="coral", alpha=0.7, edgecolor="black")
        ax2.set_xlabel("Fanout Weight (log scale)", fontsize=11, fontweight="bold")
        ax2.set_ylabel("Frequency (log scale)", fontsize=11, fontweight="bold")
        ax2.set_title("Weight Distribution (Log Scale)", fontsize=12, fontweight="bold")
        ax2.set_xscale("log")
        ax2.set_yscale("log")
        ax2.grid(True, alpha=0.3, which="both")

        # Statistics
        stats_text = (
            f"Count: {len(weights):,}\n"
            f"Min: {np.min(weights):,}\n"
            f"Max: {np.max(weights):,}\n"
            f"Mean: {np.mean(weights):,.0f}\n"
            f"Median: {np.median(weights):,.0f}"
        )
        ax1.text(0.98, 0.97, stats_text, transform=ax1.transAxes,
                fontsize=9, verticalalignment="top", horizontalalignment="right",
                bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            print(f"✓ Weight distribution plot saved to: {output_file}")
        else:
            plt.show()

        plt.close()

    def plot_confidence_interval_width(self, output_file: Optional[str] = None) -> None:
        """
        Plot the width of 95% confidence interval vs. sample count.

        Shows how the interval shrinks as sample size increases, useful for
        understanding the accuracy-effort tradeoff.

        Parameters
        ----------
        output_file : str, optional
            If provided, save to this path.
        """
        if not self.sample_history:
            print("⚠ No convergence history recorded.")
            return

        fig, ax = plt.subplots(figsize=self.figsize)

        samples = [h["n_samples"] for h in self.sample_history]
        ci_widths = [h["ci_upper"] - h["ci_lower"] for h in self.sample_history]

        ax.plot(samples, ci_widths, "purple", linewidth=2.5, marker="s", markersize=4)

        ax.set_xlabel("Number of Samples", fontsize=12, fontweight="bold")
        ax.set_ylabel("95% Confidence Interval Width ($)", fontsize=12, fontweight="bold")
        ax.set_title("Confidence Interval Width vs. Sample Count",
                    fontsize=13, fontweight="bold")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x/1000)}K"))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            print(f"✓ CI width plot saved to: {output_file}")
        else:
            plt.show()

        plt.close()

    def plot_accuracy_summary(self, summary_text: str,
                             output_file: Optional[str] = None) -> None:
        """
        Create a summary report visualization showing key metrics.

        Parameters
        ----------
        summary_text : str
            Multi-line summary text to display.
        output_file : str, optional
            If provided, save to this path.
        """
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.axis("off")

        # Display summary text
        ax.text(0.5, 0.5, summary_text, transform=ax.transAxes,
               fontsize=11, verticalalignment="center", horizontalalignment="center",
               family="monospace",
               bbox=dict(boxstyle="round", facecolor="lightgray", alpha=0.9, pad=1))

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            print(f"✓ Summary report saved to: {output_file}")
        else:
            plt.show()

        plt.close()


def create_all_visualizations(samples: List[Dict], ground_truth: Optional[float] = None,
                             output_dir: str = "reports") -> None:
    """
    Generate all standard visualizations for a Wander Join experiment.

    Parameters
    ----------
    samples : list[dict]
        All accumulated samples with 'value' and 'weight' keys.
    ground_truth : float, optional
        True population mean for reference.
    output_dir : str
        Directory to save PNG outputs.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    viz = WanderJoinVisualizer()

    # Extract values and weights
    values = [s["value"] for s in samples]
    weights = [s["weight"] for s in samples]

    # Generate visualizations
    print(f"\n📊 Generating visualizations in {output_dir}/ ...")

    viz.plot_value_distribution(
        values,
        output_file=str(output_path / "01_value_distribution.png")
    )

    viz.plot_weight_distribution(
        weights,
        output_file=str(output_path / "02_weight_distribution.png")
    )

    print(f"✓ Visualizations complete")
