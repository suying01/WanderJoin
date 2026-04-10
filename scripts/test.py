from build_indexes import load_and_index
from wander_join import run_walks
from horvitz_thompson_estimator import HorvitzThompsonEstimator

# 1. Load data
customers, orders_idx, lineitems_idx = load_and_index("data/clean/sf1")

# 2. Estimate population size
population_size = len(customers) * 10 * 4  # rough estimate

# 3. Create estimator
estimator = HorvitzThompsonEstimator(population_size=population_size)

# 4. Run walks in a loop
for batch in range(10):
    results = run_walks(5000, customers, orders_idx, lineitems_idx)
    estimator.add_samples(results)
    
    # Check if we've reached target accuracy
    should_stop, reason = estimator.should_stop_sampling(rel_error_threshold=0.01)
    print(f"Batch {batch}: {reason}")
    
    if should_stop:
        break

# 5. Get results
print(f"Estimate: ${estimator.estimate_mean():,.2f}")
print(f"95% CI: {estimator.confidence_interval_95()}")
estimator.print_report()