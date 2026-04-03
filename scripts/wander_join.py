"""
Wander Join — Random Walk Engine
=================================
Implements the core random-walk sampling from the Wander Join algorithm
(Li et al., SIGMOD 2016) over the join path:

    Customer  →  Orders  →  LineItem

Public API
----------
- ``run_walks(n_walks, customers, orders_idx, lineitems_idx)``
  Returns a list of ``{'value': float, 'weight': int}`` dicts.

The *weight* field is the **fanout product** at each join step and is
essential for the Horvitz-Thompson estimator that turns these samples into
an unbiased aggregate estimate.
"""

import random


def single_walk(customers, orders_by_custkey, lineitems_by_orderkey):
    """Execute one random walk: Customer → Order → LineItem.

    Sampling procedure
    ------------------
    1. Pick a customer uniformly at random from *customers*.
    2. Look up all orders for that customer. If none exist the walk is a
       **dead end** — return ``None``.
    3. Pick one order uniformly at random.
    4. Look up all line items for that order. If none → dead end.
    5. Pick one line item uniformly at random.

    Fanout weight
    -------------
    At each join step we pick one row out of *k* candidates. The probability
    of reaching this particular (customer, order, lineitem) tuple is:

        P = (1 / N_customers) × (1 / |orders|) × (1 / |lineitems|)

    The inverse of the per-walk sampling probability (ignoring the constant
    1/N_customers which cancels out in the ratio estimator) is:

        weight = |orders_for_customer| × |lineitems_for_order|

    This is the multiplier the Horvitz-Thompson estimator needs so that each
    sample contributes proportionally to how many join-result rows it
    "represents".

    Returns
    -------
    dict or None
        ``{'value': float, 'weight': int}`` on success, ``None`` on dead end.
    """
    # Step 1 — pick a random customer
    cust = random.choice(customers)
    custkey = cust["c_custkey"]

    # Step 2 — look up orders for this customer
    orders = orders_by_custkey.get(custkey)
    if not orders:
        return None  # dead end: customer has no orders

    # Step 3 — pick a random order
    order = random.choice(orders)
    orderkey = order["o_orderkey"]

    # Step 4 — look up line items for this order
    lineitems = lineitems_by_orderkey.get(orderkey)
    if not lineitems:
        return None  # dead end: order has no line items

    # Step 5 — pick a random line item
    lineitem = random.choice(lineitems)

    # Fanout weight = product of choices at each join step
    weight = len(orders) * len(lineitems)

    return {"value": lineitem["l_extendedprice"], "weight": weight}


def run_walks(n_walks, customers, orders_idx, lineitems_idx):
    """Run *n_walks* independent random walks, discarding dead ends.

    This is the main entry point that other modules (multiprocessing wrapper,
    AWS Lambda handler, Horvitz-Thompson estimator) should call.

    Parameters
    ----------
    n_walks : int
        Number of random walks to attempt.
    customers : list[dict]
        Customer records (each must have ``'c_custkey'``).
    orders_idx : defaultdict(list)
        Maps custkey → list of order dicts.
    lineitems_idx : defaultdict(list)
        Maps orderkey → list of lineitem dicts.

    Returns
    -------
    list[dict]
        Each element is ``{'value': float, 'weight': int}``.
        Length ≤ *n_walks* (dead ends are excluded).
    """
    results = []
    for _ in range(n_walks):
        outcome = single_walk(customers, orders_idx, lineitems_idx)
        if outcome is not None:
            results.append(outcome)
    return results
