"""
Index Builder for Wander Join
=============================
Reads TPC-H .tbl files (customer, orders, lineitem) and builds in-memory
lookup dictionaries for fast random-walk traversal.

Usage:
    from build_indexes import load_and_index
    customers, orders_by_custkey, lineitems_by_orderkey = load_and_index("data/clean/sf1")
"""

from collections import defaultdict
from pathlib import Path

import pandas as pd

# Column definitions for the three tables we need (matches validate_tpch.py)
CUSTOMER_COLS = [
    "c_custkey", "c_name", "c_address", "c_nationkey",
    "c_phone", "c_acctbal", "c_mktsegment", "c_comment",
]
ORDERS_COLS = [
    "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
    "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment",
]
LINEITEM_COLS = [
    "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
    "l_quantity", "l_extendedprice", "l_discount", "l_tax",
    "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
    "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment",
]


def _read_tbl(path, columns, usecols):
    """Read a pipe-delimited .tbl file with no header, returning only *usecols*."""
    df = pd.read_csv(
        path,
        sep="|",
        header=None,
        names=columns,
        usecols=usecols,
    )
    return df


def load_and_index(data_dir):
    """Load TPC-H tables and build in-memory indexes for random walks.

    Parameters
    ----------
    data_dir : str or Path
        Directory containing customer.tbl, orders.tbl, and lineitem.tbl.

    Returns
    -------
    customers : list[dict]
        Each dict has key ``'c_custkey'`` (int). Used for uniform random sampling.
    orders_by_custkey : defaultdict(list)
        Maps ``int(custkey)`` -> list of order dicts (keys: ``o_orderkey``).
    lineitems_by_orderkey : defaultdict(list)
        Maps ``int(orderkey)`` -> list of lineitem dicts (keys: ``l_extendedprice``).
    """
    data_dir = Path(data_dir)

    # --- Customers (only need custkey for sampling) ---
    print("Loading customer.tbl ...")
    cust_df = _read_tbl(data_dir / "customer.tbl", CUSTOMER_COLS, usecols=["c_custkey"])
    cust_df["c_custkey"] = cust_df["c_custkey"].astype(int)
    customers = cust_df.to_dict(orient="records")
    print(f"  {len(customers):,} customers loaded")

    # --- Orders (need orderkey + custkey for lookup) ---
    print("Loading orders.tbl ...")
    ord_df = _read_tbl(data_dir / "orders.tbl", ORDERS_COLS, usecols=["o_orderkey", "o_custkey"])
    ord_df["o_orderkey"] = ord_df["o_orderkey"].astype(int)
    ord_df["o_custkey"] = ord_df["o_custkey"].astype(int)

    orders_by_custkey = defaultdict(list)
    for row in ord_df.itertuples(index=False):
        orders_by_custkey[row.o_custkey].append({"o_orderkey": row.o_orderkey})
    print(f"  {len(ord_df):,} orders loaded, {len(orders_by_custkey):,} distinct customers with orders")

    # --- Lineitems (need orderkey + extendedprice) ---
    print("Loading lineitem.tbl ...")
    li_df = _read_tbl(data_dir / "lineitem.tbl", LINEITEM_COLS, usecols=["l_orderkey", "l_extendedprice"])
    li_df["l_orderkey"] = li_df["l_orderkey"].astype(int)
    li_df["l_extendedprice"] = li_df["l_extendedprice"].astype(float)

    lineitems_by_orderkey = defaultdict(list)
    for row in li_df.itertuples(index=False):
        lineitems_by_orderkey[row.l_orderkey].append({"l_extendedprice": row.l_extendedprice})
    print(f"  {len(li_df):,} lineitems loaded, {len(lineitems_by_orderkey):,} distinct orders with lineitems")

    return customers, orders_by_custkey, lineitems_by_orderkey
