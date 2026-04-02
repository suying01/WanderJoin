import argparse
import json
from pathlib import Path

import duckdb

TABLES = [
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]

COLUMNS = {
    "region": ["r_regionkey", "r_name", "r_comment"],
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
    ],
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate cleaned TPC-H .tbl files")
    parser.add_argument(
        "--input-dir",
        default="data/clean/sf1",
        help="Clean data directory (default: data/clean/sf1)",
    )
    parser.add_argument(
        "--report",
        default="reports/validation_sf1.json",
        help="Validation report JSON output path (default: reports/validation_sf1.json)",
    )
    return parser.parse_args()


def create_views(con: duckdb.DuckDBPyConnection, base_dir: Path) -> None:
    for table in TABLES:
        col_names = ", ".join(COLUMNS[table])
        col_mapping = "{" + ", ".join([f"'{c}': 'VARCHAR'" for c in COLUMNS[table]]) + "}"
        file_path = (base_dir / f"{table}.tbl").as_posix()
        con.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT {col_names} FROM read_csv('{file_path}', delim='|', header=False, columns={col_mapping});"
        )


def query_scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"Query returned no rows: {sql}")
    return int(row[0])


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    report_path = Path(args.report).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    create_views(con, input_dir)

    row_counts = {}
    pk_checks = {}
    fk_checks = {}

    for table in TABLES:
        row_counts[table] = query_scalar_int(con, f"SELECT COUNT(*) FROM {table};")

    pk_checks["region"] = query_scalar_int(
        con, "SELECT COUNT(*) FROM (SELECT r_regionkey, COUNT(*) c FROM region GROUP BY 1 HAVING c > 1);"
    )
    pk_checks["nation"] = query_scalar_int(
        con, "SELECT COUNT(*) FROM (SELECT n_nationkey, COUNT(*) c FROM nation GROUP BY 1 HAVING c > 1);"
    )
    pk_checks["supplier"] = query_scalar_int(
        con, "SELECT COUNT(*) FROM (SELECT s_suppkey, COUNT(*) c FROM supplier GROUP BY 1 HAVING c > 1);"
    )
    pk_checks["customer"] = query_scalar_int(
        con, "SELECT COUNT(*) FROM (SELECT c_custkey, COUNT(*) c FROM customer GROUP BY 1 HAVING c > 1);"
    )
    pk_checks["part"] = query_scalar_int(
        con, "SELECT COUNT(*) FROM (SELECT p_partkey, COUNT(*) c FROM part GROUP BY 1 HAVING c > 1);"
    )
    pk_checks["orders"] = query_scalar_int(
        con, "SELECT COUNT(*) FROM (SELECT o_orderkey, COUNT(*) c FROM orders GROUP BY 1 HAVING c > 1);"
    )
    pk_checks["partsupp"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM (SELECT ps_partkey, ps_suppkey, COUNT(*) c FROM partsupp GROUP BY 1,2 HAVING c > 1);",
    )
    pk_checks["lineitem"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM (SELECT l_orderkey, l_linenumber, COUNT(*) c FROM lineitem GROUP BY 1,2 HAVING c > 1);",
    )

    fk_checks["nation_to_region_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM nation n LEFT JOIN region r ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey IS NULL;",
    )
    fk_checks["supplier_to_nation_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM supplier s LEFT JOIN nation n ON s.s_nationkey = n.n_nationkey WHERE n.n_nationkey IS NULL;",
    )
    fk_checks["customer_to_nation_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM customer c LEFT JOIN nation n ON c.c_nationkey = n.n_nationkey WHERE n.n_nationkey IS NULL;",
    )
    fk_checks["orders_to_customer_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM orders o LEFT JOIN customer c ON o.o_custkey = c.c_custkey WHERE c.c_custkey IS NULL;",
    )
    fk_checks["partsupp_part_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM partsupp ps LEFT JOIN part p ON ps.ps_partkey = p.p_partkey WHERE p.p_partkey IS NULL;",
    )
    fk_checks["partsupp_supplier_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM partsupp ps LEFT JOIN supplier s ON ps.ps_suppkey = s.s_suppkey WHERE s.s_suppkey IS NULL;",
    )
    fk_checks["lineitem_orders_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM lineitem l LEFT JOIN orders o ON l.l_orderkey = o.o_orderkey WHERE o.o_orderkey IS NULL;",
    )
    fk_checks["lineitem_part_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM lineitem l LEFT JOIN part p ON l.l_partkey = p.p_partkey WHERE p.p_partkey IS NULL;",
    )
    fk_checks["lineitem_supplier_missing"] = query_scalar_int(
        con,
        "SELECT COUNT(*) FROM lineitem l LEFT JOIN supplier s ON l.l_suppkey = s.s_suppkey WHERE s.s_suppkey IS NULL;",
    )

    con.close()

    report = {
        "input_dir": str(input_dir),
        "row_counts": row_counts,
        "pk_duplicate_groups": pk_checks,
        "fk_missing_rows": fk_checks,
        "status": "pass"
        if all(v == 0 for v in pk_checks.values()) and all(v == 0 for v in fk_checks.values())
        else "fail",
    }

    with open(report_path, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2)

    print(f"Validation status: {report['status']}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
