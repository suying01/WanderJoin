import argparse
import json
from pathlib import Path

import duckdb

TABLE_SCHEMAS = {
    "region": [
        ("r_regionkey", "INTEGER"),
        ("r_name", "VARCHAR"),
        ("r_comment", "VARCHAR"),
    ],
    "nation": [
        ("n_nationkey", "INTEGER"),
        ("n_name", "VARCHAR"),
        ("n_regionkey", "INTEGER"),
        ("n_comment", "VARCHAR"),
    ],
    "supplier": [
        ("s_suppkey", "INTEGER"),
        ("s_name", "VARCHAR"),
        ("s_address", "VARCHAR"),
        ("s_nationkey", "INTEGER"),
        ("s_phone", "VARCHAR"),
        ("s_acctbal", "DECIMAL(15,2)"),
        ("s_comment", "VARCHAR"),
    ],
    "customer": [
        ("c_custkey", "INTEGER"),
        ("c_name", "VARCHAR"),
        ("c_address", "VARCHAR"),
        ("c_nationkey", "INTEGER"),
        ("c_phone", "VARCHAR"),
        ("c_acctbal", "DECIMAL(15,2)"),
        ("c_mktsegment", "VARCHAR"),
        ("c_comment", "VARCHAR"),
    ],
    "part": [
        ("p_partkey", "INTEGER"),
        ("p_name", "VARCHAR"),
        ("p_mfgr", "VARCHAR"),
        ("p_brand", "VARCHAR"),
        ("p_type", "VARCHAR"),
        ("p_size", "INTEGER"),
        ("p_container", "VARCHAR"),
        ("p_retailprice", "DECIMAL(15,2)"),
        ("p_comment", "VARCHAR"),
    ],
    "partsupp": [
        ("ps_partkey", "INTEGER"),
        ("ps_suppkey", "INTEGER"),
        ("ps_availqty", "INTEGER"),
        ("ps_supplycost", "DECIMAL(15,2)"),
        ("ps_comment", "VARCHAR"),
    ],
    "orders": [
        ("o_orderkey", "INTEGER"),
        ("o_custkey", "INTEGER"),
        ("o_orderstatus", "VARCHAR"),
        ("o_totalprice", "DECIMAL(15,2)"),
        ("o_orderdate", "DATE"),
        ("o_orderpriority", "VARCHAR"),
        ("o_clerk", "VARCHAR"),
        ("o_shippriority", "INTEGER"),
        ("o_comment", "VARCHAR"),
    ],
    "lineitem": [
        ("l_orderkey", "INTEGER"),
        ("l_partkey", "INTEGER"),
        ("l_suppkey", "INTEGER"),
        ("l_linenumber", "INTEGER"),
        ("l_quantity", "DECIMAL(15,2)"),
        ("l_extendedprice", "DECIMAL(15,2)"),
        ("l_discount", "DECIMAL(15,2)"),
        ("l_tax", "DECIMAL(15,2)"),
        ("l_returnflag", "VARCHAR"),
        ("l_linestatus", "VARCHAR"),
        ("l_shipdate", "DATE"),
        ("l_commitdate", "DATE"),
        ("l_receiptdate", "DATE"),
        ("l_shipinstruct", "VARCHAR"),
        ("l_shipmode", "VARCHAR"),
        ("l_comment", "VARCHAR"),
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export cleaned TPC-H .tbl files to Parquet")
    parser.add_argument("--input-dir", required=True, help="Directory containing cleaned .tbl files")
    parser.add_argument("--output-dir", required=True, help="Directory to write Parquet files")
    parser.add_argument("--force", action="store_true", help="Overwrite existing Parquet outputs")
    return parser.parse_args()


def make_column_map(schema: list[tuple[str, str]]) -> str:
    pairs = [f"'{name}': '{dtype}'" for name, dtype in schema]
    return "{" + ", ".join(pairs) + "}"


def query_scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"Query returned no rows: {sql}")
    return int(row[0])


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    row_counts: dict[str, int] = {}

    for table, schema in TABLE_SCHEMAS.items():
        src = input_dir / f"{table}.tbl"
        dst = output_dir / f"{table}.parquet"

        if not src.exists():
            raise FileNotFoundError(f"Missing cleaned source file: {src}")
        if dst.exists() and not args.force:
            raise FileExistsError(f"Output exists: {dst}. Use --force to overwrite.")

        col_map = make_column_map(schema)
        con.execute(
            f"CREATE OR REPLACE VIEW v_{table} AS "
            f"SELECT * FROM read_csv('{src.as_posix()}', delim='|', header=False, columns={col_map});"
        )
        con.execute(f"COPY v_{table} TO '{dst.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);")
        row_counts[table] = query_scalar_int(con, f"SELECT COUNT(*) FROM v_{table};")

    con.close()

    report = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "tables": row_counts,
        "status": "pass",
    }
    report_path = output_dir / "parquet_export_report.json"
    with open(report_path, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2)

    print(f"Parquet export completed: {output_dir}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
