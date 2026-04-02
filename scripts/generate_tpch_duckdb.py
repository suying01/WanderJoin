import argparse
import json
import time
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


def query_scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"Query returned no rows: {sql}")
    return int(row[0])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate TPC-H .tbl files using DuckDB tpch extension")
    parser.add_argument("--sf", type=int, default=1, help="TPC-H scale factor (default: 1)")
    parser.add_argument(
        "--out-dir",
        default="data/raw/sf1",
        help="Output directory for raw .tbl files (default: data/raw/sf1)",
    )
    parser.add_argument("--force", action="store_true", help="Allow overwrite if output dir exists")
    return parser.parse_args()


def ensure_dir(out_dir: Path, force: bool) -> None:
    if out_dir.exists() and any(out_dir.iterdir()) and not force:
        raise FileExistsError(f"Output directory {out_dir} is not empty. Use --force to overwrite.")
    out_dir.mkdir(parents=True, exist_ok=True)


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir).resolve()
    ensure_dir(out_dir, args.force)

    for existing in out_dir.glob("*.tbl"):
        existing.unlink()

    start = time.time()
    db_path = out_dir / f"tpch_sf{args.sf}.duckdb"
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    con.execute("INSTALL tpch;")
    con.execute("LOAD tpch;")
    con.execute(f"CALL dbgen(sf={args.sf});")

    row_counts = {}
    for table in TABLES:
        target = out_dir / f"{table}.tbl"
        con.execute(
            f"COPY {table} TO '{target.as_posix()}' (FORMAT CSV, DELIMITER '|', HEADER FALSE);"
        )
        row_counts[table] = query_scalar_int(con, f"SELECT COUNT(*) FROM {table};")

    version_row = con.execute("SELECT version();").fetchone()
    if version_row is None:
        raise RuntimeError("Failed to read DuckDB version")
    version = version_row[0]
    con.close()

    elapsed = round(time.time() - start, 2)

    metadata = {
        "generator": "duckdb-tpch",
        "duckdb_version": version,
        "scale_factor": args.sf,
        "tables": row_counts,
        "elapsed_seconds": elapsed,
        "output_dir": str(out_dir),
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    metadata_path = out_dir / "generation_metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as fp:
        json.dump(metadata, fp, indent=2)

    print(f"Generated TPC-H SF{args.sf} in {elapsed}s at {out_dir}")
    print(f"Metadata: {metadata_path}")


if __name__ == "__main__":
    main()
