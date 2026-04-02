import argparse
import csv
import json
from pathlib import Path

EXPECTED_COLUMNS = {
    "region": 3,
    "nation": 4,
    "supplier": 7,
    "customer": 8,
    "part": 9,
    "partsupp": 5,
    "orders": 9,
    "lineitem": 16,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean raw TPC-H .tbl files")
    parser.add_argument("--input-dir", required=True, help="Raw input directory")
    parser.add_argument("--output-dir", required=True, help="Cleaned output directory")
    parser.add_argument("--force", action="store_true", help="Overwrite outputs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "tables": {},
    }

    for table, expected_cols in EXPECTED_COLUMNS.items():
        src = input_dir / f"{table}.tbl"
        dst = output_dir / f"{table}.tbl"

        if not src.exists():
            raise FileNotFoundError(f"Missing source file: {src}")

        if dst.exists() and not args.force:
            raise FileExistsError(f"Output exists: {dst}. Use --force to overwrite.")

        total_rows = 0
        cleaned_rows = 0
        trimmed_rows = 0

        with open(src, "r", encoding="utf-8", newline="") as in_fp, open(
            dst, "w", encoding="utf-8", newline=""
        ) as out_fp:
            reader = csv.reader(in_fp, delimiter="|")
            writer = csv.writer(out_fp, delimiter="|", lineterminator="\n")

            for row in reader:
                total_rows += 1
                if row and row[-1] == "":
                    row = row[:-1]
                    trimmed_rows += 1

                if len(row) != expected_cols:
                    raise ValueError(
                        f"{table}: row {total_rows} has {len(row)} columns; expected {expected_cols}"
                    )

                cleaned = [cell.strip() for cell in row]
                writer.writerow(cleaned)
                cleaned_rows += 1

        report["tables"][table] = {
            "input_rows": total_rows,
            "output_rows": cleaned_rows,
            "trimmed_trailing_delimiter_rows": trimmed_rows,
            "expected_columns": expected_cols,
        }

    report_path = output_dir / "cleaning_report.json"
    with open(report_path, "w", encoding="utf-8") as fp:
        json.dump(report, fp, indent=2)

    print(f"Cleaning completed: {output_dir}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
