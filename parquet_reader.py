import argparse
import os
from typing import Iterable, List

import pandas as pd
import pyarrow.parquet as pq


def iter_parquet_files(root: str) -> Iterable[str]:
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.lower().endswith(".parquet"):
                yield os.path.join(dirpath, name)


def read_parquet_sample(path: str, rows: int) -> pd.DataFrame:
    table = pq.read_table(path)
    df = table.to_pandas()
    if rows > 0:
        return df.head(rows)
    return df


def summarize_parquet(paths: List[str], sample_rows: int) -> None:
    total_rows = 0
    for path in paths:
        table = pq.read_table(path, columns=None)
        row_count = table.num_rows
        total_rows += row_count
        print(f"File: {path}")
        print(f"Rows: {row_count}")
        print(f"Schema: {table.schema}")
        if sample_rows:
            sample = table.slice(0, sample_rows).to_pandas()
            print(sample)
        print("-" * 80)

    print(f"Total rows across {len(paths)} file(s): {total_rows}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simple parquet reader to validate ETL output."
    )
    parser.add_argument(
        "path",
        help="Parquet file or directory containing parquet files.",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=5,
        help="Number of sample rows to print per file (default: 5).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=5,
        help="Maximum number of files to read from a directory (default: 5).",
    )
    args = parser.parse_args()

    target = args.path
    if os.path.isdir(target):
        files = list(iter_parquet_files(target))
        files.sort()
        if args.max_files > 0:
            files = files[: args.max_files]
    else:
        files = [target]

    if not files:
        print("No parquet files found.")
        return 1

    summarize_parquet(files, args.sample)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
