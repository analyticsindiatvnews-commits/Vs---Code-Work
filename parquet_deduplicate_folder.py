#!/usr/bin/env python3
"""
Batch Parquet Deduplicator
==========================

Purpose
-------
Reads all parquet files from an input folder, removes EXACT full-row duplicates
across the whole folder, and writes a cleaned parquet dataset to an output folder.

It does NOT modify/delete original parquet files.

Recommended install:
    pip install duckdb pyarrow pandas

Basic usage:
    python parquet_deduplicate_folder.py --input "D:/logs/raw" --output "D:/logs/clean_deduped"

Recursive scan:
    python parquet_deduplicate_folder.py --input "/data/raw" --output "/data/clean" --recursive

Single output file:
    python parquet_deduplicate_folder.py --input "/data/raw" --output "/data/clean" --single-file clean.parquet

Partitioned output by date column:
    python parquet_deduplicate_folder.py --input "/data/raw" --output "/data/clean" --partition-by event_date

Notes
-----
- Dedup mode is strict full-row dedup: SELECT DISTINCT *.
- Rows are considered duplicates only when every selected column value is identical.
- By default, schema mismatch is handled with union_by_name=true.
- Output is written as parquet by DuckDB, which is faster and safer than loading everything into pandas.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("DuckDB is required. Install with: pip install duckdb") from exc


LOGGER = logging.getLogger("parquet_deduplicator")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def collect_parquet_files(input_dir: Path, recursive: bool = False) -> List[Path]:
    pattern = "**/*.parquet" if recursive else "*.parquet"
    files = sorted(p for p in input_dir.glob(pattern) if p.is_file())
    # Deduplicate paths only to avoid accidental repeated command expansion; this does NOT dedup data by file.
    return sorted(set(files), key=lambda p: str(p).lower())


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def validate_paths(input_dir: Path, output_dir: Path, overwrite: bool) -> None:
    if not input_dir.exists() or not input_dir.is_dir():
        raise ValueError(f"Input folder does not exist or is not a directory: {input_dir}")

    if output_dir.resolve() == input_dir.resolve():
        raise ValueError("Output folder must be different from input folder to protect originals.")

    if output_dir.exists() and any(output_dir.iterdir()):
        if not overwrite:
            raise ValueError(
                f"Output folder already exists and is not empty: {output_dir}\n"
                "Use --overwrite to replace it, or choose a new output folder."
            )
        LOGGER.warning("Removing existing output folder because --overwrite was provided: %s", output_dir)
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)


def get_columns(con: duckdb.DuckDBPyConnection, files: List[Path], union_by_name: bool) -> List[str]:
    query = "DESCRIBE SELECT * FROM read_parquet(?, union_by_name := ?) LIMIT 0"
    rows = con.execute(query, [[str(f) for f in files], union_by_name]).fetchall()
    return [r[0] for r in rows]


def folder_stats(con: duckdb.DuckDBPyConnection, files: List[Path], union_by_name: bool) -> dict:
    total_rows = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?, union_by_name := ?)",
        [[str(f) for f in files], union_by_name],
    ).fetchone()[0]

    distinct_rows = con.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT * FROM read_parquet(?, union_by_name := ?))",
        [[str(f) for f in files], union_by_name],
    ).fetchone()[0]

    duplicate_rows = int(total_rows) - int(distinct_rows)
    duplicate_pct = (duplicate_rows / total_rows * 100.0) if total_rows else 0.0

    return {
        "input_files": len(files),
        "input_rows": int(total_rows),
        "unique_rows": int(distinct_rows),
        "duplicate_rows_removed": int(duplicate_rows),
        "duplicate_pct": round(duplicate_pct, 4),
    }


def build_copy_sql(
    files: List[Path],
    output_dir: Path,
    union_by_name: bool,
    compression: str,
    row_group_size: int,
    single_file: Optional[str],
    partition_by: Optional[str],
    order_by: Optional[str],
) -> tuple[str, list]:
    source = "read_parquet($files, union_by_name := $union_by_name)"
    select_sql = f"SELECT DISTINCT * FROM {source}"

    if order_by:
        select_sql += f" ORDER BY {quote_ident(order_by)}"

    if single_file:
        out_path = output_dir / single_file
        if out_path.suffix.lower() != ".parquet":
            out_path = out_path.with_suffix(".parquet")
        copy_target = str(out_path)
    else:
        copy_target = str(output_dir)

    options = [
        "FORMAT PARQUET",
        f"COMPRESSION '{compression}'",
        f"ROW_GROUP_SIZE {int(row_group_size)}",
    ]

    if partition_by:
        options.append(f"PARTITION_BY ({quote_ident(partition_by)})")

    sql = f"COPY ({select_sql}) TO $copy_target ({', '.join(options)})"
    params = {
        "files": [str(f) for f in files],
        "union_by_name": union_by_name,
        "copy_target": copy_target,
    }
    return sql, [params]


def write_summary(output_dir: Path, stats: dict, extra: dict) -> None:
    summary = {**stats, **extra}
    path = output_dir / "dedup_summary.json"
    path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    input_dir = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output).expanduser().resolve()
    validate_paths(input_dir, output_dir, overwrite=args.overwrite)

    files = collect_parquet_files(input_dir, recursive=args.recursive)
    if not files:
        raise ValueError(f"No parquet files found in {input_dir}")

    LOGGER.info("Found %s parquet file(s).", len(files))
    LOGGER.info("Input : %s", input_dir)
    LOGGER.info("Output: %s", output_dir)

    con = duckdb.connect(database=args.duckdb_database or ":memory:")
    con.execute(f"PRAGMA threads={int(args.threads)}")
    if args.memory_limit:
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    if args.temp_directory:
        temp_dir = Path(args.temp_directory).expanduser().resolve()
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{str(temp_dir).replace(chr(39), chr(39)+chr(39))}'")

    start = time.time()
    columns = get_columns(con, files, union_by_name=not args.strict_schema)
    LOGGER.info("Detected %s column(s).", len(columns))

    if args.partition_by and args.partition_by not in columns:
        raise ValueError(f"--partition-by column not found: {args.partition_by}")
    if args.order_by and args.order_by not in columns:
        raise ValueError(f"--order-by column not found: {args.order_by}")

    LOGGER.info("Counting duplicates with exact full-row DISTINCT...")
    stats = folder_stats(con, files, union_by_name=not args.strict_schema)
    LOGGER.info(
        "Rows: input=%s | unique=%s | duplicates_removed=%s | duplicate_pct=%s%%",
        f"{stats['input_rows']:,}",
        f"{stats['unique_rows']:,}",
        f"{stats['duplicate_rows_removed']:,}",
        stats["duplicate_pct"],
    )

    LOGGER.info("Writing deduplicated parquet output...")
    sql, param_wrappers = build_copy_sql(
        files=files,
        output_dir=output_dir,
        union_by_name=not args.strict_schema,
        compression=args.compression,
        row_group_size=args.row_group_size,
        single_file=args.single_file,
        partition_by=args.partition_by,
        order_by=args.order_by,
    )
    params = param_wrappers[0]
    con.execute(sql, params)

    elapsed = round(time.time() - start, 2)
    out_files = collect_parquet_files(output_dir, recursive=True)

    write_summary(
        output_dir,
        stats,
        {
            "input_folder": str(input_dir),
            "output_folder": str(output_dir),
            "output_files": len(out_files),
            "recursive_scan": bool(args.recursive),
            "strict_schema": bool(args.strict_schema),
            "compression": args.compression,
            "partition_by": args.partition_by,
            "single_file": args.single_file,
            "order_by": args.order_by,
            "elapsed_seconds": elapsed,
        },
    )

    LOGGER.info("Done. Output parquet files: %s", len(out_files))
    LOGGER.info("Summary written to: %s", output_dir / "dedup_summary.json")
    LOGGER.info("Elapsed: %s seconds", elapsed)
    return 0


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove exact full-row duplicates across all parquet files in a folder and recreate clean parquet output.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", required=True, help="Input folder containing parquet files.")
    parser.add_argument("--output", required=True, help="Output folder for cleaned parquet files. Must be different from input.")
    parser.add_argument("--recursive", action="store_true", help="Scan parquet files recursively.")
    parser.add_argument("--overwrite", action="store_true", help="Delete existing output folder before writing.")
    parser.add_argument("--single-file", default=None, help="Write one parquet file with this name instead of a parquet dataset folder.")
    parser.add_argument("--partition-by", default=None, help="Partition output by a column name, e.g. event_date.")
    parser.add_argument("--order-by", default=None, help="Optional column to order output by before writing.")
    parser.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip", "brotli", "uncompressed"], help="Output parquet compression.")
    parser.add_argument("--row-group-size", type=int, default=122_880, help="Parquet row group size.")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB worker threads.")
    parser.add_argument("--memory-limit", default=None, help="DuckDB memory limit, e.g. 8GB or 12000MB.")
    parser.add_argument("--temp-directory", default=None, help="Folder DuckDB can use for temporary spilling if needed.")
    parser.add_argument("--duckdb-database", default=None, help="Optional DuckDB database path instead of in-memory.")
    parser.add_argument("--strict-schema", action="store_true", help="Require identical schemas instead of union_by_name.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)
    try:
        return run(args)
    except Exception as exc:
        LOGGER.error("Failed: %s", exc)
        if args.verbose:
            LOGGER.exception("Full traceback")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
