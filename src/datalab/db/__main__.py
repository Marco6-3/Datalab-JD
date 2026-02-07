from __future__ import annotations

import argparse

from datalab.db.build import build_duckdb, write_example_queries
from datalab.logging_utils import setup_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="DataLab DuckDB utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build DuckDB from cleaned parquet.")
    build_parser.add_argument("--input", required=True, help="Input cleaned parquet path.")
    build_parser.add_argument("--output", required=True, help="Output duckdb file path.")
    build_parser.add_argument(
        "--query-doc",
        default="data/analytics/example_queries.md",
        help="Where to write example SQL queries markdown.",
    )
    build_parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )

    args = parser.parse_args()
    if args.command == "build":
        setup_logging(args.log_level)
        build_duckdb(args.input, args.output)
        write_example_queries(args.query_doc)


if __name__ == "__main__":
    main()

