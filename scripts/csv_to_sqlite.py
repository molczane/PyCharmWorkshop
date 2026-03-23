#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a CSV file into a SQLite database table."
    )
    parser.add_argument("csv_path", type=Path, help="Path to the source CSV file.")
    parser.add_argument(
        "sqlite_path",
        type=Path,
        nargs="?",
        help="Path to the output SQLite database file. Defaults to CSV name with .sqlite suffix.",
    )
    parser.add_argument(
        "--table",
        help="SQLite table name. Defaults to the CSV file name without extension.",
    )
    parser.add_argument(
        "--if-exists",
        choices=("replace", "append", "fail"),
        default="replace",
        help="What to do when the table already exists.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows to insert per batch.",
    )
    return parser.parse_args()


def quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def is_integer(value: str) -> bool:
    try:
        int(value)
    except ValueError:
        return False
    return True


def is_real(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def infer_column_types(csv_path: Path, fieldnames: list[str]) -> dict[str, str]:
    column_types = {field: "INTEGER" for field in fieldnames}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            for field in fieldnames:
                value = (row.get(field) or "").strip()
                if not value or column_types[field] == "TEXT":
                    continue
                if is_integer(value):
                    continue
                if is_real(value):
                    column_types[field] = "REAL"
                    continue
                column_types[field] = "TEXT"

    return column_types


def convert_value(value: str | None, sqlite_type: str) -> str | int | float | None:
    if value is None:
        return None

    stripped = value.strip()
    if stripped == "":
        return None
    if sqlite_type == "INTEGER":
        return int(stripped)
    if sqlite_type == "REAL":
        return float(stripped)
    return value


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    query = """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        LIMIT 1
    """
    return connection.execute(query, (table_name,)).fetchone() is not None


def create_table(
    connection: sqlite3.Connection,
    table_name: str,
    fieldnames: list[str],
    column_types: dict[str, str],
    if_exists: str,
) -> None:
    quoted_table = quote_identifier(table_name)
    if if_exists == "fail" and table_exists(connection, table_name):
        raise SystemExit(f"Table '{table_name}' already exists in the database.")

    if if_exists == "replace":
        connection.execute(f"DROP TABLE IF EXISTS {quoted_table}")

    column_definitions = ", ".join(
        f"{quote_identifier(field)} {column_types[field]}" for field in fieldnames
    )
    connection.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} ({column_definitions})")


def insert_rows(
    connection: sqlite3.Connection,
    csv_path: Path,
    table_name: str,
    fieldnames: list[str],
    column_types: dict[str, str],
    batch_size: int,
) -> int:
    quoted_table = quote_identifier(table_name)
    quoted_columns = ", ".join(quote_identifier(field) for field in fieldnames)
    placeholders = ", ".join("?" for _ in fieldnames)
    insert_sql = (
        f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"
    )

    inserted_rows = 0
    batch: list[tuple[str | int | float | None, ...]] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            batch.append(
                tuple(
                    convert_value(row.get(field), column_types[field]) for field in fieldnames
                )
            )
            if len(batch) >= batch_size:
                connection.executemany(insert_sql, batch)
                inserted_rows += len(batch)
                batch.clear()

    if batch:
        connection.executemany(insert_sql, batch)
        inserted_rows += len(batch)

    return inserted_rows


def main() -> None:
    args = parse_args()
    csv_path = args.csv_path
    sqlite_path = args.sqlite_path or csv_path.with_suffix(".sqlite")
    table_name = args.table or csv_path.stem

    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be greater than 0.")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        fieldnames = next(reader, None)

    if not fieldnames:
        raise SystemExit(f"CSV file is empty: {csv_path}")

    column_types = infer_column_types(csv_path, fieldnames)

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(sqlite_path) as connection:
        create_table(connection, table_name, fieldnames, column_types, args.if_exists)
        inserted_rows = insert_rows(
            connection,
            csv_path,
            table_name,
            fieldnames,
            column_types,
            args.batch_size,
        )
        connection.commit()

    print(
        f"Imported {inserted_rows} rows from {csv_path} "
        f"into {sqlite_path} (table: {table_name})."
    )


if __name__ == "__main__":
    main()
