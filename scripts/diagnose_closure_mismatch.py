#!/usr/bin/env python3
"""Diagnose export vs web closure mismatches for incidents/crises.

Usage:
    python scripts/diagnose_closure_mismatch.py [action_no]

Without action_no, scans all items and reports likely mismatch causes.
"""
from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "crisis_data.db"


def is_closed_status(action_status: str | None, end_date: str | None) -> bool:
    status = (action_status or "").strip()
    end = (end_date or "").strip()
    return status == "Closed" or (bool(end) and end.lower() != "n/a")


def parse_date(date_str: str | None):
    if not date_str or str(date_str).strip().lower() == "n/a":
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    return None


def get_week_year(date: datetime) -> tuple[int, int]:
    return int(date.year), int(date.strftime("%U"))


def connect():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_items(conn, action_no: str | None = None):
    query = """
        SELECT i.id, i.action_no, i.data_type, i.resolution_date, i.resolution_time,
               i.activity_status
        FROM items i
    """
    params: tuple = ()
    if action_no:
        query += " WHERE i.action_no = ?"
        params = (action_no,)
    query += " ORDER BY i.action_no"
    return conn.execute(query, params).fetchall()


def fetch_week_status(conn, action_id: int):
    return conn.execute(
        """
        SELECT year, week, action_status, end_date, end_time
        FROM week_status
        WHERE action_id = ?
        ORDER BY year, week
        """,
        (action_id,),
    ).fetchall()


def latest_week_status(rows) -> sqlite3.Row | None:
    if not rows:
        return None
    return rows[-1]


def export_all_time_status(rows) -> sqlite3.Row | None:
    if not rows:
        return None
    return max(rows, key=lambda r: (r["year"], r["week"]))


def year_mode_status(rows, year: int) -> sqlite3.Row | None:
    in_year = [r for r in rows if r["year"] == year]
    if not in_year:
        return None
    return max(in_year, key=lambda r: r["week"])


def week_mode_status(rows, year: int, week: int) -> sqlite3.Row | None:
    for row in rows:
        if row["year"] == year and row["week"] == week:
            return row
    prior = [r for r in rows if (r["year"], r["week"]) < (year, week)]
    if prior:
        return prior[-1]
    future = [r for r in rows if (r["year"], r["week"]) > (year, week)]
    if future:
        return future[0]
    return None


def format_status(row: sqlite3.Row | None) -> str:
    if not row:
        return "Open (no week_status)"
    closed = is_closed_status(row["action_status"], row["end_date"])
    state = "Closed" if closed else "Open"
    end = row["end_date"] or ""
    return f"{state}, end_date={end or 'empty'}, week={row['year']}-W{row['week']}"


def diagnose_item(item, week_rows):
    issues = []
    action_no = item["action_no"]
    resolution = (item["resolution_date"] or "").strip()
    activity = (item["activity_status"] or "").strip()

    has_resolution = bool(resolution) and resolution.lower() != "n/a"
    any_closed = any(is_closed_status(r["action_status"], r["end_date"]) for r in week_rows)
    all_open = not any_closed

    if has_resolution and all_open:
        issues.append(
            "resolution_without_closure: Resolution Date is set but no week has Closure Date "
            "(Excel Resolution Date may look like a closed date while web shows Open)"
        )

    if activity.lower() == "closed" and all_open:
        issues.append(
            "activity_vs_action_status: Activity Status is Closed but Action Status is Open "
            "in all week_status rows (Excel Activity Status column vs web Incident Status badge)"
        )

    for i, earlier in enumerate(week_rows):
        if not is_closed_status(earlier["action_status"], earlier["end_date"]):
            continue
        parsed_close = parse_date(earlier["end_date"])
        if parsed_close:
            close_year, close_week = get_week_year(parsed_close)
        else:
            close_year, close_week = earlier["year"], earlier["week"]
        for later in week_rows[i + 1:]:
            if is_closed_status(later["action_status"], later["end_date"]):
                continue
            if (later["year"], later["week"]) <= (close_year, close_week):
                continue
            issues.append(
                "stale_open_after_close: "
                f"Closed at {close_year} W{close_week:02d} but Open at "
                f"{later['year']} W{later['week']:02d} "
                "(causes hidden in intermediate weeks and Open in later weeks)"
            )
            break

    if len(week_rows) > 1:
        statuses = {
            (
                (r["action_status"] or "").strip(),
                (r["end_date"] or "").strip(),
            )
            for r in week_rows
        }
        if len(statuses) > 1:
            issues.append(
                "week_snapshot_drift: action_status/end_date differ across weeks "
                "(export week X vs web week Y can disagree)"
            )

    export_latest = export_all_time_status(week_rows)
    if week_rows:
        years = sorted({r["year"] for r in week_rows})
        for year in years:
            year_row = year_mode_status(week_rows, year)
            export_row = export_latest
            if year_row and export_row and (year_row["year"], year_row["week"]) != (
                export_row["year"],
                export_row["week"],
            ):
                year_closed = is_closed_status(year_row["action_status"], year_row["end_date"])
                export_closed = is_closed_status(export_row["action_status"], export_row["end_date"])
                if year_closed != export_closed or (year_row["end_date"] or "") != (
                    export_row["end_date"] or ""
                ):
                    issues.append(
                        f"year_vs_all_time_{year}: Year mode latest ({format_status(year_row)}) "
                        f"differs from All Time export ({format_status(export_row)})"
                    )
                    break

    return issues


def print_item_report(item, week_rows):
    print(f"\n{'=' * 72}")
    print(f"{item['action_no']} ({item['data_type']})")
    print(f"  resolution_date: {item['resolution_date'] or 'empty'}")
    print(f"  activity_status: {item['activity_status'] or 'empty'}")
    print("  week_status history:")
    if not week_rows:
        print("    (none)")
    else:
        for row in week_rows:
            print(
                f"    {row['year']} W{row['week']:02d}: "
                f"status={row['action_status'] or 'Open'}, "
                f"end_date={row['end_date'] or 'empty'}, "
                f"end_time={row['end_time'] or 'empty'}"
            )

    if week_rows:
        years = sorted({r["year"] for r in week_rows})
        weeks = sorted({(r["year"], r["week"]) for r in week_rows})
        print("  simulated views:")
        print(f"    All Time export: {format_status(export_all_time_status(week_rows))}")
        for year in years:
            print(f"    Web year mode {year}: {format_status(year_mode_status(week_rows, year))}")
        for year, week in weeks[-3:]:
            print(f"    Web week mode {year} W{week}: {format_status(week_mode_status(week_rows, year, week))}")
            end = week_mode_status(week_rows, year, week)
            if end and is_closed_status(end["action_status"], end["end_date"]) and end["end_date"]:
                parsed = parse_date(end["end_date"])
                if parsed:
                    end_y, end_w = get_week_year(parsed)
                    visible = end_y == year and end_w == week
                    print(
                        f"      export current week {year} W{week}: "
                        f"{'included' if visible else 'EXCLUDED (closed in different week)'}"
                    )

    issues = diagnose_item(item, week_rows)
    if issues:
        print("  likely mismatch causes:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("  likely mismatch causes: none detected")


def main():
    action_filter = sys.argv[1] if len(sys.argv) > 1 else None
    conn = connect()
    items = fetch_items(conn, action_filter)
    if action_filter and not items:
        print(f"No item found for action_no={action_filter!r}")
        sys.exit(1)

    print(f"Closure mismatch diagnostic ({DB_PATH.name})")
    if action_filter:
        print(f"Filter: {action_filter}")
    print(
        "\nCompare export context when investigating: "
        "Current Week export uses the selected year/week; "
        "All Time export uses the latest week_status globally. "
        "Web Closure Date maps to export 'Closure Date' (not Resolution Date)."
    )

    flagged = 0
    for item in items:
        week_rows = fetch_week_status(conn, item["id"])
        issues = diagnose_item(item, week_rows)
        if action_filter or issues:
            print_item_report(item, week_rows)
            if issues:
                flagged += 1

    if not action_filter:
        print(f"\n{'=' * 72}")
        print(f"Summary: {flagged} item(s) with potential export vs web mismatch causes")

    conn.close()


if __name__ == "__main__":
    main()
