#!/usr/bin/env python3
"""Seed demo Incident, Crisis, and Miscellaneous records into data/crisis_data.db."""

import argparse
import os
import sys
from datetime import datetime, timedelta

# Allow imports from repo root when run as scripts/seed_db.py
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

from app import (  # noqa: E402
    CORE_ITEM_FIELDS,
    MISC_CORE_FIELDS,
    get_db_connection,
    get_item_id,
    get_misc_task_id,
    init_db,
    item_dict_to_db_values,
    misc_dict_core_values,
    normalize_action_status,
    normalize_time_to_24h,
)

DEMO_ITEM_IDS = {
    "Incident": ["INC-DEMO-001", "INC-DEMO-002", "INC-DEMO-003"],
    "Crisis": ["CRZ-DEMO-001", "CRZ-DEMO-002"],
}
DEMO_MISC_IDS = ["MISC-DEMO-001", "MISC-DEMO-002"]


def get_target_year_week():
    now = datetime.now()
    return now.year, int(now.strftime("%U"))


def _iso_days_ago(days):
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")


def _iso_days_ahead(days):
    return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")


def _dmy_days_ago(days):
    return (datetime.now() - timedelta(days=days)).strftime("%d/%m/%Y")


def _dmy_days_ahead(days):
    return (datetime.now() + timedelta(days=days)).strftime("%d/%m/%Y")


def format_remark(date_dmy, text):
    return f"[{date_dmy}] {text}"


def format_action(date_dmy, assignee, text, *, scope="internal", vendor_ticket=None, closed=False):
    parts = [f"[{date_dmy}]"]
    if scope == "external":
        parts.append("[EXTERNAL]")
    if assignee:
        parts.append(f"[{assignee}]")
    if scope == "external" and vendor_ticket:
        parts.append(f"[Vendor: {vendor_ticket}]")
    if closed:
        parts.append("[CLOSED]")
    body = (text or "").strip()
    return f"{' '.join(parts)} {body}" if body else " ".join(parts)


def join_entries(*lines):
    return "\r\n".join(lines)


def demo_incidents():
    return [
        {
            "Action No.": "INC-DEMO-001",
            "ITSM Ticket": "INC0001001",
            "Section Head": "Infrastructure",
            "System": "Core Banking",
            "Category": "Availability",
            "Action Tracker ( Weekly Crisis Meeting)": "Yes",
            "Crisis/Incident": "Payment gateway timeout",
            "Description": "Intermittent timeouts on payment API affecting branch transactions.",
            "Action Status": "Open",
            "Owner": "Ops Team A",
            "Action Start Date": _iso_days_ago(5),
            "Start Time": "09:15",
            "Target Date": _iso_days_ahead(7),
            "Resolution Date": "",
            "Resolution Time": "",
            "Remarks": join_entries(
                format_remark(_dmy_days_ago(5), "Incident opened; error rate spike confirmed."),
                format_remark(_dmy_days_ago(3), "Vendor engaged; monitoring dashboards updated."),
                format_remark(_dmy_days_ago(1), "Awaiting vendor RCA draft."),
            ),
            "Actions": join_entries(
                format_action(_dmy_days_ago(4), "Ops Team A", "Increase connection pool on payment API."),
                format_action(_dmy_days_ago(2), "Network Ops", "Review firewall rules for timeout patterns."),
                format_action(_dmy_days_ago(1), "Ops Team A", "Validate fix in UAT before prod rollout."),
            ),
            "End Date": "",
            "Closure Time": "",
            "Target Date last update": _iso_days_ago(2),
            "Crossed Target Date": "No",
            "Activity Status": "Active",
            "Age": "5",
            "Age By Months/Year": "0",
            "Crisis Reference": "",
            "History": "",
            "Time to Acknowledge": "00:45",
            "Time to Recover": "",
            "Time to Detect": "00:12",
            "Hidden": "0",
        },
        {
            "Action No.": "INC-DEMO-002",
            "ITSM Ticket": "INC0001002",
            "Section Head": "Applications",
            "System": "CRM Portal",
            "Category": "Performance",
            "Action Tracker ( Weekly Crisis Meeting)": "Yes",
            "Crisis/Incident": "Slow login response",
            "Description": "Users report 30+ second login delays during peak hours.",
            "Action Status": "Open",
            "Owner": "App Support",
            "Action Start Date": _iso_days_ago(10),
            "Start Time": "14:30",
            "Target Date": _iso_days_ahead(3),
            "Resolution Date": _iso_days_ago(2),
            "Resolution Time": "11:00",
            "Remarks": join_entries(
                format_remark(_dmy_days_ago(10), "Login latency reported by multiple branches."),
                format_remark(_dmy_days_ago(6), "DB profiling shows missing index on session table."),
                format_remark(_dmy_days_ago(2), "Index rebuild scheduled for weekend maintenance window."),
            ),
            "Actions": join_entries(
                format_action(_dmy_days_ago(8), "App Support", "Profile DB queries during peak load."),
                format_action(_dmy_days_ago(5), "DBA Team", "Add read replica for authentication service."),
                format_action(
                    _dmy_days_ago(3),
                    "Vendor Support",
                    "Request emergency patch for CRM auth module.",
                    scope="external",
                    vendor_ticket="VND-88421",
                ),
            ),
            "End Date": "",
            "Closure Time": "",
            "Target Date last update": _iso_days_ago(1),
            "Crossed Target Date": "No",
            "Activity Status": "Active",
            "Age": "10",
            "Age By Months/Year": "0",
            "Crisis Reference": "",
            "History": "",
            "Time to Acknowledge": "01:20",
            "Time to Recover": "",
            "Time to Detect": "00:30",
            "Hidden": "0",
        },
        {
            "Action No.": "INC-DEMO-003",
            "ITSM Ticket": "INC0001003",
            "Section Head": "Network",
            "System": "WAN Links",
            "Category": "Connectivity",
            "Action Tracker ( Weekly Crisis Meeting)": "Yes",
            "Crisis/Incident": "Branch link flap",
            "Description": "Repeated BGP flaps on regional branch MPLS circuit.",
            "Action Status": "Closed",
            "Owner": "Network Ops",
            "Action Start Date": _iso_days_ago(14),
            "Start Time": "08:00",
            "Target Date": _iso_days_ago(7),
            "Resolution Date": _iso_days_ago(6),
            "Resolution Time": "16:45",
            "Remarks": join_entries(
                format_remark(_dmy_days_ago(14), "BGP flaps detected on regional MPLS circuit."),
                format_remark(_dmy_days_ago(8), "Carrier dispatched field engineer to branch site."),
                format_remark(_dmy_days_ago(6), "Faulty SFP module replaced; link stable for 24 hours."),
            ),
            "Actions": join_entries(
                format_action(
                    _dmy_days_ago(12),
                    "Network Ops",
                    "Initiate failover to backup WAN link.",
                    closed=True,
                ),
                format_action(
                    _dmy_days_ago(7),
                    "Carrier NOC",
                    "Replace faulty SFP module on branch router.",
                    scope="external",
                    vendor_ticket="CAR-55201",
                    closed=True,
                ),
                format_action(
                    _dmy_days_ago(6),
                    "Network Ops",
                    "Update runbook with failover test results.",
                    closed=True,
                ),
            ),
            "End Date": _iso_days_ago(6),
            "Closure Time": "16:45",
            "Target Date last update": _iso_days_ago(7),
            "Crossed Target Date": "No",
            "Activity Status": "Closed",
            "Age": "14",
            "Age By Months/Year": "0",
            "Crisis Reference": "",
            "History": "",
            "Time to Acknowledge": "00:30",
            "Time to Recover": "08:15",
            "Time to Detect": "00:05",
            "Hidden": "0",
        },
    ]


def demo_crises():
    return [
        {
            "Action No.": "CRZ-DEMO-001",
            "ITSM Ticket": "CRZ0002001",
            "Section Head": "Crisis Management",
            "System": "Data Center",
            "Category": "Power",
            "Action Tracker ( Weekly Crisis Meeting)": "Yes",
            "Crisis/Incident": "UPS maintenance overrun",
            "Description": "Planned UPS maintenance extended; failover to secondary site active.",
            "Action Status": "Open",
            "Owner": "DC Operations",
            "Action Start Date": _iso_days_ago(2),
            "Start Time": "06:00",
            "Target Date": _iso_days_ahead(1),
            "Resolution Date": "",
            "Resolution Time": "",
            "Remarks": join_entries(
                format_remark(_dmy_days_ago(2), "Crisis bridge convened; failover to secondary site active."),
                format_remark(_dmy_days_ago(1), "Hourly leadership updates in progress."),
                format_remark(_dmy_days_ago(0), "UPS vendor ETA confirmed for completion today."),
            ),
            "Actions": join_entries(
                format_action(_dmy_days_ago(2), "DC Operations", "Validate generator fuel levels and load test."),
                format_action(_dmy_days_ago(1), "DR Team", "Confirm DR site readiness and network paths."),
                format_action(_dmy_days_ago(0), "Crisis PMO", "Coordinate return-to-primary timeline with vendor."),
            ),
            "End Date": "",
            "Closure Time": "",
            "Target Date last update": _iso_days_ago(1),
            "Crossed Target Date": "No",
            "Activity Status": "Active",
            "Age": "2",
            "Age By Months/Year": "0",
            "Crisis Reference": "",
            "History": "",
            "Time to Acknowledge": "00:15",
            "Time to Recover": "",
            "Time to Detect": "00:02",
            "Hidden": "0",
        },
        {
            "Action No.": "CRZ-DEMO-002",
            "ITSM Ticket": "CRZ0002002",
            "Section Head": "Security",
            "System": "Email Gateway",
            "Category": "Security",
            "Action Tracker ( Weekly Crisis Meeting)": "Yes",
            "Crisis/Incident": "Phishing campaign blocked",
            "Description": "Targeted phishing wave detected and contained; no credential compromise confirmed.",
            "Action Status": "Closed",
            "Owner": "SOC",
            "Action Start Date": _iso_days_ago(8),
            "Start Time": "11:20",
            "Target Date": _iso_days_ago(5),
            "Resolution Date": _iso_days_ago(4),
            "Resolution Time": "18:00",
            "Remarks": join_entries(
                format_remark(_dmy_days_ago(8), "Phishing campaign detected by email gateway."),
                format_remark(_dmy_days_ago(5), "Malicious domains blocked; no credential compromise found."),
                format_remark(_dmy_days_ago(4), "User awareness bulletin sent to all staff."),
            ),
            "Actions": join_entries(
                format_action(
                    _dmy_days_ago(7),
                    "SOC",
                    "Block sender domains at email gateway.",
                    closed=True,
                ),
                format_action(
                    _dmy_days_ago(6),
                    "SOC",
                    "Reset affected mailboxes and enforce MFA.",
                    closed=True,
                ),
                format_action(
                    _dmy_days_ago(4),
                    "Security Comms",
                    "Publish phishing awareness bulletin.",
                    closed=True,
                ),
            ),
            "End Date": _iso_days_ago(4),
            "Closure Time": "18:00",
            "Target Date last update": _iso_days_ago(5),
            "Crossed Target Date": "No",
            "Activity Status": "Closed",
            "Age": "8",
            "Age By Months/Year": "0",
            "Crisis Reference": "",
            "History": "",
            "Time to Acknowledge": "00:10",
            "Time to Recover": "06:40",
            "Time to Detect": "00:08",
            "Hidden": "0",
        },
    ]


def demo_misc_tasks():
    return [
        {
            "Task No.": "MISC-DEMO-001",
            "ITSM Ticket": "TSK0003001",
            "Title": "Update crisis contact list",
            "Description": "Refresh on-call roster and escalation matrix for Q2.",
            "Assigned Team": "Crisis PMO",
            "Assigned By": "General Manager",
            "Status": "Open",
            "Created Date": _iso_days_ago(4),
            "Created Time": "10:00",
            "Due Date": _iso_days_ahead(10),
            "Remarks": join_entries(
                format_remark(_dmy_days_ago(4), "Task assigned; awaiting HR confirmation on new hires."),
            ),
            "Actions": join_entries(
                format_action(_dmy_days_ago(3), "Crisis PMO", "Draft updated on-call roster and escalation matrix."),
                format_action(_dmy_days_ago(1), "Crisis PMO", "Circulate matrix for GM sign-off."),
            ),
            "Completed Date": "",
            "Completed Time": "",
            "Hidden": "0",
        },
        {
            "Task No.": "MISC-DEMO-002",
            "ITSM Ticket": "TSK0003002",
            "Title": "Review DR test results",
            "Description": "Summarize findings from last month's disaster recovery exercise.",
            "Assigned Team": "Infrastructure",
            "Assigned By": "General Manager",
            "Status": "Closed",
            "Created Date": _iso_days_ago(20),
            "Created Time": "09:30",
            "Due Date": _iso_days_ago(5),
            "Remarks": join_entries(
                format_remark(_dmy_days_ago(5), "DR test report draft completed."),
                format_remark(_dmy_days_ago(3), "Report submitted to leadership."),
            ),
            "Actions": join_entries(
                format_action(
                    _dmy_days_ago(8),
                    "Infrastructure",
                    "Summarize DR test findings and gaps.",
                    closed=True,
                ),
                format_action(
                    _dmy_days_ago(4),
                    "Infrastructure",
                    "Schedule follow-up remediation tasks with owners.",
                    closed=True,
                ),
            ),
            "Completed Date": _iso_days_ago(3),
            "Completed Time": "15:00",
            "Hidden": "0",
        },
    ]


def delete_demo_records(cursor):
    for data_type, ids in DEMO_ITEM_IDS.items():
        for action_no in ids:
            item_id = get_item_id(cursor, data_type, action_no)
            if item_id:
                cursor.execute("DELETE FROM items WHERE id = ?", (item_id,))

    for task_no in DEMO_MISC_IDS:
        task_id = get_misc_task_id(cursor, task_no)
        if task_id:
            cursor.execute("DELETE FROM misc_tasks WHERE id = ?", (task_id,))


def insert_item(cursor, data_type, action, year, week):
    action_no = action.get("Action No.", "")
    if not action_no:
        return "skipped"

    item_id = get_item_id(cursor, data_type, action_no)
    if item_id:
        return "skipped"

    item_col_names = ["data_type", "action_no"] + CORE_ITEM_FIELDS
    placeholders = ", ".join(["?"] * len(item_col_names))
    values = item_dict_to_db_values(action, data_type)
    cursor.execute(
        f"INSERT INTO items ({', '.join(item_col_names)}) VALUES ({placeholders})",
        values,
    )
    item_id = cursor.lastrowid

    normalized_end_time = normalize_time_to_24h(action.get("Closure Time", ""))
    cursor.execute(
        """
        INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            item_id,
            year,
            week,
            normalize_action_status(action.get("Action Status", "")),
            action.get("End Date", ""),
            normalized_end_time,
        ),
    )
    return "inserted"


def insert_misc_task(cursor, task, year, week):
    task_no = task.get("Task No.", "")
    if not task_no:
        return "skipped"

    task_id = get_misc_task_id(cursor, task_no)
    if task_id:
        return "skipped"

    item_col_names = ["task_no"] + MISC_CORE_FIELDS
    placeholders = ", ".join(["?"] * len(item_col_names))
    item_values = [task_no] + misc_dict_core_values(task)
    cursor.execute(
        f"INSERT INTO misc_tasks ({', '.join(item_col_names)}) VALUES ({placeholders})",
        tuple(item_values),
    )
    task_id = cursor.lastrowid

    normalized_completed_time = normalize_time_to_24h(task.get("Completed Time", ""))
    cursor.execute(
        """
        INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            year,
            week,
            task.get("Status", "Open"),
            task.get("Completed Date", ""),
            normalized_completed_time,
        ),
    )
    return "inserted"


def seed(force=False):
    init_db()
    year, week = get_target_year_week()

    conn = get_db_connection()
    cursor = conn.cursor()

    if force:
        delete_demo_records(cursor)

    counts = {"inserted": 0, "skipped": 0}

    for data_type, records in [("Incident", demo_incidents()), ("Crisis", demo_crises())]:
        for action in records:
            result = insert_item(cursor, data_type, action, year, week)
            counts[result] += 1

    for task in demo_misc_tasks():
        result = insert_misc_task(cursor, task, year, week)
        counts[result] += 1

    conn.commit()
    conn.close()

    return year, week, counts


def main():
    parser = argparse.ArgumentParser(description="Seed demo data into crisis_data.db")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete existing *-DEMO-* records and re-insert them",
    )
    args = parser.parse_args()

    year, week, counts = seed(force=args.force)
    print(f"Seeded demo data for year {year}, week {week}")
    print(f"  inserted: {counts['inserted']}")
    print(f"  skipped:  {counts['skipped']}")


if __name__ == "__main__":
    main()
