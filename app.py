import http.server
import socketserver
import json
import csv
import os
import io
import re
import sqlite3
import urllib.parse
from datetime import datetime, timedelta

# Load environment variables from .env file if it exists
if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                os.environ[key] = value

PORT = int(os.environ.get("PORT", 8005))
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "crisis_data.db")

COLUMNS = [
    "Action No.", "ITSM Ticket", "Section Head", "System", "Category", 
    "Action Tracker ( Weekly Crisis Meeting)", "Crisis/Incident", 
    "Description", "Action Status", "Owner", "Action Start Date", 
    "Start Time", "Target Date", "Resolution Date", "Resolution Time",
    "Remarks", "Actions", "End Date", "Closure Time", 
    # "Aging", 
    # "Number of postponed dates", 
    "Target Date last update", 
    "Crossed Target Date", "Activity Status", "Age", 
    "Age By Months/Year", "Crisis Reference", "History",
    "Time to Acknowledge", "Time to Recover", "Time to Detect", "Hidden"
]

# Mapping from CSV column names to SQLite column names (sanitized for SQL)
CSV_TO_DB = {
    "Action No.": "action_no",
    "ITSM Ticket": "itsm_ticket",
    "Section Head": "section_head",
    "System": "system",
    "Category": "category",
    "Action Tracker ( Weekly Crisis Meeting)": "action_tracker",
    "Crisis/Incident": "crisis_incident",
    "Description": "description",
    "Action Status": "action_status",
    "Owner": "owner",
    "Action Start Date": "action_start_date",
    "Start Time": "start_time",
    "Target Date": "target_date",
    "Resolution Date": "resolution_date",
    "Resolution Time": "resolution_time",
    "Remarks": "remarks",
    "Actions": "actions_field",
    "End Date": "end_date",
    "Closure Time": "end_time",
    "Target Date last update": "target_date_last_update",
    "Crossed Target Date": "crossed_target_date",
    "Activity Status": "activity_status",
    "Age": "age",
    "Age By Months/Year": "age_by_months_year",
    "Crisis Reference": "crisis_reference",
    "History": "history",
    "Time to Acknowledge": "time_to_acknowledge",
    "Time to Recover": "time_to_recover",
    "Time to Detect": "time_to_detect",
    "Hidden": "hidden"
}

# Reverse mapping: SQLite column names to CSV column names
DB_TO_CSV = {v: k for k, v in CSV_TO_DB.items()}

# SQLite column names in order (for queries)
DB_COLUMNS = list(CSV_TO_DB.values())

# Fields that belong in the week_status table (not items table)
WEEK_STATUS_FIELDS = {"action_status", "end_date", "end_time"}

# Core item fields (excluding week_status fields and action_no which is handled separately)
CORE_ITEM_FIELDS = [col for col in DB_COLUMNS if col not in WEEK_STATUS_FIELDS and col != "action_no"]

# Mapping for fields that have different names in frontend vs backend
FRONTEND_TO_BACKEND = {
    "Crisis/Incident": "crisis_incident",
    "Actions": "actions_field"
}
BACKEND_TO_FRONTEND = {v: k for k, v in FRONTEND_TO_BACKEND.items()}

# --- Miscellaneous (GM-assigned tasks) ---
MISC_COLUMNS = [
    "Task No.", "ITSM Ticket", "Title", "Description", "Assigned Team", "Assigned By",
    "Status", "Created Date", "Created Time", "Due Date", "Remarks", "Actions",
    "Completed Date", "Completed Time", "Hidden"
]

MISC_CSV_TO_DB = {
    "Task No.": "task_no",
    "ITSM Ticket": "itsm_ticket",
    "Title": "title",
    "Description": "description",
    "Assigned Team": "assigned_team",
    "Assigned By": "assigned_by",
    "Due Date": "due_date",
    "Remarks": "remarks",
    "Actions": "actions",
    "Created Date": "created_date",
    "Created Time": "created_time",
    "Hidden": "hidden",
}

MISC_DB_TO_CSV = {v: k for k, v in MISC_CSV_TO_DB.items()}

MISC_CORE_FIELDS = [
    "title", "itsm_ticket", "description", "assigned_team", "assigned_by",
    "created_date", "created_time", "due_date", "remarks", "actions", "hidden"
]

MISC_WEEK_STATUS_FIELDS = {"status", "completed_date", "completed_time"}

MISC_DATE_FIELDS = {"created_date", "due_date"}

MISC_TIME_FIELDS = {"created_time", "completed_time"}

EXPORT_PLURAL = {
    "Crisis": "Crises",
    "Incident": "Incidents",
    "Miscellaneous": "Miscellaneous",
}


def is_normalized_schema():
    """Check if the database uses the normalized schema (has week_status table)."""
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='week_status'")
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except:
        return False


def migrate_to_normalized_schema():
    """Migrate from old denormalized schema to new normalized schema."""
    import time
    start_time = time.time()
    
    if not db_exists():
        return {
            "status": "error",
            "message": "No database to migrate. Initialize database first."
        }
    
    if is_normalized_schema():
        return {
            "status": "error",
            "message": "Database is already using normalized schema."
        }
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    
    try:
        # Step 1: Rename existing table to items_legacy
        cursor.execute("ALTER TABLE items RENAME TO items_legacy")
        
        # Step 2: Create new normalized tables
        # Create items table with core fields only
        item_cols = ["id INTEGER PRIMARY KEY AUTOINCREMENT", "data_type TEXT NOT NULL", "action_no TEXT NOT NULL"]
        for col in CORE_ACTION_FIELDS:
            item_cols.append(f"{col} TEXT")
        item_cols.append("UNIQUE(data_type, action_no)")
        
        cursor.execute(f"CREATE TABLE items ({', '.join(item_cols)})")
        
        # Create week_status table
        cursor.execute("""
            CREATE TABLE week_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                week INTEGER NOT NULL,
                action_status TEXT,
                end_date TEXT,
                end_time TEXT,
                FOREIGN KEY (action_id) REFERENCES items(id) ON DELETE CASCADE,
                UNIQUE(action_id, year, week)
            )
        """)
        
        cursor.execute("CREATE INDEX idx_week_lookup ON week_status(year, week)")
        
        # Step 3: Extract unique actions from legacy table
        # Group by data_type and action_no, taking the most recent version
        cursor.execute("""
            SELECT data_type, action_no, MAX(year * 100 + week) as latest_week
            FROM items_legacy
            GROUP BY data_type, action_no
        """)
        
        unique_actions = cursor.fetchall()
        migrated_actions = 0
        migrated_weeks = 0
        
        # Step 4: For each unique action, get the latest version and insert
        for data_type, action_no, _ in unique_actions:
            # Get the latest version of this action
            cursor.execute("""
                SELECT * FROM items_legacy
                WHERE data_type = ? AND action_no = ?
                ORDER BY year DESC, week DESC
                LIMIT 1
            """, (data_type, action_no))
            
            latest_row = cursor.fetchone()
            if not latest_row:
                continue
            
            # Build INSERT for items table (core fields only)
            action_values = [data_type, action_no]
            for col in CORE_ACTION_FIELDS:
                # Find column index in legacy table
                col_idx = None
                for i, desc in enumerate(cursor.description):
                    if desc[0] == col:
                        col_idx = i
                        break
                action_values.append(latest_row[col_idx] if col_idx is not None else "")
            
            placeholders = ", ".join(["?"] * len(action_values))
            action_col_names = ["data_type", "action_no"] + CORE_ACTION_FIELDS
            cursor.execute(
                f"INSERT INTO items ({', '.join(action_col_names)}) VALUES ({placeholders})",
                action_values
            )
            action_id = cursor.lastrowid
            migrated_actions += 1
            
            # Step 5: Insert all week_status entries for this action
            cursor.execute("""
                SELECT year, week, action_status, end_date, end_time
                FROM items_legacy
                WHERE data_type = ? AND action_no = ?
                ORDER BY year, week
            """, (data_type, action_no))
            
            week_rows = cursor.fetchall()
            for year, week, action_status, end_date, end_time in week_rows:
                cursor.execute("""
                    INSERT INTO week_status (action_id, year, week, action_status, end_date, end_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (action_id, year, week, action_status or "", end_date or "", end_time or ""))
                migrated_weeks += 1
        
        conn.commit()
        
        elapsed_time = time.time() - start_time
        
        # Verify data integrity
        cursor.execute("SELECT COUNT(*) FROM items_legacy")
        legacy_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM week_status")
        new_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "status": "success",
            "message": "Database successfully migrated to normalized schema.",
            "migrated_actions": migrated_actions,
            "migrated_weeks": migrated_weeks,
            "legacy_rows": legacy_count,
            "new_week_status_rows": new_count,
            "elapsed_time": f"{elapsed_time:.2f}s",
            "storage_reduction": f"{((legacy_count - migrated_actions) / legacy_count * 100):.1f}%" if legacy_count > 0 else "0%"
        }
        
    except Exception as e:
        # Rollback on error
        try:
            cursor.execute("DROP TABLE IF EXISTS items")
            cursor.execute("DROP TABLE IF EXISTS week_status")
            cursor.execute("ALTER TABLE items_legacy RENAME TO items")
            conn.commit()
        except:
            pass
        conn.close()
        return {
            "status": "error",
            "message": f"Migration failed: {str(e)}"
        }


def run_schema_migrations(conn):
    """Apply incremental schema migrations to an existing database connection."""
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
    if cursor.fetchone():
        try:
            cursor.execute("SELECT hidden FROM items LIMIT 1")
        except sqlite3.OperationalError:
            print("Migrating database: Adding hidden column...")
            cursor.execute("ALTER TABLE items ADD COLUMN hidden INTEGER DEFAULT 0")

        try:
            cursor.execute("SELECT time_to_acknowledge FROM items LIMIT 1")
        except sqlite3.OperationalError:
            print("Migrating database: Adding TTA, TTR, TTD columns...")
            cursor.execute("ALTER TABLE items ADD COLUMN time_to_acknowledge TEXT")
            cursor.execute("ALTER TABLE items ADD COLUMN time_to_recover TEXT")
            cursor.execute("ALTER TABLE items ADD COLUMN time_to_detect TEXT")

        try:
            cursor.execute("SELECT resolution_date FROM items LIMIT 1")
        except sqlite3.OperationalError:
            print("Migrating database: Adding Resolution Date and Resolution Time columns...")
            cursor.execute("ALTER TABLE items ADD COLUMN resolution_date TEXT")
            cursor.execute("ALTER TABLE items ADD COLUMN resolution_time TEXT")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='misc_tasks'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(misc_tasks)")
        misc_cols = [row[1] for row in cursor.fetchall()]
        if 'actions' not in misc_cols:
            print("Migrating database: Adding actions column to misc_tasks...")
            cursor.execute("ALTER TABLE misc_tasks ADD COLUMN actions TEXT DEFAULT ''")
        if 'itsm_ticket' not in misc_cols:
            print("Migrating database: Adding itsm_ticket column to misc_tasks...")
            cursor.execute("ALTER TABLE misc_tasks ADD COLUMN itsm_ticket TEXT DEFAULT ''")


def ensure_schema_migrations():
    """Run schema migrations on an existing database file."""
    if not os.path.exists(DB_PATH):
        return
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    run_schema_migrations(conn)
    conn.commit()
    conn.close()


def init_db():
    """Initialize the SQLite database with normalized schema."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    # Migration: Rename actions to items if it exists
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='actions'")
        if cursor.fetchone():
            print("Migrating database: Renaming actions table to items...")
            cursor.execute("ALTER TABLE actions RENAME TO items")
            # Update foreign key in week_status if it exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='week_status'")
            if cursor.fetchone():
                # SQLite doesn't support ALTER TABLE for foreign keys easily, 
                # but since we are renaming the referenced table, we might need to recreate week_status
                # if we want to be strictly correct. However, SQLite often handles this if PRAGMA legacy_alter_table is OFF.
                # For safety, we'll ensure week_status is created with the correct reference.
                pass
    except sqlite3.OperationalError:
        pass
    
    # Create normalized items table
    item_cols = [
        "id INTEGER PRIMARY KEY AUTOINCREMENT",
        "data_type TEXT NOT NULL",
        "action_no TEXT NOT NULL"
    ]
    for col in CORE_ITEM_FIELDS:
        item_cols.append(f"{col} TEXT")
    item_cols.append("UNIQUE(data_type, action_no)")
    
    cursor.execute(f"CREATE TABLE IF NOT EXISTS items ({', '.join(item_cols)})")
    
    # Create week_status table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS week_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            week INTEGER NOT NULL,
            action_status TEXT,
            end_date TEXT,
            end_time TEXT,
            FOREIGN KEY (action_id) REFERENCES items(id) ON DELETE CASCADE,
            UNIQUE(action_id, year, week)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_week_lookup ON week_status(year, week)")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS misc_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_no TEXT NOT NULL UNIQUE,
            title TEXT,
            itsm_ticket TEXT DEFAULT '',
            description TEXT,
            assigned_team TEXT,
            assigned_by TEXT,
            created_date TEXT,
            created_time TEXT,
            due_date TEXT,
            remarks TEXT,
            actions TEXT DEFAULT '',
            hidden INTEGER DEFAULT 0
        )
    """)

    run_schema_migrations(conn)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS misc_week_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            week INTEGER NOT NULL,
            status TEXT,
            completed_date TEXT,
            completed_time TEXT,
            FOREIGN KEY (task_id) REFERENCES misc_tasks(id) ON DELETE CASCADE,
            UNIQUE(task_id, year, week)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_misc_week_lookup ON misc_week_status(year, week)")
    
    conn.commit()
    conn.close()


def normalize_date(date_str):
    """Convert various date formats (d-MMM-yyyy, d-MMM-yy, dd/mm/yyyy, etc.) to ISO YYYY-MM-DD."""
    if not date_str or date_str.strip().lower() == "n/a" or not date_str.strip():
        return date_str
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y", "%d-%b-%y", "%d/%m/%y"):
        try:
            parsed = datetime.strptime(date_str.strip(), fmt)
            if parsed.year < 100:
                parsed = parsed.replace(year=parsed.year + 2000)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


DATE_FIELDS = {"action_start_date", "target_date", "target_date_last_update", "resolution_date"}

TIME_FIELDS = {"start_time", "end_time", "resolution_time"}


def get_db_connection():
    """Returns a connection to the SQLite database with row_factory set."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(item_row, week_status_row=None):
    """Converts SQLite Rows to an item dictionary with CSV column names.
    
    Args:
        item_row: Row from items table
        week_status_row: Optional Row from week_status table
    """
    if item_row is None:
        return None
    
    item = {}
    
    # Add core item fields
    for csv_col, db_col in CSV_TO_DB.items():
        if db_col in CORE_ITEM_FIELDS or db_col == "action_no" or db_col == "hidden":
            # Use BACKEND_TO_FRONTEND mapping if necessary
            frontend_key = BACKEND_TO_FRONTEND.get(db_col, csv_col)
            item[frontend_key] = item_row[db_col] if db_col in item_row.keys() and item_row[db_col] is not None else ""
    
    # Add hidden status (for frontend internal use)
    hidden_val = item_row["hidden"] if "hidden" in item_row.keys() else None
    try:
        item["hidden"] = int(hidden_val) if hidden_val is not None and hidden_val != '' else 0
    except (ValueError, TypeError):
        item["hidden"] = 0
    
    # Add week status fields if provided
    if week_status_row:
        item["Action Status"] = week_status_row["action_status"] if week_status_row["action_status"] else ""
        item["End Date"] = week_status_row["end_date"] if week_status_row["end_date"] else ""
        item["Closure Time"] = week_status_row["end_time"] if week_status_row["end_time"] else ""
    else:
        item["Action Status"] = ""
        item["End Date"] = ""
        item["Closure Time"] = ""
    
    return item


def item_dict_to_db_values(item, data_type):
    """Converts item dictionary to values for items table insertion."""
    values = [data_type, item.get("Action No.", "")]
    for col in CORE_ITEM_FIELDS:
        csv_col = DB_TO_CSV.get(col)
        if csv_col:
            # Use FRONTEND_TO_BACKEND mapping if necessary
            frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
            val = item.get(frontend_key, "")
            if col in DATE_FIELDS:
                val = normalize_date(val)
            elif col in TIME_FIELDS:
                val = normalize_time_to_24h(val)
            values.append(val)
    return tuple(values)


ITEM_WEEK_STATUS_CSV = {"Action Status", "End Date", "Closure Time"}
MISC_WEEK_STATUS_CSV = {"Status", "Completed Date", "Completed Time"}


def detect_import_delimiter(text):
    """Detect comma vs tab (Excel paste) delimiter."""
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',\t;')
        return dialect.delimiter
    except csv.Error:
        tab_count = sample.count('\t')
        comma_count = sample.count(',')
        return '\t' if tab_count > comma_count else ','


def match_header_to_canonical(raw_header, expected_headers, used):
    """Map a pasted header cell to a canonical column name."""
    raw = raw_header.strip()
    if not raw:
        return None

    raw_lower = raw.lower()

    for exp in expected_headers:
        if exp not in used and exp.lower() == raw_lower:
            return exp

    prefix_matches = []
    for exp in expected_headers:
        if exp in used:
            continue
        exp_lower = exp.lower()
        if exp_lower.startswith(raw_lower) or (
            len(raw_lower) >= 3 and raw_lower.startswith(exp_lower[:len(raw_lower)])
        ):
            prefix_matches.append(exp)

    if len(prefix_matches) == 1:
        return prefix_matches[0]

    starts_with = [c for c in prefix_matches if c.lower().startswith(raw_lower)]
    if len(starts_with) == 1:
        return starts_with[0]
    if starts_with:
        return max(starts_with, key=len)

    for exp in expected_headers:
        if exp not in used and exp.lower().startswith(raw_lower):
            return exp

    return None


def count_header_matches(row, expected_headers):
    used = set()
    matches = 0
    for cell in row:
        canonical = match_header_to_canonical(cell, expected_headers, used)
        if canonical:
            used.add(canonical)
            matches += 1
    return matches


def row_looks_like_header(row, expected_headers):
    if not row:
        return False
    id_header = expected_headers[0]
    if row[0].strip() == id_header:
        return True
    matches = count_header_matches(row, expected_headers)
    return matches >= max(2, len(row) // 2)


def row_looks_like_data(row, expected_headers, id_header):
    if not row or not any(cell.strip() for cell in row):
        return False
    if row[0].strip() == id_header:
        return False
    if count_header_matches(row, expected_headers) >= max(2, len(row) // 2):
        return False
    return True


def map_header_row(raw_headers, expected_headers):
    mapped = []
    present_columns = []
    used = set()
    for raw in raw_headers:
        canonical = match_header_to_canonical(raw, expected_headers, used)
        if canonical:
            used.add(canonical)
            mapped.append(canonical)
            present_columns.append(canonical)
        else:
            stripped = raw.strip()
            mapped.append(stripped)
            if stripped:
                present_columns.append(stripped)
    return mapped, present_columns


def parse_import_text(text, data_type):
    """Parse CSV/TSV import text into row dicts with column validation."""
    expected_headers = MISC_COLUMNS if data_type == "Miscellaneous" else COLUMNS
    id_header = expected_headers[0]

    validation = {
        "valid": False,
        "message": "",
        "header_columns": 0,
        "rows_parsed": 0,
        "rows_valid": 0,
        "rows_invalid": [],
        "present_columns": [],
    }

    text = text.strip()
    if not text:
        validation["message"] = "Import data is empty"
        return [], set(), validation

    delimiter = detect_import_delimiter(text)
    all_rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))

    if not all_rows:
        validation["message"] = "Import data is empty"
        return [], set(), validation

    has_header = row_looks_like_header(all_rows[0], expected_headers)

    if has_header:
        mapped_headers, present_columns = map_header_row(all_rows[0], expected_headers)
        data_rows_raw = all_rows[1:]
        header_row_num = 1
    elif row_looks_like_data(all_rows[0], expected_headers, id_header):
        mapped_headers = list(expected_headers)
        present_columns = list(expected_headers)
        data_rows_raw = all_rows
        header_row_num = 0
    else:
        validation["message"] = (
            f"Could not detect header row. Include column headers or start with {id_header}."
        )
        return [], set(), validation

    header_columns = len(mapped_headers)
    validation["header_columns"] = header_columns
    validation["present_columns"] = present_columns

    if header_columns < 1:
        validation["message"] = "No columns found in import data"
        return [], set(present_columns), validation

    if header_columns < 2:
        validation["message"] = "Import must include at least 2 columns (identifier + at least one field)"
        return [], set(present_columns), validation

    if id_header not in present_columns:
        validation["message"] = f"Import must include the {id_header} column to match or create records"
        return [], set(present_columns), validation

    rows = []
    rows_invalid = []

    for idx, raw_row in enumerate(data_rows_raw):
        row_num = (header_row_num + 1 + idx) if has_header else (idx + 1)
        if not any(cell.strip() for cell in raw_row):
            continue

        col_count = len(raw_row)
        if col_count != header_columns:
            rows_invalid.append({"row": row_num, "columns": col_count, "expected": header_columns})
            continue

        row_dict = {}
        for i, canonical in enumerate(mapped_headers):
            row_dict[canonical] = raw_row[i] if i < len(raw_row) else ""
        rows.append(row_dict)

    validation["rows_parsed"] = len(data_rows_raw)
    validation["rows_valid"] = len(rows)
    validation["rows_invalid"] = [entry["row"] for entry in rows_invalid]

    if rows_invalid:
        first = rows_invalid[0]
        validation["message"] = (
            f"Column count mismatch: header has {header_columns} columns, "
            f"row {first['row']} has {first['columns']} columns"
        )
        if len(rows_invalid) > 1:
            validation["message"] += f" ({len(rows_invalid)} rows with mismatched columns)"
        return [], set(present_columns), validation

    if not rows:
        validation["message"] = "No valid data rows found in import"
        return [], set(present_columns), validation

    validation["valid"] = True
    return rows, set(present_columns), validation


def get_item_id(cursor, data_type, action_no):
    """Get the action_id for a given data_type and action_no."""
    cursor.execute(
        "SELECT id FROM items WHERE data_type = ? AND action_no = ?",
        (data_type, action_no)
    )
    row = cursor.fetchone()
    return row[0] if row else None


ID_PREFIX = {
    "Incident": "M",
    "Crisis": "C",
    "Miscellaneous": "T",
}


def parse_serial_number(value, prefix):
    """Extract serial integer from prefixed ID (M3) or legacy plain numeric (3)."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.match(rf"^{re.escape(prefix)}(\d+)$", s, re.IGNORECASE)
    if m:
        return int(m.group(1))
    if re.match(r"^\d+$", s):
        return int(s)
    return None


def get_next_prefixed_id(cursor, data_type):
    """Return next auto-increment ID for Incident (M), Crisis (C), or Task (T)."""
    prefix = ID_PREFIX.get(data_type)
    if not prefix:
        raise ValueError(f"Unknown data_type: {data_type}")
    if data_type == "Miscellaneous":
        cursor.execute("SELECT task_no FROM misc_tasks")
        rows = [r[0] for r in cursor.fetchall()]
    else:
        cursor.execute(
            "SELECT action_no FROM items WHERE data_type = ?",
            (data_type,),
        )
        rows = [r[0] for r in cursor.fetchall()]
    max_n = 0
    for row in rows:
        n = parse_serial_number(row, prefix)
        if n is not None and n > max_n:
            max_n = n
    return f"{prefix}{max_n + 1}"


def db_exists():
    """Check if the SQLite database exists and has been initialized."""
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except:
        return False


def normalize_time_to_24h(time_str):
    """Normalize time string to 24-hour format (HH:MM).
    
    Handles various input formats:
    - 24-hour format: "15:30", "9:05"
    - 12-hour format: "3:30 PM", "11:36 PM"
    - Returns empty string for invalid/empty input
    """
    if not time_str or time_str.strip() == '' or time_str == 'N/A':
        return ''
    
    time_str = time_str.strip()
    
    # Check if already in 24-hour format (HH:MM)
    if ':' in time_str and not ('AM' in time_str.upper() or 'PM' in time_str.upper()):
        try:
            parts = time_str.split(':')
            if len(parts) >= 2:
                hours = int(parts[0])
                minutes = int(parts[1])
                if 0 <= hours < 24 and 0 <= minutes < 60:
                    return f"{hours:02d}:{minutes:02d}"
        except (ValueError, IndexError):
            pass
    
    # Parse 12-hour format with AM/PM
    try:
        time_upper = time_str.upper()
        has_am = 'AM' in time_upper
        has_pm = 'PM' in time_upper
        
        if has_am or has_pm:
            # Remove AM/PM and get the time part
            time_part = time_upper.replace('AM', '').replace('PM', '').strip()
            parts = time_part.split(':')
            if len(parts) >= 2:
                hours = int(parts[0])
                minutes = int(parts[1])
                
                # Convert to 24-hour format
                if has_pm and hours != 12:
                    hours += 12
                elif has_am and hours == 12:
                    hours = 0
                
                if 0 <= hours < 24 and 0 <= minutes < 60:
                    return f"{hours:02d}:{minutes:02d}"
    except (ValueError, IndexError):
        pass
    
    # If we can't parse it, return as-is (fallback)
    return time_str


def misc_row_to_dict(task_row, week_status_row=None):
    """Convert misc_tasks + misc_week_status rows to API/CSV dict."""
    if task_row is None:
        return None

    item = {}
    for csv_col, db_col in MISC_CSV_TO_DB.items():
        item[csv_col] = task_row[db_col] if db_col in task_row.keys() and task_row[db_col] is not None else ""

    hidden_val = task_row["hidden"] if "hidden" in task_row.keys() else None
    try:
        item["hidden"] = int(hidden_val) if hidden_val is not None and hidden_val != '' else 0
    except (ValueError, TypeError):
        item["hidden"] = 0

    if week_status_row:
        item["Status"] = week_status_row["status"] if week_status_row["status"] else ""
        item["Completed Date"] = week_status_row["completed_date"] if week_status_row["completed_date"] else ""
        item["Completed Time"] = week_status_row["completed_time"] if week_status_row["completed_time"] else ""
    else:
        item["Status"] = ""
        item["Completed Date"] = ""
        item["Completed Time"] = ""

    return item


def misc_dict_core_values(item):
    """Values for misc_tasks insert/update (excluding task_no)."""
    values = []
    for col in MISC_CORE_FIELDS:
        csv_col = MISC_DB_TO_CSV.get(col)
        val = item.get(csv_col, "") if csv_col else ""
        if col in MISC_DATE_FIELDS:
            val = normalize_date(val)
        elif col in MISC_TIME_FIELDS:
            val = normalize_time_to_24h(val)
        values.append(val)
    return values


def get_misc_task_id(cursor, task_no):
    cursor.execute("SELECT id FROM misc_tasks WHERE task_no = ?", (task_no,))
    row = cursor.fetchone()
    return row[0] if row else None


def misc_is_closed(item):
    status = item.get("Status", "")
    completed_date = item.get("Completed Date", "")
    return status == "Closed" or (completed_date and completed_date.strip() and completed_date.lower() != "n/a")


def misc_has_started_by_week(item, target_year, target_week):
    start_date_str = item.get("Created Date", "")
    if not start_date_str or start_date_str == "N/A":
        return True
    try:
        if "-" in start_date_str:
            year, month, day = map(int, start_date_str.split("-"))
        elif "/" in start_date_str:
            day, month, year = map(int, start_date_str.split("/"))
        else:
            return True
        start_date = datetime(year, month, day)
        year_start = datetime(target_year, 1, 1)
        first_sunday_offset = (7 - year_start.weekday()) % 7
        first_sunday = year_start + timedelta(days=first_sunday_offset)
        if target_week == 0:
            week_end = first_sunday - timedelta(days=1)
        else:
            week_end = first_sunday + timedelta(weeks=target_week, days=-1)
        return start_date <= week_end
    except Exception:
        return True


def misc_carry_over_from_previous(target_year, target_week):
    if not db_exists():
        return []

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT ws.year, ws.week
        FROM misc_week_status ws
        ORDER BY ws.year DESC, ws.week DESC
    """)
    weeks_available = cursor.fetchall()

    for row in weeks_available:
        y, w = row['year'], row['week']
        if y > target_year:
            continue
        if y == target_year and w >= target_week:
            continue

        cursor.execute("""
            SELECT t.*, ws.status, ws.completed_date, ws.completed_time
            FROM misc_tasks t
            INNER JOIN misc_week_status ws ON t.id = ws.task_id
            WHERE ws.year = ? AND ws.week = ?
        """, (y, w))
        prev_rows = cursor.fetchall()
        if len(prev_rows) == 0:
            continue

        prev_items = []
        for r in prev_rows:
            week_status = {
                "status": r["status"],
                "completed_date": r["completed_date"],
                "completed_time": r["completed_time"],
            }
            prev_items.append(misc_row_to_dict(r, week_status))

        carried = [
            a for a in prev_items
            if not misc_is_closed(a) and misc_has_started_by_week(a, target_year, target_week)
        ]
        conn.close()
        return carried

    conn.close()
    return []


class CrisisHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Custom logging to see what's happening
        print(f"[{self.date_time_string()}] {format % args}")

    def end_headers(self):
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == "/api/data":
            self.handle_get_data(parsed_path.query)
        elif parsed_path.path == "/api/next-id":
            self.handle_get_next_id(parsed_path.query)
        elif parsed_path.path == "/api/export":
            self.handle_export(parsed_path.query)
        else:
            super().do_GET()

    def do_POST(self):
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == "/api/data":
            self.handle_save_data()
        elif parsed_path.path == "/api/hide-row":
            self.handle_hide_row()
        elif parsed_path.path == "/api/import":
            self.handle_import()
        elif parsed_path.path == "/api/regularize":
            self.handle_regularize()
        elif parsed_path.path == "/api/migrate_schema":
            self.handle_migrate_schema()
        else:
            self.send_error(404)

    def handle_get_next_id(self, query):
        params = urllib.parse.parse_qs(query)
        data_type = params.get("type", ["Incident"])[0]
        if data_type not in ID_PREFIX:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps({"error": f"Invalid type. Use one of: {', '.join(ID_PREFIX)}"}).encode()
            )
            return
        if db_exists():
            conn = get_db_connection()
            cursor = conn.cursor()
            next_id = get_next_prefixed_id(cursor, data_type)
            conn.close()
        else:
            next_id = f"{ID_PREFIX[data_type]}1"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps({"next_id": next_id, "data_type": data_type}).encode()
        )

    def handle_hide_row(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        
        action_no = post_data.get("action_no")
        data_type = post_data.get("type", "Incident")
        hidden = post_data.get("hidden", 1)
        
        if not action_no:
            self.send_error(400, "Missing action_no")
            return
            
        conn = get_db_connection()
        cursor = conn.cursor()
        if data_type == "Miscellaneous":
            cursor.execute(
                "UPDATE misc_tasks SET hidden = ? WHERE task_no = ?",
                (hidden, action_no)
            )
        else:
            cursor.execute(
                "UPDATE items SET hidden = ? WHERE action_no = ? AND data_type = ?",
                (hidden, action_no, data_type)
            )
        conn.commit()
        conn.close()
        
        self.send_response(200)
        self.end_headers()

    def handle_import(self):
        """Handle POST /api/import - Import CSV data into the database."""
        content_type = self.headers.get('Content-Type', '')
        if 'multipart/form-data' not in content_type:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "Content-Type must be multipart/form-data"}).encode())
            return

        try:
            # Simple multipart parser
            boundary_parts = content_type.split("boundary=")
            if len(boundary_parts) < 2:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "Missing boundary in Content-Type"}).encode())
                return
                
            boundary = boundary_parts[1].encode()
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "Empty request body"}).encode())
                return
                
            body = self.rfile.read(content_length)
            
            # Use a more robust split that handles both --boundary and --boundary--
            parts = body.split(b'--' + boundary)
            
            file_content = None
            csv_text_raw = None
            year = None
            week = None
            data_type = "Incident"
            
            for part in parts:
                if not part.strip() or part == b'--\r\n':
                    continue
                    
                if b'name="file"' in part:
                    # Extract file content - skip headers
                    header_end = part.find(b'\r\n\r\n')
                    if header_end != -1:
                        file_content = part[header_end+4:].rstrip(b'\r\n')
                elif b'name="csv_text"' in part:
                    header_end = part.find(b'\r\n\r\n')
                    if header_end != -1:
                        csv_text_raw = part[header_end+4:].rstrip(b'\r\n').decode('utf-8')
                elif b'name="year"' in part:
                    header_end = part.find(b'\r\n\r\n')
                    if header_end != -1:
                        year_val = part[header_end+4:].rstrip(b'\r\n').decode().strip()
                        if year_val:
                            year = int(year_val)
                elif b'name="week"' in part:
                    header_end = part.find(b'\r\n\r\n')
                    if header_end != -1:
                        week_val = part[header_end+4:].rstrip(b'\r\n').decode().strip()
                        if week_val:
                            week = int(week_val)
                elif b'name="type"' in part:
                    header_end = part.find(b'\r\n\r\n')
                    if header_end != -1:
                        data_type = part[header_end+4:].rstrip(b'\r\n').decode().strip()

            if (file_content is None and csv_text_raw is None) or year is None or week is None:
                missing = []
                if file_content is None and csv_text_raw is None:
                    missing.append("file or csv_text")
                if year is None:
                    missing.append("year")
                if week is None:
                    missing.append("week")
                
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": f"Missing required fields: {', '.join(missing)}"}).encode())
                return

            # Parse CSV
            if file_content is not None:
                try:
                    csv_text = file_content.decode('utf-8-sig')
                except UnicodeDecodeError:
                    csv_text = file_content.decode('latin-1')
            else:
                csv_text = csv_text_raw

            rows, present_columns, validation = parse_import_text(csv_text, data_type)
            if not validation["valid"]:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "message": validation["message"],
                    "validation": {
                        "header_columns": validation["header_columns"],
                        "rows_parsed": validation["rows_parsed"],
                        "rows_valid": validation["rows_valid"],
                        "rows_invalid": validation["rows_invalid"],
                        "present_columns": validation["present_columns"],
                    }
                }).encode())
                return

            validation_summary = {
                "header_columns": validation["header_columns"],
                "rows_parsed": validation["rows_parsed"],
                "rows_valid": validation["rows_valid"],
                "rows_invalid": validation["rows_invalid"],
                "present_columns": validation["present_columns"],
            }
            
            new_count = 0
            updated_count = 0
            skipped_count = 0
            error_count = 0
            
            # Ensure database exists
            if not db_exists():
                init_db()

            if data_type == "Miscellaneous":
                conn = get_db_connection()
                cursor = conn.cursor()
                for row in rows:
                    try:
                        task_no = row.get("Task No.")
                        if not task_no or str(task_no).strip() == "Task No.":
                            continue
                        task_id = get_misc_task_id(cursor, task_no)
                        if not task_id:
                            item_col_names = ["task_no"] + MISC_CORE_FIELDS
                            placeholders = ", ".join(["?"] * len(item_col_names))
                            item_values = [task_no] + misc_dict_core_values(row)
                            cursor.execute(
                                f"INSERT INTO misc_tasks ({', '.join(item_col_names)}) VALUES ({placeholders})",
                                tuple(item_values)
                            )
                            task_id = cursor.lastrowid
                            cursor.execute("""
                                INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (task_id, year, week,
                                  row.get("Status", "Open"),
                                  normalize_date(row.get("Completed Date", "")),
                                  normalize_time_to_24h(row.get("Completed Time", ""))))
                            new_count += 1
                        else:
                            changed = False
                            cursor.execute(f"SELECT {', '.join(MISC_CORE_FIELDS)} FROM misc_tasks WHERE id = ?", (task_id,))
                            existing_item = cursor.fetchone()
                            if existing_item:
                                for i, col in enumerate(MISC_CORE_FIELDS):
                                    csv_col = MISC_DB_TO_CSV.get(col)
                                    if csv_col and csv_col not in present_columns:
                                        continue
                                    if csv_col:
                                        csv_val = str(row.get(csv_col, "")).strip()
                                        db_val = str(existing_item[i] if existing_item[i] is not None else "").strip()
                                        if csv_val != db_val:
                                            changed = True
                                            break
                            week_fields_present = MISC_WEEK_STATUS_CSV & present_columns
                            if not changed and week_fields_present:
                                cursor.execute("""
                                    SELECT status, completed_date, completed_time
                                    FROM misc_week_status WHERE task_id = ? AND year = ? AND week = ?
                                """, (task_id, year, week))
                                existing_status = cursor.fetchone()
                                if not existing_status:
                                    changed = True
                                else:
                                    if "Status" in present_columns and str(row.get("Status", "")).strip() != str(existing_status["status"] or "").strip():
                                        changed = True
                                    elif "Completed Date" in present_columns and str(row.get("Completed Date", "")).strip() != str(existing_status["completed_date"] or "").strip():
                                        changed = True
                                    elif "Completed Time" in present_columns and str(row.get("Completed Time", "")).strip() != str(existing_status["completed_time"] or "").strip():
                                        changed = True
                            if changed:
                                update_parts = []
                                values = []
                                for col in MISC_CORE_FIELDS:
                                    csv_col = MISC_DB_TO_CSV.get(col)
                                    if csv_col and csv_col not in present_columns:
                                        continue
                                    if csv_col:
                                        update_parts.append(f"{col} = ?")
                                        val = row.get(csv_col, "")
                                        if col in MISC_DATE_FIELDS:
                                            val = normalize_date(val)
                                        elif col in MISC_TIME_FIELDS:
                                            val = normalize_time_to_24h(val)
                                        values.append(val)
                                if update_parts:
                                    values.append(task_id)
                                    cursor.execute(
                                        f"UPDATE misc_tasks SET {', '.join(update_parts)} WHERE id = ?",
                                        values
                                    )
                                if week_fields_present:
                                    cursor.execute("""
                                        SELECT status, completed_date, completed_time
                                        FROM misc_week_status WHERE task_id = ? AND year = ? AND week = ?
                                    """, (task_id, year, week))
                                    existing_status = cursor.fetchone()
                                    status = row.get("Status", "") if "Status" in present_columns else (
                                        existing_status["status"] if existing_status else ""
                                    )
                                    completed_date = normalize_date(row.get("Completed Date", "")) if "Completed Date" in present_columns else (
                                        existing_status["completed_date"] if existing_status else ""
                                    )
                                    completed_time = normalize_time_to_24h(row.get("Completed Time", "")) if "Completed Time" in present_columns else (
                                        existing_status["completed_time"] if existing_status else ""
                                    )
                                    cursor.execute("""
                                        INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                    """, (task_id, year, week, status, completed_date, completed_time))
                                updated_count += 1
                            else:
                                skipped_count += 1
                    except Exception as row_err:
                        print(f"Error importing misc row: {row_err}")
                        error_count += 1
                conn.commit()
                conn.close()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "success",
                    "new": new_count,
                    "updated": updated_count,
                    "skipped": skipped_count,
                    "errors": error_count,
                    "validation": validation_summary
                }).encode())
                return
                
            conn = get_db_connection()
            cursor = conn.cursor()
            
            for row in rows:
                try:
                    action_no = row.get("Action No.")
                    if not action_no or str(action_no).strip() == "Action No.":
                        continue
                        
                    # Check if item exists
                    cursor.execute(
                        "SELECT id FROM items WHERE data_type = ? AND action_no = ?",
                        (data_type, action_no)
                    )
                    item_row = cursor.fetchone()
                    item_id = item_row[0] if item_row else None
                    
                    if not item_id:
                        # Insert new item
                        item_col_names = ["data_type", "action_no"] + CORE_ITEM_FIELDS
                        placeholders = ", ".join(["?"] * len(item_col_names))
                        values = item_dict_to_db_values(row, data_type)
                        cursor.execute(
                            f"INSERT INTO items ({', '.join(item_col_names)}) VALUES ({placeholders})",
                            values
                        )
                        item_id = cursor.lastrowid
                        
                        # Insert week status for the import week only
                        cursor.execute("""
                            INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (item_id, year, week,
                              row.get("Action Status", ""),
                              normalize_date(row.get("End Date", "")),
                              row.get("Closure Time", "")))
                        
                        new_count += 1
                    else:
                        # Check if anything changed
                        changed = False
                        week_fields_present = ITEM_WEEK_STATUS_CSV & present_columns
                        
                        # 1. Check core fields
                        cursor.execute(f"SELECT {', '.join(CORE_ITEM_FIELDS)} FROM items WHERE id = ?", (item_id,))
                        existing_item = cursor.fetchone()
                        
                        if existing_item:
                            for i, col in enumerate(CORE_ITEM_FIELDS):
                                csv_col = DB_TO_CSV.get(col)
                                if csv_col:
                                    frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
                                    if frontend_key not in present_columns:
                                        continue
                                    csv_val = str(row.get(frontend_key, "")).strip()
                                    db_val = str(existing_item[i] if existing_item[i] is not None else "").strip()
                                    if csv_val != db_val:
                                        changed = True
                                        break
                        
                        # 2. Check week status
                        if not changed and week_fields_present:
                            cursor.execute("""
                                SELECT action_status, end_date, end_time 
                                FROM week_status 
                                WHERE action_id = ? AND year = ? AND week = ?
                            """, (item_id, year, week))
                            existing_status = cursor.fetchone()
                            
                            if not existing_status:
                                changed = True
                            else:
                                if "Action Status" in present_columns:
                                    csv_status = str(row.get("Action Status", "")).strip()
                                    db_status = str(existing_status["action_status"] or "").strip()
                                    if csv_status != db_status:
                                        changed = True
                                if not changed and "End Date" in present_columns:
                                    csv_end_date = str(row.get("End Date", "")).strip()
                                    db_end_date = str(existing_status["end_date"] or "").strip()
                                    if csv_end_date != db_end_date:
                                        changed = True
                                if not changed and "Closure Time" in present_columns:
                                    csv_end_time = str(row.get("Closure Time", "")).strip()
                                    db_end_time = str(existing_status["end_time"] or "").strip()
                                    if csv_end_time != db_end_time:
                                        changed = True
                        
                        if changed:
                            # Update existing item core fields (only pasted columns)
                            update_parts = []
                            values = []
                            for col in CORE_ITEM_FIELDS:
                                csv_col = DB_TO_CSV.get(col)
                                if csv_col:
                                    frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
                                    if frontend_key not in present_columns:
                                        continue
                                    update_parts.append(f"{col} = ?")
                                    val = row.get(frontend_key, "")
                                    if col in DATE_FIELDS:
                                        val = normalize_date(val)
                                    values.append(val)
                            if update_parts:
                                values.append(item_id)
                                cursor.execute(
                                    f"UPDATE items SET {', '.join(update_parts)} WHERE id = ?",
                                    values
                                )
                            
                            # Insert or update week status (merge with existing for omitted fields)
                            if week_fields_present:
                                cursor.execute("""
                                    SELECT action_status, end_date, end_time
                                    FROM week_status
                                    WHERE action_id = ? AND year = ? AND week = ?
                                """, (item_id, year, week))
                                existing_status = cursor.fetchone()
                                action_status = row.get("Action Status", "") if "Action Status" in present_columns else (
                                    existing_status["action_status"] if existing_status else ""
                                )
                                end_date = normalize_date(row.get("End Date", "")) if "End Date" in present_columns else (
                                    existing_status["end_date"] if existing_status else ""
                                )
                                end_time = row.get("Closure Time", "") if "Closure Time" in present_columns else (
                                    existing_status["end_time"] if existing_status else ""
                                )
                                cursor.execute("""
                                    INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (item_id, year, week, action_status, end_date, end_time))
                            
                            updated_count += 1
                        else:
                            skipped_count += 1
                    
                    # Update last modified time of the database file to trigger frontend refresh
                    if new_count > 0 or updated_count > 0:
                        os.utime(DB_PATH, None)
                except Exception as e:
                    print(f"Error importing row: {e}")
                    error_count += 1
            
            conn.commit()
            conn.close()
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success",
                "new": new_count,
                "updated": updated_count,
                "skipped": skipped_count,
                "errors": error_count,
                "validation": validation_summary
            }).encode())
            
        except Exception as e:
            print(f"Import failed: {e}")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": f"Import failed: {str(e)}"}).encode())

    def handle_regularize(self):
        """Initialize SQLite database and migrate all existing CSV data into normalized schema."""
        # Initialize the database (creates it if it doesn't exist)
        init_db()
        
        if not os.path.exists(DATA_DIR):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success", 
                "message": "Database initialized. No CSV data to migrate.", 
                "migrated": 0,
                "files_processed": 0
            }).encode())
            return

        migrated_count = 0
        files_processed = 0
        error_count = 0

        conn = get_db_connection()
        cursor = conn.cursor()

        for root, dirs, files in os.walk(DATA_DIR):
            for file in files:
                # Process Incident, Crisis, and Action CSV files
                if ((file.startswith("Incident_Week_") or file.startswith("Crisis_Week_") or
                     file.startswith("Action_Week_") or file.startswith("Miscellaneous_Week_")) and
                        file.endswith(".csv")):
                    file_path = os.path.join(root, file)
                    try:
                        # Extract year from directory name and week/type from filename
                        year_dir = os.path.basename(root)
                        if not year_dir.isdigit():
                            continue
                        year = int(year_dir)
                        
                        # Parse filename to get data_type and week
                        # Format: {Type}_Week_{N}.csv
                        parts = file.replace(".csv", "").split("_Week_")
                        if len(parts) != 2:
                            continue
                        data_type = parts[0]  # Incident, Crisis, Action, or Miscellaneous
                        week = int(parts[1])

                        if data_type == "Miscellaneous":
                            tasks = self.read_misc_csv(file_path)
                            if tasks is not None:
                                for task in tasks:
                                    task_no = task.get("Task No.", "")
                                    if not task_no:
                                        continue
                                    task_id = get_misc_task_id(cursor, task_no)
                                    if not task_id:
                                        item_col_names = ["task_no"] + MISC_CORE_FIELDS
                                        placeholders = ", ".join(["?"] * len(item_col_names))
                                        item_values = [task_no] + misc_dict_core_values(task)
                                        cursor.execute(
                                            f"INSERT INTO misc_tasks ({', '.join(item_col_names)}) VALUES ({placeholders})",
                                            tuple(item_values)
                                        )
                                        task_id = cursor.lastrowid
                                    normalized_completed_time = normalize_time_to_24h(task.get("Completed Time", ""))
                                    cursor.execute("""
                                        INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                    """, (task_id, year, week,
                                          task.get("Status", "Open"),
                                          task.get("Completed Date", ""),
                                          normalized_completed_time))
                                    migrated_count += 1
                                files_processed += 1
                            else:
                                error_count += 1
                            continue
                        
                        actions = self.read_csv(file_path)
                        if actions is not None:
                            for action in actions:
                                action_no = action.get("Action No.", "")
                                
                                # Check if item exists
                                item_id = get_item_id(cursor, data_type, action_no)
                                
                                if not item_id:
                                    # Insert new item
                                    item_col_names = ["data_type", "action_no"] + CORE_ITEM_FIELDS
                                    placeholders = ", ".join(["?"] * len(item_col_names))
                                    values = item_dict_to_db_values(action, data_type)
                                    cursor.execute(
                                        f"INSERT INTO items ({', '.join(item_col_names)}) VALUES ({placeholders})",
                                        values
                                    )
                                    item_id = cursor.lastrowid
                                
                                # Insert week status (use INSERT OR REPLACE to handle duplicates)
                                # Normalize time fields to 24-hour format
                                normalized_end_time = normalize_time_to_24h(action.get("Closure Time", ""))
                                cursor.execute("""
                                    INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (item_id, year, week,
                                      action.get("Action Status", ""),
                                      action.get("End Date", ""),
                                      normalized_end_time))
                                
                                migrated_count += 1
                            files_processed += 1
                        else:
                            error_count += 1
                    except Exception as e:
                        print(f"Error migrating {file_path}: {e}")
                        error_count += 1

        conn.commit()
        conn.close()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "status": "success",
            "message": "Database initialized and CSV data migrated to normalized schema.",
            "migrated": migrated_count,
            "files_processed": files_processed,
            "errors": error_count
        }).encode())

    def get_data_path(self, year, week, data_type="Incident"):
        dt = data_type.capitalize()
        year_dir = os.path.join(DATA_DIR, str(year))
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
        return os.path.join(year_dir, f"{dt}_Week_{week}.csv")

    def read_csv(self, file_path):
        if not os.path.exists(file_path):
            return []
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                actions = []
                for row in reader:
                    # Ensure all expected columns are present
                    for col in COLUMNS:
                        if col not in row:
                            row[col] = ""
                    actions.append(row)
                return actions
        except PermissionError:
            # Handle cases where Excel might have the file locked
            return None

    def write_csv(self, file_path, data):
        with open(file_path, mode='w', encoding='utf-8-sig', newline='') as f:
            # Quote all fields to keep embedded newlines safe for Excel
            writer = csv.DictWriter(
                f,
                fieldnames=COLUMNS,
                extrasaction='ignore',
                quoting=csv.QUOTE_ALL
            )
            writer.writeheader()
            writer.writerows(data)

    def get_current_week(self):
        # %U treats Sunday as the first day of the week
        now = datetime.now()
        return int(now.strftime("%U"))

    def parse_date(self, date_str):
        if not date_str or date_str.lower() == "n/a":
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    def get_week_year(self, date):
        # Using %U to match the dashboard's Sunday-based week logic
        return int(date.year), int(date.strftime("%U"))

    def handle_migrate_schema(self):
        """Handle POST /api/migrate_schema - migrate to normalized schema."""
        result = migrate_to_normalized_schema()
        
        self.send_response(200 if result["status"] == "success" else 400)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())

    def handle_get_misc_data(self, year, week, is_year_mode):
        conn = get_db_connection()
        run_schema_migrations(conn)
        conn.commit()
        cursor = conn.cursor()
        last_modified = os.path.getmtime(DB_PATH) if os.path.exists(DB_PATH) else 0

        if is_year_mode:
            cursor.execute("""
                SELECT t.*, ws.status, ws.completed_date, ws.completed_time
                FROM misc_tasks t
                LEFT JOIN (
                    SELECT task_id, status, completed_date, completed_time,
                           ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY year DESC, week DESC) as rn
                    FROM misc_week_status
                    WHERE year = ?
                ) ws ON t.id = ws.task_id AND ws.rn = 1
            """, (year,))
        else:
            cursor.execute("""
                SELECT t.*, ws.status, ws.completed_date, ws.completed_time
                FROM misc_tasks t
                LEFT JOIN misc_week_status ws ON t.id = ws.task_id AND ws.year = ? AND ws.week = ?
            """, (year, week))

        rows = cursor.fetchall()
        items_with_status = [row for row in rows if row["status"] is not None]
        items_without_status = [row for row in rows if row["status"] is None]
        items = []

        for row in items_with_status:
            week_status = {
                "status": row["status"],
                "completed_date": row["completed_date"],
                "completed_time": row["completed_time"],
            }
            items.append(misc_row_to_dict(row, week_status))

        if items_without_status and not is_year_mode:
            made_changes = False
            for row in items_without_status:
                task_id = row["id"]
                cursor.execute("""
                    SELECT status, completed_date, completed_time
                    FROM misc_week_status
                    WHERE task_id = ? AND (year < ? OR (year = ? AND week < ?))
                    ORDER BY year DESC, week DESC
                    LIMIT 1
                """, (task_id, year, year, week))
                nearest = cursor.fetchone()
                if not nearest:
                    cursor.execute("""
                        SELECT status, completed_date, completed_time
                        FROM misc_week_status
                        WHERE task_id = ? AND (year > ? OR (year = ? AND week > ?))
                        ORDER BY year ASC, week ASC
                        LIMIT 1
                    """, (task_id, year, year, week))
                    nearest = cursor.fetchone()
                if nearest:
                    status_val = nearest["status"] or ""
                    completed_date_val = nearest["completed_date"] or ""
                    completed_time_val = nearest["completed_time"] or ""
                    week_status = {
                        "status": status_val,
                        "completed_date": completed_date_val,
                        "completed_time": completed_time_val,
                    }
                    items.append(misc_row_to_dict(row, week_status))
                    cursor.execute("""
                        INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (task_id, year, week, status_val, completed_date_val, completed_time_val))
                    made_changes = True
            if made_changes:
                conn.commit()
                last_modified = os.path.getmtime(DB_PATH)
        elif items_without_status and is_year_mode:
            for row in items_without_status:
                task_id = row["id"]
                cursor.execute("""
                    SELECT status, completed_date, completed_time
                    FROM misc_week_status
                    WHERE task_id = ?
                    ORDER BY year DESC, week DESC
                    LIMIT 1
                """, (task_id,))
                nearest = cursor.fetchone()
                if nearest:
                    week_status = {
                        "status": nearest["status"] or "",
                        "completed_date": nearest["completed_date"] or "",
                        "completed_time": nearest["completed_time"] or "",
                    }
                    items.append(misc_row_to_dict(row, week_status))
                else:
                    items.append(misc_row_to_dict(row, None))

        conn.close()
        return items, last_modified

    def handle_save_misc_data(self, year, week, new_item, old_item_no, is_update):
        task_no = new_item.get("Task No.", "").strip()
        title = new_item.get("Title", "").strip()
        assigned_team = new_item.get("Assigned Team", "").strip()
        assigned_by = new_item.get("Assigned By", "").strip()

        if not task_no or not title or not assigned_team or not assigned_by:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "error": "Task No., Title, Assigned Team, and Assigned By are required"
            }).encode())
            return

        conn = get_db_connection()
        run_schema_migrations(conn)
        conn.commit()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) as cnt FROM misc_week_status WHERE year = ? AND week = ?",
            (year, week)
        )
        count = cursor.fetchone()['cnt']

        if count == 0 and not is_update:
            carried_items = misc_carry_over_from_previous(year, week)
            for item in carried_items:
                t_no = item.get("Task No.", "")
                task_id = get_misc_task_id(cursor, t_no)
                if not task_id:
                    item_col_names = ["task_no"] + MISC_CORE_FIELDS
                    placeholders = ", ".join(["?"] * len(item_col_names))
                    item_values = [t_no] + misc_dict_core_values(item)
                    cursor.execute(
                        f"INSERT INTO misc_tasks ({', '.join(item_col_names)}) VALUES ({placeholders})",
                        tuple(item_values)
                    )
                    task_id = cursor.lastrowid
                normalized_completed_time = normalize_time_to_24h(item.get("Completed Time", ""))
                cursor.execute("""
                    INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (task_id, year, week,
                      item.get("Status", "Open"),
                      item.get("Completed Date", ""),
                      normalized_completed_time))

        if old_item_no and old_item_no != task_no:
            old_task_id = get_misc_task_id(cursor, old_item_no)
            if old_task_id:
                cursor.execute("DELETE FROM misc_tasks WHERE id = ?", (old_task_id,))

        task_id = get_misc_task_id(cursor, task_no)

        if not is_update and not old_item_no and task_id:
            conn.close()
            self.send_response(409)
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Task Number already exists"}).encode())
            return

        if task_id:
            changed = False
            cursor.execute(f"SELECT {', '.join(MISC_CORE_FIELDS)} FROM misc_tasks WHERE id = ?", (task_id,))
            existing_item = cursor.fetchone()
            if existing_item:
                for i, col in enumerate(MISC_CORE_FIELDS):
                    csv_col = MISC_DB_TO_CSV.get(col)
                    if csv_col:
                        new_val = str(new_item.get(csv_col, "")).strip()
                        db_val = str(existing_item[i] if existing_item[i] is not None else "").strip()
                        if new_val != db_val:
                            changed = True
                            break
            if not changed:
                cursor.execute("""
                    SELECT status, completed_date, completed_time
                    FROM misc_week_status WHERE task_id = ? AND year = ? AND week = ?
                """, (task_id, year, week))
                existing_status = cursor.fetchone()
                if not existing_status:
                    changed = True
                else:
                    new_status = str(new_item.get("Status", "")).strip()
                    db_status = str(existing_status["status"] or "").strip()
                    new_completed_date = str(new_item.get("Completed Date", "")).strip()
                    db_completed_date = str(existing_status["completed_date"] or "").strip()
                    new_completed_time = str(new_item.get("Completed Time", "")).strip()
                    db_completed_time = str(existing_status["completed_time"] or "").strip()
                    if (new_status != db_status or new_completed_date != db_completed_date or
                            new_completed_time != db_completed_time):
                        changed = True
            if changed:
                update_parts = []
                values = []
                for col in MISC_CORE_FIELDS:
                    csv_col = MISC_DB_TO_CSV.get(col)
                    if csv_col:
                        update_parts.append(f"{col} = ?")
                        val = new_item.get(csv_col, "")
                        if col in MISC_DATE_FIELDS:
                            val = normalize_date(val)
                        elif col in MISC_TIME_FIELDS:
                            val = normalize_time_to_24h(val)
                        values.append(val)
                values.append(task_id)
                cursor.execute(
                    f"UPDATE misc_tasks SET {', '.join(update_parts)} WHERE id = ?",
                    values
                )
                normalized_completed_time = normalize_time_to_24h(new_item.get("Completed Time", ""))
                cursor.execute("""
                    INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (task_id, year, week,
                      new_item.get("Status", "Open"),
                      new_item.get("Completed Date", ""),
                      normalized_completed_time))
                os.utime(DB_PATH, None)
        else:
            item_col_names = ["task_no"] + MISC_CORE_FIELDS
            placeholders = ", ".join(["?"] * len(item_col_names))
            item_values = [task_no] + misc_dict_core_values(new_item)
            cursor.execute(
                f"INSERT INTO misc_tasks ({', '.join(item_col_names)}) VALUES ({placeholders})",
                tuple(item_values)
            )
            task_id = cursor.lastrowid
            normalized_completed_time = normalize_time_to_24h(new_item.get("Completed Time", ""))
            cursor.execute("""
                INSERT OR REPLACE INTO misc_week_status (task_id, year, week, status, completed_date, completed_time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (task_id, year, week,
                  new_item.get("Status", "Open"),
                  new_item.get("Completed Date", ""),
                  normalized_completed_time))
            os.utime(DB_PATH, None)

        conn.commit()
        conn.close()
        self.send_response(200)
        self.end_headers()

    def handle_get_data(self, query):
        import json
        from datetime import datetime
        params = urllib.parse.parse_qs(query)
        year = int(params.get("year", [str(datetime.now().year)])[0])
        week_param = params.get("week", [str(self.get_current_week())])[0]
        is_year_mode = (week_param == "all")
        week = int(week_param) if not is_year_mode else self.get_current_week()
        data_type = params.get("type", ["Incident"])[0]

        # Check if database exists
        if not db_exists():
            # Return empty data with a hint to initialize the database
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "items": [],
                "current_year": year,
                "current_week": week,
                "data_type": data_type,
                "last_modified": 0,
                "db_initialized": False
            }).encode())
            return

        if data_type == "Miscellaneous":
            items, last_modified = self.handle_get_misc_data(year, week, is_year_mode)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "items": items,
                "current_year": year,
                "current_week": week,
                "data_type": data_type,
                "last_modified": last_modified,
                "db_initialized": True
            }).encode())
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        # Get last modified time from DB file
        last_modified = os.path.getmtime(DB_PATH) if os.path.exists(DB_PATH) else 0

        if is_year_mode:
            # Year mode: Get all items with their most recent status for the year
            cursor.execute("""
                SELECT a.*, ws.action_status, ws.end_date, ws.end_time
                FROM items a
                LEFT JOIN (
                    SELECT action_id, action_status, end_date, end_time,
                           ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY year DESC, week DESC) as rn
                    FROM week_status
                    WHERE year = ?
                ) ws ON a.id = ws.action_id AND ws.rn = 1
                WHERE a.data_type = ?
            """, (year, data_type))
        else:
            # Week mode: Get items with status for specific week
            cursor.execute("""
                SELECT a.*, ws.action_status, ws.end_date, ws.end_time
                FROM items a
                LEFT JOIN week_status ws ON a.id = ws.action_id AND ws.year = ? AND ws.week = ?
                WHERE a.data_type = ?
            """, (year, week, data_type))

        rows = cursor.fetchall()

        items_with_status = [row for row in rows if row["action_status"] is not None]
        items_without_status = [row for row in rows if row["action_status"] is None]

        items = []

        for row in items_with_status:
            week_status = {
                "action_status": row["action_status"],
                "end_date": row["end_date"],
                "end_time": row["end_time"]
            }
            items.append(row_to_dict(row, week_status))

        if items_without_status and not is_year_mode:
            made_changes = False
            for row in items_without_status:
                item_id = row["id"]

                # Find the most recent week_status BEFORE this week
                cursor.execute("""
                    SELECT action_status, end_date, end_time
                    FROM week_status
                    WHERE action_id = ? AND (year < ? OR (year = ? AND week < ?))
                    ORDER BY year DESC, week DESC
                    LIMIT 1
                """, (item_id, year, year, week))
                nearest = cursor.fetchone()

                if not nearest:
                    # No previous status — fallback to the earliest FUTURE week
                    cursor.execute("""
                        SELECT action_status, end_date, end_time
                        FROM week_status
                        WHERE action_id = ? AND (year > ? OR (year = ? AND week > ?))
                        ORDER BY year ASC, week ASC
                        LIMIT 1
                    """, (item_id, year, year, week))
                    nearest = cursor.fetchone()

                if nearest:
                    status_val = nearest["action_status"] or ""
                    end_date_val = nearest["end_date"] or ""
                    end_time_val = nearest["end_time"] or ""

                    week_status = {
                        "action_status": status_val,
                        "end_date": end_date_val,
                        "end_time": end_time_val
                    }
                    items.append(row_to_dict(row, week_status))

                    cursor.execute("""
                        INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (item_id, year, week, status_val, end_date_val, end_time_val))
                    made_changes = True

            if made_changes:
                conn.commit()
                last_modified = os.path.getmtime(DB_PATH)
        elif items_without_status and is_year_mode:
            # Year mode: For items without any status in this year, find most recent from any year
            for row in items_without_status:
                item_id = row["id"]

                cursor.execute("""
                    SELECT action_status, end_date, end_time
                    FROM week_status
                    WHERE action_id = ?
                    ORDER BY year DESC, week DESC
                    LIMIT 1
                """, (item_id,))
                nearest = cursor.fetchone()

                if nearest:
                    week_status = {
                        "action_status": nearest["action_status"] or "",
                        "end_date": nearest["end_date"] or "",
                        "end_time": nearest["end_time"] or ""
                    }
                    items.append(row_to_dict(row, week_status))
                else:
                    # No status at all - add with empty status
                    items.append(row_to_dict(row, None))
        
        conn.close()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "items": items,
            "current_year": year,
            "current_week": week,
            "data_type": data_type,
            "last_modified": last_modified,
            "db_initialized": True
        }).encode())

    def carry_over_from_previous(self, target_year, target_week, data_type="Incident"):
        """
        Queries SQLite to find the most recent week's data before target_year/target_week.
        Returns non-closed actions for carry-over using normalized schema.
        """
        if not db_exists():
            return []
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Find all distinct year/week combinations, ordered desc
        cursor.execute("""
            SELECT DISTINCT ws.year, ws.week 
            FROM week_status ws
            INNER JOIN items a ON ws.action_id = a.id
            WHERE a.data_type = ?
            ORDER BY ws.year DESC, ws.week DESC
        """, (data_type,))
        
        weeks_available = cursor.fetchall()
        
        for row in weeks_available:
            y, w = row['year'], row['week']
            
            # Must be before target week
            if y > target_year:
                continue
            if y == target_year and w >= target_week:
                continue
            
            # Found a previous week - fetch its items with status
            cursor.execute("""
                SELECT a.*, ws.action_status, ws.end_date, ws.end_time
                FROM items a
                INNER JOIN week_status ws ON a.id = ws.action_id
                WHERE ws.year = ? AND ws.week = ? AND a.data_type = ?
            """, (y, w, data_type))
            
            prev_rows = cursor.fetchall()

            
            if len(prev_rows) == 0:
                continue
            
            # Convert to item dicts and filter out closed ones and items that haven't started yet
            def is_closed(item):
                status = item.get("Action Status", "")
                end_date = item.get("End Date", "")
                return status == "Closed" or (end_date and end_date.strip() and end_date.lower() != "n/a")
            
            def has_started_by_week(item, target_year, target_week):
                """Check if item's start date is on or before the target week end (Saturday)."""
                start_date_str = item.get("Action Start Date", "")
                if not start_date_str or start_date_str == "N/A":
                    return True  # If no start date, assume it has started
                
                try:
                    # Parse start date
                    if "-" in start_date_str:
                        year, month, day = map(int, start_date_str.split("-"))
                    elif "/" in start_date_str:
                        day, month, year = map(int, start_date_str.split("/"))
                    else:
                        return True
                    
                    start_date = datetime(year, month, day)
                    
                    # Calculate target week end (Saturday)
                    # Using Sunday-based week number (matches Python's %U)
                    year_start = datetime(target_year, 1, 1)
                    # First Sunday of the year or Jan 1
                    # Python's %U: first Sunday starts week 1, days before are week 0
                    first_sunday_offset = (7 - year_start.weekday()) % 7  # Days until first Sunday
                    first_sunday = year_start + timedelta(days=first_sunday_offset)
                    
                    # Week end (Saturday) = first_sunday + (target_week * 7) - 1 day
                    # But for week 0, we need special handling
                    if target_week == 0:
                        # Week 0 ends on the Saturday before the first Sunday
                        week_end = first_sunday - timedelta(days=1)
                    else:
                        week_end = first_sunday + timedelta(weeks=target_week, days=-1)
                    
                    return start_date <= week_end
                except Exception:
                    return True
            
            prev_items = []
            for r in prev_rows:
                week_status = {
                    "action_status": r["action_status"],
                    "end_date": r["end_date"],
                    "end_time": r["end_time"]
                }
                prev_items.append(row_to_dict(r, week_status))
            
            # Filter: not closed AND has started by target week
            carried = [a for a in prev_items if not is_closed(a) and has_started_by_week(a, target_year, target_week)]
            
            conn.close()
            return carried
        
        conn.close()
        return []

    def handle_save_data(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        
        year = int(post_data.get("year"))
        week = int(post_data.get("week"))
        data_type = post_data.get("type", "Incident")
        new_item = post_data.get("action")
        old_item_no = post_data.get("old_action_no")
        is_update = post_data.get("is_update", False)
        
        # Ensure database exists
        if not db_exists():
            init_db()

        if data_type == "Miscellaneous":
            self.handle_save_misc_data(year, week, new_item, old_item_no, is_update)
            return
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if data exists for this week; if not, carry over first
        # (This is only relevant for GET, but we do it here too just in case)
        cursor.execute("""
            SELECT COUNT(*) as cnt 
            FROM week_status ws
            INNER JOIN items a ON ws.action_id = a.id
            WHERE ws.year = ? AND ws.week = ? AND a.data_type = ?
        """, (year, week, data_type))
        count = cursor.fetchone()['cnt']
        
        if count == 0 and not is_update:
            # Carry over from previous week only if we are NOT in update mode
            # (If it's an update, the item MUST already exist)
            carried_items = self.carry_over_from_previous(year, week, data_type)
            if carried_items:
                for item in carried_items:
                    item_no = item.get("Action No.", "")
                    
                    # Check if item exists in items table
                    item_id = get_item_id(cursor, data_type, item_no)
                    
                    if not item_id:
                        # Insert new item
                        item_col_names = ["data_type", "action_no"] + CORE_ITEM_FIELDS
                        placeholders = ", ".join(["?"] * len(item_col_names))
                        
                        # Use field mapping
                        item_values = [data_type, item_no]
                        for col in CORE_ITEM_FIELDS:
                            csv_col = DB_TO_CSV.get(col)
                            if csv_col:
                                frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
                                item_values.append(item.get(frontend_key, ""))
                        
                        cursor.execute(
                            f"INSERT INTO items ({', '.join(item_col_names)}) VALUES ({placeholders})",
                            tuple(item_values)
                        )
                        item_id = cursor.lastrowid
                    
                    # Insert week status with normalized time
                    normalized_end_time = normalize_time_to_24h(item.get("Closure Time", ""))
                    cursor.execute("""
                        INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (item_id, year, week,
                          item.get("Action Status", ""),
                          item.get("End Date", ""),
                          normalized_end_time))
        
        # Handle item number change
        new_item_no = new_item.get("Action No.")
        
        if old_item_no and old_item_no != new_item_no:
            # Delete old item and all its week statuses (CASCADE will handle week_status)
            old_item_id = get_item_id(cursor, data_type, old_item_no)
            if old_item_id:
                cursor.execute("DELETE FROM items WHERE id = ?", (old_item_id,))
        
        # Check if item exists
        item_id = get_item_id(cursor, data_type, new_item_no)
        
        # If it's a new item (is_update is False AND no old_item_no) 
        # but the item_no already exists, return conflict
        if not is_update and not old_item_no and item_id:
            conn.close()
            self.send_response(409)
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Action Number already exists"}).encode())
            return

        if item_id:
            # Check if anything changed
            changed = False
            
            # 1. Check core fields
            cursor.execute(f"SELECT {', '.join(CORE_ITEM_FIELDS)} FROM items WHERE id = ?", (item_id,))
            existing_item = cursor.fetchone()
            
            if existing_item:
                for i, col in enumerate(CORE_ITEM_FIELDS):
                    csv_col = DB_TO_CSV.get(col)
                    if csv_col:
                        frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
                        new_val = str(new_item.get(frontend_key, "")).strip()
                        db_val = str(existing_item[i] if existing_item[i] is not None else "").strip()
                        if new_val != db_val:
                            changed = True
                            break
            
            # 2. Check week status
            if not changed:
                cursor.execute("""
                    SELECT action_status, end_date, end_time 
                    FROM week_status 
                    WHERE action_id = ? AND year = ? AND week = ?
                """, (item_id, year, week))
                existing_status = cursor.fetchone()
                
                if not existing_status:
                    changed = True
                else:
                    new_status = str(new_item.get("Action Status", "")).strip()
                    db_status = str(existing_status["action_status"] or "").strip()
                    new_end_date = str(new_item.get("End Date", "")).strip()
                    db_end_date = str(existing_status["end_date"] or "").strip()
                    new_end_time = str(new_item.get("Closure Time", "")).strip()
                    db_end_time = str(existing_status["end_time"] or "").strip()

                    if (new_status != db_status or
                        new_end_date != db_end_date or
                        new_end_time != db_end_time):
                        changed = True
            
            if changed:
                # Update existing item (core fields)
                update_parts = []
                values = []
                for col in CORE_ITEM_FIELDS:
                    csv_col = DB_TO_CSV.get(col)
                    if csv_col:
                        update_parts.append(f"{col} = ?")
                        # Use FRONTEND_TO_BACKEND mapping if necessary
                        frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
                        values.append(new_item.get(frontend_key, ""))
                values.append(item_id)
                
                cursor.execute(
                    f"UPDATE items SET {', '.join(update_parts)} WHERE id = ?",
                    values
                )
                
                # Insert or update week status with normalized time
                normalized_end_time = normalize_time_to_24h(new_item.get("Closure Time", ""))
                cursor.execute("""
                    INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (item_id, year, week,
                      new_item.get("Action Status", ""),
                      new_item.get("End Date", ""),
                      normalized_end_time))

                # Update last modified time
                os.utime(DB_PATH, None)
        else:
            # Insert new item
            item_col_names = ["data_type", "action_no"] + CORE_ITEM_FIELDS
            placeholders = ", ".join(["?"] * len(item_col_names))

            # Use a custom dict to db values conversion to handle field mapping and normalization
            item_values = [data_type, new_item.get("Action No.", "")]
            for col in CORE_ITEM_FIELDS:
                csv_col = DB_TO_CSV.get(col)
                if csv_col:
                    frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
                    val = new_item.get(frontend_key, "")
                    # Normalize time fields to 24-hour format
                    if col in TIME_FIELDS:
                        val = normalize_time_to_24h(val)
                    item_values.append(val)

            cursor.execute(
                f"INSERT INTO items ({', '.join(item_col_names)}) VALUES ({placeholders})",
                tuple(item_values)
            )
            item_id = cursor.lastrowid

            # Insert or update week status with normalized time
            normalized_end_time = normalize_time_to_24h(new_item.get("Closure Time", ""))
            cursor.execute("""
                INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (item_id, year, week,
                  new_item.get("Action Status", ""),
                  new_item.get("End Date", ""),
                  normalized_end_time))
            
            # Update last modified time
            os.utime(DB_PATH, None)
        
        conn.commit()
        conn.close()
        
        self.send_response(200)
        self.end_headers()

    def read_misc_csv(self, file_path):
        if not os.path.exists(file_path):
            return []
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                tasks = []
                for row in reader:
                    for col in MISC_COLUMNS:
                        if col not in row:
                            row[col] = ""
                    tasks.append(row)
                return tasks
        except PermissionError:
            return None

    def handle_export(self, query):
        params = urllib.parse.parse_qs(query)
        year = params.get("year", [""])[0]
        week = params.get("week", [""])[0]
        data_type = params.get("type", ["Incident"])[0]
        range_type = params.get("range", ["week"])[0]

        # Check if database exists
        if not db_exists():
            self.send_error(404, "Database not initialized")
            return

        if data_type == "Miscellaneous":
            conn = get_db_connection()
            cursor = conn.cursor()
            if range_type == "all":
                cursor.execute("""
                    SELECT t.*, ws.status, ws.completed_date, ws.completed_time
                    FROM misc_tasks t
                    LEFT JOIN (
                        SELECT task_id, status, completed_date, completed_time,
                               ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY year DESC, week DESC) as rn
                        FROM misc_week_status
                    ) ws ON t.id = ws.task_id AND ws.rn = 1
                """)
                filename = "All_Miscellaneous.csv"
            else:
                cursor.execute("""
                    SELECT t.*, ws.status, ws.completed_date, ws.completed_time
                    FROM misc_tasks t
                    INNER JOIN misc_week_status ws ON t.id = ws.task_id
                    WHERE ws.year = ? AND ws.week = ?
                """, (int(year), int(week)))
                filename = f"Miscellaneous_Status_{year}_Week_{week}.csv"
            rows = cursor.fetchall()
            actions = []
            for row in rows:
                week_status = {
                    "status": row["status"],
                    "completed_date": row["completed_date"],
                    "completed_time": row["completed_time"],
                }
                task_dict = misc_row_to_dict(row, week_status)
                if range_type != "all":
                    start_date_str = task_dict.get("Created Date", "")
                    if start_date_str and start_date_str != "N/A":
                        start_date = self.parse_date(start_date_str)
                        if start_date:
                            target_year = int(year)
                            year_start = datetime(target_year, 1, 1)
                            first_sunday = year_start + timedelta(days=(7 - year_start.weekday()) % 7)
                            if year_start.weekday() == 6:
                                first_sunday = year_start
                            week_end = first_sunday + timedelta(days=int(week) * 7 - 1)
                            if start_date > week_end:
                                continue
                    if misc_is_closed(task_dict):
                        end_date_str = task_dict.get("Completed Date", "")
                        if end_date_str and end_date_str != "N/A":
                            end_date = self.parse_date(end_date_str)
                            if end_date:
                                end_year, end_week = self.get_week_year(end_date)
                                if end_year != int(year) or end_week != int(week):
                                    continue
                actions.append(task_dict)
            conn.close()
            output = io.StringIO()
            writer = csv.DictWriter(
                output, fieldnames=MISC_COLUMNS, extrasaction='ignore', quoting=csv.QUOTE_ALL
            )
            writer.writeheader()
            writer.writerows(actions)
            csv_content = output.getvalue().encode('utf-8-sig')
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.end_headers()
            self.wfile.write(csv_content)
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        if range_type == "all":
            # For "All Time", get the most recent status for each item
            cursor.execute("""
                SELECT a.*, ws.action_status, ws.end_date, ws.end_time
                FROM items a
                LEFT JOIN (
                    SELECT action_id, action_status, end_date, end_time,
                           ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY year DESC, week DESC) as rn
                    FROM week_status
                ) ws ON a.id = ws.action_id AND ws.rn = 1
                WHERE a.data_type = ?
            """, (data_type,))
            plural_type = EXPORT_PLURAL.get(data_type, f"{data_type}s")
            filename = f"All_{plural_type}.csv"
        else:
            # Query for specific week - only get items that have a status entry for this week
            cursor.execute("""
                SELECT a.*, ws.action_status, ws.end_date, ws.end_time
                FROM items a
                INNER JOIN week_status ws ON a.id = ws.action_id
                WHERE ws.year = ? AND ws.week = ? AND a.data_type = ?
            """, (int(year), int(week), data_type))
            filename = f"{data_type}_Status_{year}_Week_{week}.csv"

        rows = cursor.fetchall()

        # Convert rows to action dictionaries with filtering for "week" mode
        actions = []
        for row in rows:
            week_status = {
                "action_status": row["action_status"],
                "end_date": row["end_date"],
                "end_time": row["end_time"]
            }
            action_dict = row_to_dict(row, week_status)

            # For "week" export, apply the same filtering as the dashboard
            if range_type != "all":
                # Check if item has started by current week
                start_date_str = action_dict.get("Action Start Date", "")
                if start_date_str and start_date_str != "N/A":
                    start_date = self.parse_date(start_date_str)
                    if start_date:
                        # Calculate end of current week (Saturday) in the TARGET year
                        target_year = int(year)
                        year_start = datetime(target_year, 1, 1)
                        first_sunday = year_start + timedelta(days=(7 - year_start.weekday()) % 7)
                        if year_start.weekday() == 6:  # Sunday
                            first_sunday = year_start
                        week_end = first_sunday + timedelta(days=int(week) * 7 - 1)
                        if start_date > week_end:
                            continue  # Skip items that haven't started yet

                # For closed items, only include if closed in current week
                action_status = action_dict.get("Action Status", "")
                end_date_str = action_dict.get("End Date", "")
                is_closed = action_status == "Closed" or (end_date_str and end_date_str.strip() and end_date_str != "N/A")

                if is_closed and end_date_str and end_date_str != "N/A":
                    end_date = self.parse_date(end_date_str)
                    if end_date:
                        end_year, end_week = self.get_week_year(end_date)
                        if end_year != int(year) or end_week != int(week):
                            continue  # Skip closed items not closed in current week

            actions.append(action_dict)

        conn.close()
        
        # Generate CSV in-memory
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=COLUMNS,
            extrasaction='ignore',
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(actions)
        
        # Get the CSV content as bytes with BOM for Excel compatibility
        csv_content = output.getvalue().encode('utf-8-sig')
            
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(csv_content)

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

def migrate_existing_files():
    """Renames any Week_N.csv files to Incident_Week_N.csv."""
    if not os.path.exists(DATA_DIR):
        return
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.startswith("Week_") and file.endswith(".csv"):
                old_path = os.path.join(root, file)
                new_path = os.path.join(root, f"Incident_{file}")
                if not os.path.exists(new_path):
                    print(f"Migrating {old_path} to {new_path}")
                    os.rename(old_path, new_path)

if __name__ == "__main__":
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    migrate_existing_files()
    ensure_schema_migrations()
    
    with ThreadingTCPServer(("", PORT), CrisisHandler) as httpd:
        print(f"Serving Crisis Hub at http://localhost:{PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass

