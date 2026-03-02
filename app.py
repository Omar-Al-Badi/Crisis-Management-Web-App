import http.server
import socketserver
import json
import csv
import os
import io
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
    "Start Time", "Target Date", "Remarks", "Actions", "End Date", "End Time", 
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
    "Remarks": "remarks",
    "Actions": "actions_field",
    "End Date": "end_date",
    "End Time": "end_time",
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

    # Migration: Check if hidden column exists, if not add it
    try:
        cursor.execute("SELECT hidden FROM items LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding hidden column...")
        cursor.execute("ALTER TABLE items ADD COLUMN hidden INTEGER DEFAULT 0")

    # Migration: Check if TTA, TTR, TTD columns exist, if not add them
    try:
        cursor.execute("SELECT time_to_acknowledge FROM items LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating database: Adding TTA, TTR, TTD columns...")
        cursor.execute("ALTER TABLE items ADD COLUMN time_to_acknowledge TEXT")
        cursor.execute("ALTER TABLE items ADD COLUMN time_to_recover TEXT")
        cursor.execute("ALTER TABLE items ADD COLUMN time_to_detect TEXT")
    
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


DATE_FIELDS = {"action_start_date", "target_date", "target_date_last_update"}

TIME_FIELDS = {"start_time", "end_time"}


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
        item["End Time"] = week_status_row["end_time"] if week_status_row["end_time"] else ""
    else:
        item["Action Status"] = ""
        item["End Date"] = ""
        item["End Time"] = ""
    
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


def get_item_id(cursor, data_type, action_no):
    """Get the action_id for a given data_type and action_no."""
    cursor.execute(
        "SELECT id FROM items WHERE data_type = ? AND action_no = ?",
        (data_type, action_no)
    )
    row = cursor.fetchone()
    return row[0] if row else None


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
        elif parsed_path.path == "/api/show-all-rows":
            self.handle_show_all_rows()
        elif parsed_path.path == "/api/import":
            self.handle_import()
        elif parsed_path.path == "/api/regularize":
            self.handle_regularize()
        elif parsed_path.path == "/api/migrate_schema":
            self.handle_migrate_schema()
        else:
            self.send_error(404)

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

            if file_content is None or year is None or week is None:
                missing = []
                if file_content is None: missing.append("file")
                if year is None: missing.append("year")
                if week is None: missing.append("week")
                
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": f"Missing required fields: {', '.join(missing)}"}).encode())
                return

            # Parse CSV
            try:
                csv_text = file_content.decode('utf-8-sig')
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
                
            f = io.StringIO(csv_text)
            reader = csv.DictReader(f)
            
            new_count = 0
            updated_count = 0
            skipped_count = 0
            error_count = 0
            
            # Ensure database exists
            if not db_exists():
                init_db()
                
            conn = get_db_connection()
            cursor = conn.cursor()
            
            for row in reader:
                try:
                    action_no = row.get("Action No.")
                    if not action_no:
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
                              row.get("End Time", "")))
                        
                        new_count += 1
                    else:
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
                                    csv_val = str(row.get(frontend_key, "")).strip()
                                    db_val = str(existing_item[i] if existing_item[i] is not None else "").strip()
                                    if csv_val != db_val:
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
                                csv_status = str(row.get("Action Status", "")).strip()
                                db_status = str(existing_status["action_status"] or "").strip()
                                csv_end_date = str(row.get("End Date", "")).strip()
                                db_end_date = str(existing_status["end_date"] or "").strip()
                                csv_end_time = str(row.get("End Time", "")).strip()
                                db_end_time = str(existing_status["end_time"] or "").strip()

                                if (csv_status != db_status or
                                    csv_end_date != db_end_date or
                                    csv_end_time != db_end_time):
                                    changed = True
                        
                        if changed:
                            # Update existing item core fields
                            update_parts = []
                            values = []
                            for col in CORE_ITEM_FIELDS:
                                csv_col = DB_TO_CSV.get(col)
                                if csv_col:
                                    update_parts.append(f"{col} = ?")
                                    frontend_key = BACKEND_TO_FRONTEND.get(col, csv_col)
                                    val = row.get(frontend_key, "")
                                    if col in DATE_FIELDS:
                                        val = normalize_date(val)
                                    values.append(val)
                            values.append(item_id)
                            cursor.execute(
                                f"UPDATE items SET {', '.join(update_parts)} WHERE id = ?",
                                values
                            )
                            
                            # Insert or update week status
                            cursor.execute("""
                                INSERT OR REPLACE INTO week_status (action_id, year, week, action_status, end_date, end_time)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (item_id, year, week,
                                  row.get("Action Status", ""),
                                  normalize_date(row.get("End Date", "")),
                                  row.get("End Time", "")))
                            
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
                "errors": error_count
            }).encode())
            
        except Exception as e:
            print(f"Import failed: {e}")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": f"Import failed: {str(e)}"}).encode())

    def handle_show_all_rows(self):
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        
        data_type = post_data.get("type", "Incident")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE items SET hidden = 0 WHERE data_type = ?",
            (data_type,)
        )
        conn.commit()
        conn.close()
        
        self.send_response(200)
        self.end_headers()

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
                if (file.startswith("Incident_Week_") or file.startswith("Crisis_Week_") or file.startswith("Action_Week_")) and file.endswith(".csv"):
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
                        data_type = parts[0]  # Incident, Crisis, or Action
                        week = int(parts[1])
                        
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
                                normalized_end_time = normalize_time_to_24h(action.get("End Time", ""))
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
                    normalized_end_time = normalize_time_to_24h(item.get("End Time", ""))
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
                    new_end_time = str(new_item.get("End Time", "")).strip()
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
                normalized_end_time = normalize_time_to_24h(new_item.get("End Time", ""))
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
            normalized_end_time = normalize_time_to_24h(new_item.get("End Time", ""))
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
            # Handle pluralization properly (Crisis -> Crises, Incident -> Incidents)
            plural_type = "Crises" if data_type == "Crisis" else f"{data_type}s"
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
    
    with ThreadingTCPServer(("", PORT), CrisisHandler) as httpd:
        print(f"Serving Crisis Hub at http://localhost:{PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass

