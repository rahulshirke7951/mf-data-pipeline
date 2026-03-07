from datetime import datetime
import sqlite3
import sys
import re

DB_NAME = "mf.db"

# --------------------------------------------------
# Create database table
# --------------------------------------------------
def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS nav_history(
        scheme_code TEXT,
        amc_name TEXT,
        scheme_category TEXT,
        isin_div_payout_growth TEXT,
        isin_div_reinvestment TEXT,
        scheme_name TEXT,
        nav REAL,
        nav_date TEXT,
        PRIMARY KEY (scheme_code, nav_date)
    )
    """)

    conn.commit()


# --------------------------------------------------
# Helper: detect scheme row
# --------------------------------------------------
def is_scheme_row(line):
    return bool(re.match(r'^\d+;', line))


# --------------------------------------------------
# Parse AMFI NAV file
# --------------------------------------------------
def parse_and_insert(file_name):

    conn = sqlite3.connect(DB_NAME)
    create_tables(conn)
    cur = conn.cursor()

    current_amc = None
    current_category = None

    inserted = 0
    skipped = 0

    with open(file_name, encoding="utf-8") as f:
        for raw_line in f:

            line = raw_line.strip()

            # ----------------------------
            # Skip empty lines
            # ----------------------------
            if not line:
                continue

            # ----------------------------
            # CATEGORY DETECTION
            # Matches:
            # Open Ended Schemes(...)
            # Close Ended Schemes(...)
            # Interval Fund Schemes(...)
            # ----------------------------
            if "Schemes(" in line:
                current_category = line
                continue

            # ----------------------------
            # AMC DETECTION
            # Matches:
            # SBI Mutual Fund
            # Nippon India Mutual Fund
            # ----------------------------
            if "Mutual Fund" in line and not is_scheme_row(line):
                current_amc = line
                continue

            # ----------------------------
            # SCHEME NAV ROW
            # ----------------------------
            if is_scheme_row(line):

                parts = line.split(";")

                if len(parts) < 6:
                    skipped += 1
                    continue

                try:
                    scheme_code = parts[0]
                    isin_div_payout_growth = None if parts[1] == "-" else parts[1]
                    isin_div_reinvestment = None if parts[2] == "-" else parts[2]
                    scheme_name = parts[3]
                    nav = float(parts[4])
                    
                    raw_nav_date = parts[5]
                    
                    try:
                        parsed_date = datetime.strptime(raw_nav_date, "%d-%b-%Y")
                        nav_date = parsed_date.strftime("%Y-%m-%d")
                    except ValueError:
                        skipped += 1
                        continue
                    
                    cur.execute("""
                    INSERT OR IGNORE INTO nav_history
                    VALUES (?,?,?,?,?,?,?,?)
                    """, (
                        scheme_code,
                        current_amc,
                        current_category,
                        isin_div_payout_growth,
                        isin_div_reinvestment,
                        scheme_name,
                        nav,
                        nav_date
                    ))

                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1

                except Exception:
                    skipped += 1

    conn.commit()
    conn.close()

    print("✅ Inserted rows:", inserted)
    print("⚠️ Skipped rows:", skipped)


# --------------------------------------------------
# Run script
# --------------------------------------------------
if __name__ == "__main__":
    parse_and_insert(sys.argv[1])
