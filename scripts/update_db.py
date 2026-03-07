import sqlite3
import sys
import re
from datetime import datetime

DB_NAME = "mf.db"

def create_tables(conn):
    """Initializes the SQLite table with a composite Primary Key."""
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

def is_scheme_row(line):
    """Checks if the line starts with a numeric Scheme Code."""
    return bool(re.match(r'^\d+;', line))

def parse_and_insert(file_name):
    conn = sqlite3.connect(DB_NAME)
    create_tables(conn)
    cur = conn.cursor()

    # Default placeholders in case a file is malformed at the top
    current_amc = "Unknown AMC"
    current_category = "Unknown Category"
    
    inserted = 0
    skipped = 0

    try:
        # 'utf-8-sig' handles the BOM (Byte Order Mark) often found in Windows-generated text files
        with open(file_name, 'r', encoding="utf-8-sig") as f:
            for line_num, raw_line in enumerate(f, 1):
                line = raw_line.strip()

                # 1. Skip empty lines or header labels
                if not line or "Scheme Code" in line:
                    continue

                # 2. Detect Category (e.g., Open Ended Schemes)
                if "Schemes(" in line:
                    current_category = line
                    continue

                # 3. Detect AMC (e.g., SBI Mutual Fund)
                if "Mutual Fund" in line and not is_scheme_row(line):
                    current_amc = line
                    continue

                # 4. Parse Scheme Data Row
                if is_scheme_row(line):
                    parts = [p.strip() for p in line.split(";")]

                    # Ensure we have enough columns (Code, ISIN1, ISIN2, Name, NAV, Date)
                    if len(parts) < 6:
                        skipped += 1
                        continue

                    try:
                        scheme_code = parts[0]
                        isin_growth = None if parts[1] in ("-", "") else parts[1]
                        isin_reinv  = None if parts[2] in ("-", "") else parts[2]
                        scheme_name = parts[3]
                        
                        # --- NAV PROTECTION ---
                        nav_str = parts[4].replace(",", "")
                        if nav_str.upper() == "N/A" or not nav_str:
                            skipped += 1
                            continue
                        nav = float(nav_str)

                        # --- DATE PROTECTION (Explicit) ---
                        raw_nav_date = parts[5]
                        if not raw_nav_date:
                            skipped += 1
                            continue

                        # Convert "01-Jan-2024" to "2024-01-01" for SQL sorting
                        try:
                            parsed_date = datetime.strptime(raw_nav_date, "%d-%b-%Y")
                            nav_date = parsed_date.strftime("%Y-%m-%d")
                        except ValueError:
                            skipped += 1
                            continue

                        # Using REPLACE so if you re-run the same file, it updates the record
                        cur.execute("""
                        INSERT OR REPLACE INTO nav_history 
                        VALUES (?,?,?,?,?,?,?,?)
                        """, (
                            scheme_code, current_amc, current_category,
                            isin_growth, isin_reinv, scheme_name, nav, nav_date
                        ))
                        inserted += 1

                    except Exception:
                        skipped += 1
                        continue

        conn.commit()
        print("-" * 30)
        print(f"📊 Processing Complete for: {file_name}")
        print(f"✅ Records Inserted/Updated: {inserted}")
        print(f"⚠️ Records Skipped:          {skipped}")
        print("-" * 30)

    except FileNotFoundError:
        print(f"❌ Error: The file '{file_name}' was not found.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <your_amfi_file.txt>")
    else:
        parse_and_insert(sys.argv[1])
