import sqlite3
import sys

DB_NAME = "mf.db"

# -----------------------------
# Create table
# -----------------------------
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


# -----------------------------
# Parse NAV file + insert data
# -----------------------------
def parse_and_insert(file_name):

    conn = sqlite3.connect(DB_NAME)
    create_tables(conn)
    cur = conn.cursor()

    current_amc = None
    current_category = None

    inserted = 0
    skipped = 0

    with open(file_name, encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            # -----------------------------
            # Category line
            # Example:
            # Open Ended Schemes(Equity Scheme - Focused Fund)
            # -----------------------------
            if (
                "Open Ended" in line
                or "Close Ended" in line
                or "Interval Fund" in line
            ):
                current_category = line
                continue

            # -----------------------------
            # AMC line
            # Example:
            # SBI Mutual Fund
            # -----------------------------
            if "Mutual Fund" in line and ";" not in line:
                current_amc = line
                continue

            # -----------------------------
            # Scheme NAV row
            # -----------------------------
            parts = line.split(";")

            if len(parts) >= 6 and parts[0].isdigit():

                scheme_code = parts[0]
                isin_div_payout_growth = None if parts[1] == "-" else parts[1]
                isin_div_reinvestment = None if parts[2] == "-" else parts[2]
                scheme_name = parts[3]
                nav = parts[4]
                nav_date = parts[5]

                try:
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

    print("Inserted rows:", inserted)
    print("Skipped rows:", skipped)


# -----------------------------
# Run script
# -----------------------------
if __name__ == "__main__":
    parse_and_insert(sys.argv[1])
