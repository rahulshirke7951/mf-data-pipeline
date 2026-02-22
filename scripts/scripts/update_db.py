import sqlite3
import sys

DB_NAME = "mf.db"

def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS nav_history(
        scheme_code TEXT,
        scheme_name TEXT,
        nav REAL,
        nav_date TEXT,
        PRIMARY KEY (scheme_code, nav_date)
    )
    """)

    conn.commit()

def parse_and_insert(file_name):

    conn = sqlite3.connect(DB_NAME)
    create_tables(conn)
    cur = conn.cursor()

    inserted = 0

    with open(file_name, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(";")

            if len(parts) >= 6 and parts[0].isdigit():

                try:
                    cur.execute("""
                    INSERT OR IGNORE INTO nav_history
                    VALUES (?,?,?,?)
                    """, (parts[0], parts[3], parts[4], parts[5]))

                    if cur.rowcount > 0:
                        inserted += 1

                except:
                    pass

    conn.commit()
    conn.close()

    print("Inserted rows:", inserted)

if __name__ == "__main__":
    parse_and_insert(sys.argv[1])
