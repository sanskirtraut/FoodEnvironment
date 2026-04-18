"""Patch Census_Tract.median_income from ACS DP03 CSV."""
import csv
import os
import sqlite3

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "food_env.db")
DP03_CSV = os.path.join(os.path.dirname(BASE), "data", "georgia_income.csv")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

updates = []
with open(DP03_CSV, encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        geo = row.get("GEO_ID", "")
        if not geo.startswith("1400000US"):
            continue
        # Only total population group (001) to avoid duplicate rows
        if row.get("POPGROUP", "001") != "001":
            continue
        tract_id = geo[-11:]
        try:
            income = int(row["DP03_0062E"])
        except (ValueError, KeyError):
            continue
        updates.append((income, tract_id))

cur.executemany(
    "UPDATE Census_Tract SET median_income = ? WHERE tract_id = ?",
    updates,
)
conn.commit()

filled = conn.execute(
    "SELECT COUNT(*) FROM Census_Tract WHERE median_income IS NOT NULL"
).fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM Census_Tract").fetchone()[0]
print(f"DP03 rows processed : {len(updates)}")
print(f"Income filled       : {filled} / {total} tracts")

# Sample
sample = conn.execute(
    "SELECT tract_id, county, median_income FROM Census_Tract WHERE median_income IS NOT NULL ORDER BY median_income DESC LIMIT 5"
).fetchall()
print("\nTop 5 by median income:")
for r in sample:
    print(f"  {r[0]}  {r[1]}  ${r[2]:,}")
conn.close()
