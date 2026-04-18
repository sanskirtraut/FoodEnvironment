"""Patch Census_Tract.population from ACS DP05 CSV."""
import csv
import os
import sqlite3

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "food_env.db")
ACS_CSV = os.path.join(os.path.dirname(BASE), "data", "georgia_popl.csv")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

updates = []
with open(ACS_CSV, encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        geo = row.get("GEO_ID", "")
        if not geo.startswith("1400000US"):
            continue  # skip header label row
        tract_id = geo[-11:]          # last 11 digits = state(2) + county(3) + tract(6)
        pop_raw = row.get("DP05_0001E", "")
        try:
            pop = int(pop_raw)
        except ValueError:
            continue
        updates.append((pop, tract_id))

cur.executemany(
    "UPDATE Census_Tract SET population = ? WHERE tract_id = ?",
    updates,
)
conn.commit()

changed = cur.rowcount  # last batch only; use a count query instead
total_updated = conn.execute(
    "SELECT COUNT(*) FROM Census_Tract WHERE population IS NOT NULL"
).fetchone()[0]
total_tracts = conn.execute("SELECT COUNT(*) FROM Census_Tract").fetchone()[0]

print(f"ACS rows processed : {len(updates)}")
print(f"Tracts with population now : {total_updated} / {total_tracts}")
conn.close()
