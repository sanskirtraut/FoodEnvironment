"""Add Food Bank + Convenience Store categories and load their establishments."""
import json
import math
import os
import sqlite3

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.dirname(BASE)  # Project/
DB_PATH = os.path.join(BASE, "food_env.db")

FOODBANK_JSON    = os.path.join(DATA, "georgia_foodbank.json")
CONVENIENCE_JSON = os.path.join(DATA, "georgia_convenience.json")

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# ---------- 1. Insert new Category_Types (skip if already exist) ----------
for name, score in [("Food Bank", 8), ("Convenience Store", 3)]:
    exists = cur.execute(
        "SELECT type_id FROM Category_Type WHERE category_name = ?", (name,)
    ).fetchone()
    if not exists:
        cur.execute(
            "INSERT INTO Category_Type (category_name, health_score) VALUES (?, ?)",
            (name, score),
        )
        print(f"  Added category: {name} (health_score {score})")
    else:
        print(f"  Category already exists: {name}")
conn.commit()

fb_id  = cur.execute("SELECT type_id FROM Category_Type WHERE category_name='Food Bank'").fetchone()[0]
cv_id  = cur.execute("SELECT type_id FROM Category_Type WHERE category_name='Convenience Store'").fetchone()[0]

# ---------- 2. Load tract centroids for nearest-tract assignment ----------
tract_points = [
    (row["tract_id"], row["latitude"], row["longitude"])
    for row in cur.execute(
        "SELECT tract_id, latitude, longitude FROM Census_Tract WHERE latitude IS NOT NULL"
    ).fetchall()
]

def nearest_tract(lat, lon):
    best_id, best_d = None, float("inf")
    coslat = math.cos(math.radians(lat))
    for tid, tlat, tlon in tract_points:
        dx = (tlon - lon) * coslat
        dy = (tlat - lat)
        d = dx * dx + dy * dy
        if d < best_d:
            best_d = d
            best_id = tid
    return best_id

def parse_osm(path, type_id):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for el in data.get("elements", []):
        tags = el.get("tags", {})
        name = tags.get("name") or tags.get("brand") or "(unnamed)"
        if el.get("type") == "node":
            lat, lon = el.get("lat"), el.get("lon")
        else:
            # way with geometry — use centroid of bounds
            b = el.get("bounds", {})
            if b:
                lat = (b["minlat"] + b["maxlat"]) / 2
                lon = (b["minlon"] + b["maxlon"]) / 2
            else:
                geo = el.get("geometry", [])
                if not geo:
                    continue
                lat = sum(g["lat"] for g in geo) / len(geo)
                lon = sum(g["lon"] for g in geo) / len(geo)
        if lat is None or lon is None:
            continue
        street  = " ".join(v for v in [tags.get("addr:housenumber"), tags.get("addr:street")] if v)
        city    = tags.get("addr:city", "")
        state   = tags.get("addr:state", "")
        zipcode = tags.get("addr:postcode", "")
        address = ", ".join(p for p in [street, city, state, zipcode] if p)
        tract_id = nearest_tract(lat, lon)
        rows.append((name, lat, lon, address, zipcode, tract_id, type_id))
    return rows

# ---------- 3. Remove old data for these categories if re-running ----------
for tid in (fb_id, cv_id):
    cur.execute(
        "DELETE FROM Competes_With WHERE store_1_id IN "
        "(SELECT store_id FROM Food_Establishment WHERE type_id=?) OR "
        "store_2_id IN (SELECT store_id FROM Food_Establishment WHERE type_id=?)",
        (tid, tid),
    )
    cur.execute("DELETE FROM Food_Establishment WHERE type_id = ?", (tid,))
conn.commit()

# ---------- 4. Insert establishments ----------
print("  Parsing food banks...")
fb_rows = parse_osm(FOODBANK_JSON, fb_id)
print(f"    {len(fb_rows)} food banks")

print("  Parsing convenience stores...")
cv_rows = parse_osm(CONVENIENCE_JSON, cv_id)
print(f"    {len(cv_rows)} convenience stores")

cur.executemany(
    "INSERT INTO Food_Establishment (name, latitude, longitude, address, zipcode, tract_id, type_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?)",
    fb_rows + cv_rows,
)
conn.commit()

# ---------- 5. Rebuild Competes_With for NEW stores only ----------
print("  Rebuilding Competes_With for new stores...")
new_ids = [
    row[0] for row in cur.execute(
        "SELECT store_id FROM Food_Establishment WHERE type_id IN (?, ?)", (fb_id, cv_id)
    ).fetchall()
]
all_stores = cur.execute(
    "SELECT store_id, latitude, longitude, tract_id FROM Food_Establishment"
).fetchall()

# index by tract for efficiency
buckets = {}
for s in all_stores:
    buckets.setdefault(s[3], []).append(s)

new_id_set = set(new_ids)
r2 = (1.0 / 111.0) ** 2  # 1 km in degrees squared
pairs = []
for tid, group in buckets.items():
    for i in range(len(group)):
        for j in range(i + 1, len(group)):
            a, b = group[i], group[j]
            if a[0] not in new_id_set and b[0] not in new_id_set:
                continue  # already computed between existing stores
            coslat = math.cos(math.radians(a[1]))
            dx = (a[2] - b[2]) * coslat
            dy = a[1] - b[1]
            if dx * dx + dy * dy <= r2:
                s1, s2 = (a[0], b[0]) if a[0] < b[0] else (b[0], a[0])
                pairs.append((s1, s2))

cur.executemany("INSERT OR IGNORE INTO Competes_With (store_1_id, store_2_id) VALUES (?, ?)", pairs)
conn.commit()
print(f"  New Competes_With pairs: {len(pairs)}")

# ---------- Summary ----------
totals = cur.execute(
    "SELECT c.category_name, COUNT(*) as n FROM Food_Establishment e "
    "JOIN Category_Type c ON e.type_id=c.type_id GROUP BY c.category_name"
).fetchall()
print("\nEstablishments by category:")
for row in totals:
    print(f"  {row[0]}: {row[1]}")

conn.close()
