"""Build food_env.db from CDC PLACES CSV + Georgia OSM JSON files."""
import csv
import json
import math
import os
import re
import sqlite3
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.dirname(BASE)
DB_PATH = os.path.join(BASE, "food_env.db")
SCHEMA = os.path.join(BASE, "schema.sql")

CSV_PATH = os.path.join(DATA, "PLACES__Local_Data_for_Better_Health,_Census_Tract_Data,_2025_release_20260406.csv")
FASTFOOD_JSON    = os.path.join(DATA, "georgia_fastfood.json")
SUPERMARKET_JSON = os.path.join(DATA, "georgia_supermarket.json")
FOODBANK_JSON    = os.path.join(DATA, "georgia_foodbank.json")
CONVENIENCE_JSON = os.path.join(DATA, "georgia_convenience.json")
ACS_CSV = os.path.join(os.path.dirname(DATA), "ACSDP5Y2021.DP05-Data.csv")
DP03_CSV = os.path.join(DATA, "ACSDP5YSPT2021.DP03-Data.csv")

# CSV size ~867MB. csv.field_size_limit may need bumping.
csv.field_size_limit(10**7)

POINT_RE = re.compile(r"POINT\s*\(\s*(-?\d+\.\d+)\s+(-?\d+\.\d+)\s*\)")


def load_places(conn):
    """Stream CDC PLACES CSV, insert Georgia tracts + obesity stats."""
    cur = conn.cursor()
    tracts = {}  # tract_id -> (population, county, state, lat, lon)
    obesity = []  # (rate, year, tract_id)
    income = {}  # tract_id -> median income placeholder (not in PLACES; keep None)

    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("StateAbbr") != "GA":
                continue
            tract = row.get("LocationName", "").strip()
            if not tract or len(tract) != 11:
                continue
            # Capture tract metadata once
            if tract not in tracts:
                pop = row.get("TotalPopulation") or None
                try:
                    pop = int(pop) if pop else None
                except ValueError:
                    pop = None
                geo = row.get("Geolocation", "")
                lat = lon = None
                m = POINT_RE.search(geo)
                if m:
                    lon = float(m.group(1))
                    lat = float(m.group(2))
                tracts[tract] = (pop, row.get("CountyName"), "GA", lat, lon)
            # Obesity rate
            if row.get("MeasureId") == "OBESITY" and row.get("Data_Value_Type") == "Crude prevalence":
                try:
                    rate = float(row["Data_Value"])
                    year = int(row["Year"])
                    obesity.append((rate, year, tract))
                except (ValueError, KeyError, TypeError):
                    pass

    print(f"  GA tracts: {len(tracts)}  obesity rows: {len(obesity)}")

    cur.executemany(
        "INSERT INTO Census_Tract (tract_id, population, median_income, county, state, latitude, longitude) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(tid, t[0], None, t[1], t[2], t[3], t[4]) for tid, t in tracts.items()],
    )
    cur.executemany(
        "INSERT INTO Obesity_Statistic (obesity_rate, year_recorded, tract_id) VALUES (?, ?, ?)",
        obesity,
    )
    conn.commit()
    return tracts


def nearest_tract(lat, lon, tract_points):
    """Find nearest tract_id by haversine-equivalent squared distance (small-scale approximation)."""
    best_id = None
    best_d = float("inf")
    coslat = math.cos(math.radians(lat))
    for tid, (tlat, tlon) in tract_points:
        dx = (tlon - lon) * coslat
        dy = (tlat - lat)
        d = dx * dx + dy * dy
        if d < best_d:
            best_d = d
            best_id = tid
    return best_id


def load_establishments(conn, tracts):
    cur = conn.cursor()
    cur.execute("INSERT INTO Category_Type (category_name, health_score) VALUES (?, ?)", ("Fast Food", 2))
    ff_id = cur.lastrowid
    cur.execute("INSERT INTO Category_Type (category_name, health_score) VALUES (?, ?)", ("Supermarket", 9))
    sm_id = cur.lastrowid
    cur.execute("INSERT INTO Category_Type (category_name, health_score) VALUES (?, ?)", ("Food Bank", 8))
    fb_id = cur.lastrowid
    cur.execute("INSERT INTO Category_Type (category_name, health_score) VALUES (?, ?)", ("Convenience Store", 3))
    cv_id = cur.lastrowid

    tract_points = [(tid, (t[3], t[4])) for tid, t in tracts.items() if t[3] is not None and t[4] is not None]

    def parse_file(path, type_id):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows = []
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name") or tags.get("brand") or "(unnamed)"
            if el.get("type") == "node":
                lat = el.get("lat")
                lon = el.get("lon")
            else:
                c = el.get("center") or {}
                lat = c.get("lat")
                lon = c.get("lon")
            if lat is None or lon is None:
                continue
            street = " ".join(
                v for v in [tags.get("addr:housenumber"), tags.get("addr:street")] if v
            )
            city = tags.get("addr:city", "")
            state = tags.get("addr:state", "")
            zipcode = tags.get("addr:postcode", "")
            address = ", ".join(p for p in [street, city, state, zipcode] if p)
            tract_id = nearest_tract(lat, lon, tract_points) if tract_points else None
            rows.append((name, lat, lon, address, zipcode, tract_id, type_id))
        return rows

    print("  Parsing fast food...")
    ff_rows = parse_file(FASTFOOD_JSON, ff_id)
    print(f"    {len(ff_rows)} fast food")
    print("  Parsing supermarkets...")
    sm_rows = parse_file(SUPERMARKET_JSON, sm_id)
    print(f"    {len(sm_rows)} supermarkets")
    print("  Parsing food banks...")
    fb_rows = parse_file(FOODBANK_JSON, fb_id) if os.path.exists(FOODBANK_JSON) else []
    print(f"    {len(fb_rows)} food banks")
    print("  Parsing convenience stores...")
    cv_rows = parse_file(CONVENIENCE_JSON, cv_id) if os.path.exists(CONVENIENCE_JSON) else []
    print(f"    {len(cv_rows)} convenience stores")

    cur.executemany(
        "INSERT INTO Food_Establishment (name, latitude, longitude, address, zipcode, tract_id, type_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ff_rows + sm_rows + fb_rows + cv_rows,
    )
    conn.commit()


def build_competes(conn, radius_km=1.0):
    """Pairs of nearby establishments within radius (used for clustering analysis)."""
    cur = conn.cursor()
    cur.execute("SELECT store_id, latitude, longitude, tract_id FROM Food_Establishment")
    stores = cur.fetchall()
    # Group by tract for efficiency
    buckets = {}
    for s in stores:
        buckets.setdefault(s[3], []).append(s)
    pairs = []
    r2 = (radius_km / 111.0) ** 2  # rough degrees squared
    for tid, group in buckets.items():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                coslat = math.cos(math.radians(a[1]))
                dx = (a[2] - b[2]) * coslat
                dy = a[1] - b[1]
                if dx * dx + dy * dy <= r2:
                    pairs.append((a[0], b[0]))
    print(f"  Competes pairs: {len(pairs)}")
    cur.executemany("INSERT OR IGNORE INTO Competes_With (store_1_id, store_2_id) VALUES (?, ?)", pairs)
    conn.commit()


def patch_population(conn):
    """Update Census_Tract.population from ACS DP05 CSV."""
    if not os.path.exists(ACS_CSV):
        print(f"  ACS CSV not found, skipping population patch: {ACS_CSV}")
        return
    cur = conn.cursor()
    updates = []
    with open(ACS_CSV, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geo = row.get("GEO_ID", "")
            if not geo.startswith("1400000US"):
                continue
            tract_id = geo[-11:]
            try:
                pop = int(row.get("DP05_0001E", ""))
            except ValueError:
                continue
            updates.append((pop, tract_id))
    cur.executemany("UPDATE Census_Tract SET population = ? WHERE tract_id = ?", updates)
    conn.commit()
    filled = conn.execute("SELECT COUNT(*) FROM Census_Tract WHERE population IS NOT NULL").fetchone()[0]
    print(f"  Population filled: {filled} tracts")


def patch_income(conn):
    """Update Census_Tract.median_income from ACS DP03 CSV."""
    if not os.path.exists(DP03_CSV):
        print(f"  DP03 CSV not found, skipping income patch: {DP03_CSV}")
        return
    cur = conn.cursor()
    updates = []
    with open(DP03_CSV, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geo = row.get("GEO_ID", "")
            if not geo.startswith("1400000US"):
                continue
            if row.get("POPGROUP", "001") != "001":
                continue
            tract_id = geo[-11:]
            try:
                income = int(row["DP03_0062E"])
            except (ValueError, KeyError):
                continue
            updates.append((income, tract_id))
    cur.executemany("UPDATE Census_Tract SET median_income = ? WHERE tract_id = ?", updates)
    conn.commit()
    filled = conn.execute("SELECT COUNT(*) FROM Census_Tract WHERE median_income IS NOT NULL").fetchone()[0]
    print(f"  Income filled: {filled} tracts")


def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    with open(SCHEMA, "r") as f:
        conn.executescript(f.read())
    print("Loading PLACES CSV...")
    tracts = load_places(conn)
    print("Patching population from ACS...")
    patch_population(conn)
    print("Patching median income from ACS DP03...")
    patch_income(conn)
    print("Loading establishments...")
    load_establishments(conn, tracts)
    print("Building Competes_With...")
    build_competes(conn)
    print(f"Done: {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    main()
