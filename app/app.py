"""Food Environment Tracker — Flask backend."""
import math
import os
import sqlite3

from flask import Flask, g, jsonify, render_template, request

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "food_env.db")

app = Flask(__name__, template_folder="templates", static_folder="static")


def db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
        g.db.set_trace_callback(print)   # ← prints every SQL to the terminal
    return g.db


@app.teardown_appcontext
def close_db(exc):
    conn = g.pop("db", None)
    if conn:
        conn.close()


@app.route("/")
def index():
    return render_template("index.html")


# ---------- Reference data ----------
@app.route("/api/categories")
def categories():
    rows = db().execute("SELECT type_id, category_name, health_score FROM Category_Type").fetchall()
    return jsonify([dict(r) for r in rows])


# ---------- CRUD: Food_Establishment ----------
@app.route("/api/establishments", methods=["GET"])
def list_establishments():
    """Search establishments. Filters: zip, name, type_id, tract_id. Limit default 200."""
    q = """
        SELECT e.store_id, e.name, e.latitude, e.longitude, e.address, e.zipcode,
               e.fresh_food, e.tract_id, c.category_name, c.health_score
        FROM Food_Establishment e
        JOIN Category_Type c ON e.type_id = c.type_id
        WHERE 1=1
    """
    args = []
    if zipc := request.args.get("zip"):
        q += " AND e.zipcode = ?"
        args.append(zipc)
    if name := request.args.get("name"):
        q += " AND e.name LIKE ?"
        args.append(f"%{name}%")
    if tid := request.args.get("type_id"):
        q += " AND e.type_id = ?"
        args.append(tid)
    if tract := request.args.get("tract_id"):
        q += " AND e.tract_id = ?"
        args.append(tract)
    q += " ORDER BY c.category_name, e.name LIMIT ?"
    args.append(int(request.args.get("limit", 200)))
    rows = db().execute(q, args).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/establishments", methods=["POST"])
def create_establishment():
    data = request.get_json(force=True)
    required = ["name", "latitude", "longitude", "type_id"]
    if any(k not in data or data[k] in (None, "") for k in required):
        return jsonify({"error": f"Missing required field(s): {required}"}), 400
    conn = db()
    cur = conn.execute(
        "INSERT INTO Food_Establishment (name, latitude, longitude, address, zipcode, fresh_food, tract_id, type_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            data["name"],
            float(data["latitude"]),
            float(data["longitude"]),
            data.get("address", ""),
            data.get("zipcode", ""),
            1 if data.get("fresh_food") else 0,
            data.get("tract_id"),
            int(data["type_id"]),
        ),
    )
    conn.commit()
    return jsonify({"store_id": cur.lastrowid}), 201


@app.route("/api/establishments/<int:sid>", methods=["PATCH"])
def update_establishment(sid):
    data = request.get_json(force=True)
    fields = []
    args = []
    for key in ("name", "address", "zipcode", "tract_id", "type_id", "fresh_food"):
        if key in data:
            fields.append(f"{key} = ?")
            val = data[key]
            if key == "fresh_food":
                val = 1 if val else 0
            args.append(val)
    if not fields:
        return jsonify({"error": "No fields to update"}), 400
    args.append(sid)
    conn = db()
    conn.execute(f"UPDATE Food_Establishment SET {', '.join(fields)} WHERE store_id = ?", args)
    conn.commit()
    return jsonify({"ok": True})


@app.route("/api/establishments/<int:sid>", methods=["DELETE"])
def delete_establishment(sid):
    conn = db()
    conn.execute("DELETE FROM Competes_With WHERE store_1_id = ? OR store_2_id = ?", (sid, sid))
    conn.execute("DELETE FROM Food_Establishment WHERE store_id = ?", (sid,))
    conn.commit()
    return jsonify({"ok": True})


# ---------- Analytics: establishments grouped by tract ----------
@app.route("/api/analytics/by-tract")
def analytics_by_tract():
    limit = int(request.args.get("limit", 25))
    rows = db().execute(
        """
        SELECT t.tract_id, t.county, t.population, t.median_income,
               SUM(CASE WHEN c.health_score >= 5 THEN 1 ELSE 0 END) AS healthy,
               SUM(CASE WHEN c.health_score < 5  THEN 1 ELSE 0 END) AS unhealthy,
               SUM(CASE WHEN c.category_name = 'Fast Food'         THEN 1 ELSE 0 END) AS fast_food,
               SUM(CASE WHEN c.category_name = 'Supermarket'       THEN 1 ELSE 0 END) AS supermarket,
               SUM(CASE WHEN c.category_name = 'Convenience Store' THEN 1 ELSE 0 END) AS convenience,
               SUM(CASE WHEN c.category_name = 'Food Bank'         THEN 1 ELSE 0 END) AS food_bank,
               COUNT(*) AS total
        FROM Food_Establishment e
        JOIN Category_Type c ON e.type_id = c.type_id
        JOIN Census_Tract t ON e.tract_id = t.tract_id
        GROUP BY t.tract_id, t.county
        HAVING total > 0
        ORDER BY fast_food DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return jsonify([dict(r) for r in rows])


# ---------- Advanced: Swamp Severity Calculator ----------

@app.route("/api/counties")
def counties():
    rows = db().execute(
        "SELECT DISTINCT county FROM Census_Tract WHERE county IS NOT NULL ORDER BY county"
    ).fetchall()
    return jsonify([r["county"] for r in rows])


@app.route("/api/swamp-index")
def swamp_index():
    """
    Supports three modes:
      - county=Fulton          → aggregate all establishments in that county
      - zipcode=30303          → aggregate all establishments with that ZIP
      - tract_id=13121...&radius=3  → legacy radius-based single-tract mode
    """
    conn = db()
    county  = request.args.get("county", "").strip()
    zipcode = request.args.get("zipcode", "").strip()
    tract_id = request.args.get("tract_id", "").strip()

    # ---- County mode ----
    if county:
        exists = conn.execute(
            "SELECT 1 FROM Census_Tract WHERE county = ? LIMIT 1", (county,)
        ).fetchone()
        if not exists:
            return jsonify({"error": f"County '{county}' not found"}), 404

        rows = conn.execute(
            """
            SELECT e.store_id, e.name, e.latitude, e.longitude, c.category_name, c.health_score
            FROM Food_Establishment e
            JOIN Category_Type c ON e.type_id = c.type_id
            JOIN Census_Tract t ON e.tract_id = t.tract_id
            WHERE t.county = ?
            """,
            (county,),
        ).fetchall()

        demo = conn.execute(
            "SELECT COUNT(*) AS tract_count, SUM(population) AS pop, ROUND(AVG(median_income)) AS inc "
            "FROM Census_Tract WHERE county = ?",
            (county,),
        ).fetchone()

        # Average of each tract's most-recent obesity rate
        obesity_rows = conn.execute(
            """
            SELECT AVG(s.obesity_rate) AS avg_rate, MAX(s.year_recorded) AS yr
            FROM Obesity_Statistic s
            JOIN (
                SELECT tract_id, MAX(year_recorded) AS max_yr
                FROM Obesity_Statistic
                GROUP BY tract_id
            ) latest ON s.tract_id = latest.tract_id AND s.year_recorded = latest.max_yr
            JOIN Census_Tract t ON s.tract_id = t.tract_id
            WHERE t.county = ?
            """,
            (county,),
        ).fetchone()

        area_name   = f"{county} County"
        search_mode = "county"
        tract_count = demo["tract_count"] if demo else 0
        population  = demo["pop"] if demo else None
        median_income = int(demo["inc"]) if demo and demo["inc"] else None

    # ---- ZIP code mode ----
    elif zipcode:
        rows = conn.execute(
            """
            SELECT e.store_id, e.name, e.latitude, e.longitude, c.category_name, c.health_score
            FROM Food_Establishment e
            JOIN Category_Type c ON e.type_id = c.type_id
            WHERE e.zipcode = ?
            """,
            (zipcode,),
        ).fetchall()
        if not rows:
            return jsonify({"error": f"No establishments found for ZIP {zipcode}"}), 404

        demo = conn.execute(
            """
            SELECT COUNT(DISTINCT t.tract_id) AS tract_count,
                   SUM(DISTINCT t.population) AS pop,
                   ROUND(AVG(DISTINCT t.median_income)) AS inc
            FROM Census_Tract t
            JOIN Food_Establishment e ON e.tract_id = t.tract_id
            WHERE e.zipcode = ?
            """,
            (zipcode,),
        ).fetchone()

        obesity_rows = conn.execute(
            """
            SELECT AVG(s.obesity_rate) AS avg_rate, MAX(s.year_recorded) AS yr
            FROM Obesity_Statistic s
            JOIN (
                SELECT tract_id, MAX(year_recorded) AS max_yr
                FROM Obesity_Statistic
                GROUP BY tract_id
            ) latest ON s.tract_id = latest.tract_id AND s.year_recorded = latest.max_yr
            WHERE s.tract_id IN (
                SELECT DISTINCT tract_id FROM Food_Establishment WHERE zipcode = ?
            )
            """,
            (zipcode,),
        ).fetchone()

        area_name   = f"ZIP {zipcode}"
        search_mode = "zip"
        tract_count = demo["tract_count"] if demo else 0
        population  = demo["pop"] if demo else None
        median_income = int(demo["inc"]) if demo and demo["inc"] else None

    # ---- Legacy tract + radius mode ----
    elif tract_id:
        try:
            radius_km = float(request.args.get("radius", 3.0))
        except ValueError:
            radius_km = 3.0

        center = conn.execute(
            "SELECT tract_id, county, population, median_income, latitude, longitude "
            "FROM Census_Tract WHERE tract_id = ?",
            (tract_id,),
        ).fetchone()
        if not center or center["latitude"] is None:
            return jsonify({"error": "Unknown tract or missing coordinates"}), 404

        lat0, lon0 = center["latitude"], center["longitude"]
        dlat = radius_km / 111.0
        dlon = radius_km / (111.0 * max(math.cos(math.radians(lat0)), 1e-6))

        rows = conn.execute(
            """
            SELECT e.store_id, e.name, e.latitude, e.longitude, c.category_name, c.health_score
            FROM Food_Establishment e
            JOIN Category_Type c ON e.type_id = c.type_id
            WHERE e.latitude BETWEEN ? AND ? AND e.longitude BETWEEN ? AND ?
            """,
            (lat0 - dlat, lat0 + dlat, lon0 - dlon, lon0 + dlon),
        ).fetchall()

        r2 = radius_km * radius_km
        coslat = math.cos(math.radians(lat0))
        filtered = []
        for r in rows:
            dx = (r["longitude"] - lon0) * 111.0 * coslat
            dy = (r["latitude"]  - lat0) * 111.0
            if dx * dx + dy * dy <= r2:
                filtered.append(r)
        rows = filtered

        obesity_row = conn.execute(
            "SELECT obesity_rate, year_recorded FROM Obesity_Statistic "
            "WHERE tract_id = ? ORDER BY year_recorded DESC LIMIT 1",
            (tract_id,),
        ).fetchone()
        obesity_rows = {"avg_rate": obesity_row["obesity_rate"] if obesity_row else None,
                        "yr": obesity_row["year_recorded"] if obesity_row else None}

        area_name   = f"Tract {tract_id} — {center['county']} County"
        search_mode = "tract"
        tract_count = 1
        population  = center["population"]
        median_income = center["median_income"]

    else:
        return jsonify({"error": "Provide county, zipcode, or tract_id"}), 400

    # ---- Aggregate counts (shared across all modes) ----
    healthy = unhealthy = 0
    sample = []
    for r in rows:
        if r["health_score"] >= 5:
            healthy += 1
        else:
            unhealthy += 1
        if len(sample) < 25:
            sample.append(dict(r))

    rfei = round(unhealthy / healthy, 2) if healthy > 0 else None
    avg_obesity = obesity_rows["avg_rate"] if obesity_rows else None
    obesity_year = obesity_rows["yr"] if obesity_rows else None

    return jsonify({
        "area_name":      area_name,
        "search_mode":    search_mode,
        "tract_count":    tract_count,
        "population":     population,
        "median_income":  median_income,
        "healthy_count":  healthy,
        "unhealthy_count": unhealthy,
        "total_count":    healthy + unhealthy,
        "rfei":           rfei,
        "obesity_rate":   round(avg_obesity, 1) if avg_obesity else None,
        "obesity_year":   obesity_year,
        "interpretation": interpret(rfei, avg_obesity),
        "sample":         sample,
    })


def interpret(rfei, obesity):
    if rfei is None:
        return "Food desert: no supermarket in range — all fresh-food access is absent."
    if rfei >= 5:
        level = "severe food swamp"
    elif rfei >= 3:
        level = "food swamp"
    elif rfei >= 1.5:
        level = "unbalanced"
    else:
        level = "balanced"
    msg = f"RFEI = {rfei:.2f} ({level})."
    if obesity is not None:
        msg += f" CDC obesity prevalence: {obesity:.1f}%."
    return msg


# ---------- Tract lookup ----------
@app.route("/api/tracts")
def tracts_search():
    q = request.args.get("q", "").strip()
    args = []
    sql = "SELECT tract_id, county, population FROM Census_Tract WHERE latitude IS NOT NULL"
    if q:
        sql += " AND (tract_id LIKE ? OR county LIKE ?)"
        args += [f"%{q}%", f"%{q}%"]
    sql += " ORDER BY county, tract_id LIMIT 50"
    rows = db().execute(sql, args).fetchall()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    app.run(debug=True, port=5000)
