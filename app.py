from flask import Flask, render_template, request, jsonify
import psycopg  # MIGRATION: Changed from psycopg2
import psycopg.rows  # MIGRATION: For dict_row factory (replaces RealDictCursor)
from datetime import datetime
from fin import run_induction

app = Flask(__name__)

def to_bool(val):
    if isinstance(val, bool):
        return val
    return str(val).lower() == "true"

# --- Fetch all depots ---
@app.route("/api/depots", methods=["GET"])
def get_depots():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)  # MIGRATION: row_factory replaces cursor_factory=RealDictCursor
        cur.execute("SELECT depot_id, name, location FROM depot ORDER BY depot_id")
        depots = cur.fetchall()
        return jsonify(depots)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()  # MIGRATION: Explicit close in finally for safety

# --- Add a new depot ---
@app.route("/api/depots/add", methods=["POST"])
def add_depot():
    conn = None
    try:
        data = request.get_json()
        if not data or not data.get("name") or not data.get("location"):
            return jsonify({"error": "Missing 'name' or 'location'"}), 400

        conn = get_db()
        cur = conn.cursor()  # Regular—no factory needed
        cur.execute("""
            INSERT INTO depot (name, location)
            VALUES (%s, %s)
            RETURNING depot_id, name, location
        """, (data["name"], data["location"]))
        new_depot = cur.fetchone()  # Tuple: unchanged
        conn.commit()
        return jsonify({
            "depot_id": new_depot[0],
            "name": new_depot[1],
            "location": new_depot[2]
        })
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

# --- Database connection ---
def get_db():
    return psycopg.connect(  # MIGRATION: psycopg.connect
        host="db.pofumrgccrhearjjlwhv.supabase.co",
        dbname="postgres",
        user="postgres",
        password="02496",
        port=5432  # MIGRATION: Int, not string
    )

# --------
@app.route("/tables")
def list_tables():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()  # Regular
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='public' 
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]  # Tuple access: unchanged
        return render_template("tables.html", tables=tables)
    except Exception as e:
        return f"Error fetching tables: {str(e)}"
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route("/tables/<table_name>")
def view_table(table_name):
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)  # MIGRATION: For dict rows
        cur.execute(f"SELECT * FROM {table_name} LIMIT 100")  # Limit for safety
        rows = cur.fetchall()  # Now list of dicts—template handles it
        return render_template("table_contents.html", table_name=table_name, rows=rows)
    except Exception as e:
        return f"Error fetching table {table_name}: {str(e)}"
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route("/api/induction/run", methods=["POST"])
def run_induction_api():
    try:
        success = run_induction()  # MIGRATION: Update fin.py if it uses psycopg2
        if success:
            return jsonify({"success": True, "message": "Induction calculation completed"})
        else:
            return jsonify({"success": False, "error": "Induction script failed"}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/induction")
def induction_list():
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)  # MIGRATION: dict_row
        cur.execute("""SELECT *
                     FROM train_induction_list ORDER BY 
                     CASE list_type
                        WHEN 'Induction' THEN 1
                        WHEN 'Standby' THEN 2
                        WHEN 'IBL' THEN 3
                        ELSE 4
                    END,
                    train_id
                    """)
        trains = cur.fetchall()  # Dict rows now
        return render_template("induction.html", trains=trains)
    except Exception as e:
        return f"Error fetching induction list: {str(e)}"
    finally:
        if conn:
            cur.close()
            conn.close()

@app.route("/api/trains/save", methods=["POST"])
def save_train():
    def parse_date(val):
        if not val:
            return None
        if isinstance(val, datetime):
            return val
        try:
            return datetime.fromisoformat(val)
        except Exception:
            return None

    REQUIRED_KEYS = ["train_number", "depot_id"]
    OPTIONAL_KEYS = {
        "status": "Available",
        "in_service": True,
        "last_updated": datetime.now()
    }

    data = request.get_json()
    if not data:
        return jsonify({"error": "No data received"}), 400

    # --- validate 'train' object ---
    train_data = data.get("train")
    if not train_data:
        return jsonify({"error": "Missing 'train' object"}), 400

    # --- validate required fields inside 'train' ---
    for key in REQUIRED_KEYS:
        if key not in train_data or train_data[key] is None:
            return jsonify({"error": f"Missing required field: {key}"}), 400

    # --- apply defaults ---
    for key, default in OPTIONAL_KEYS.items():
        if key not in train_data or train_data[key] is None:
            train_data[key] = default

    # --- coerce types ---
    train_data["last_updated"] = parse_date(train_data.get("last_updated")) or datetime.now()
    train_data["in_service"] = to_bool(train_data.get("in_service", True))

    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()  # Regular for all inserts

        # --- insert main train record ---
        cur.execute("""
            INSERT INTO train (train_number, status, depot_id, in_service, last_updated)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING train_id
        """, (
            train_data["train_number"],
            train_data["status"],
            train_data["depot_id"],
            train_data["in_service"],
            train_data["last_updated"]
        ))
        train_id = cur.fetchone()[0]  # Tuple: unchanged

        # --- depots ---  # MIGRATION: Logic unchanged, but note: SELECT by depot_id? Consider by name for upsert.
        for dp in data.get("depots", []):
            if not dp.get("name") or not dp.get("location"):
                continue

            cur.execute("SELECT depot_id FROM depot WHERE depot_id =%s", (dp.get("depot_id"),))  # Typo fix: space before %s
            existing = cur.fetchone()
            if existing:
                dp["depot_id"] = existing[0]  # Redundant if already set—ok
                continue

            cur.execute("""
                INSERT INTO depot (name, location)
                VALUES (%s, %s)
                RETURNING depot_id
            """, (dp.get("name"), dp.get("location")))
            dp["depot_id"] = cur.fetchone()[0]

        # --- fitness certificates ---
        for fc in data.get("fitness_certificate", []):
            if not fc.get("department") or not fc.get("status"):
                continue
            cur.execute("""
                INSERT INTO fitness_certificate (train_id, department, status, valid_from, valid_to, last_checked)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                train_id,
                fc.get("department"),
                fc.get("status"),
                parse_date(fc.get("valid_from")),
                parse_date(fc.get("valid_to")),
                parse_date(fc.get("last_checked")) or datetime.now()
            ))

        # --- job cards ---
        for jc in data.get("job_card", []):
            if not jc.get("description"):
                continue
            jc["parts_pending"] = to_bool(jc.get("parts_pending", False))
            cur.execute("""
                INSERT INTO job_card
                (train_id, severity, description, status, estimated_hours,
                 parts_pending, created_at, closed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                train_id,
                jc.get("severity"),
                jc.get("description"),
                jc.get("status", "Open"),
                jc.get("estimated_hours"),
                jc["parts_pending"],
                parse_date(jc.get("created_at")) or datetime.now(),
                parse_date(jc.get("closed_at"))
            ))

        # --- branding contracts ---
        for bc in data.get("branding_contract", []):
            if not bc.get("advertiser_name"):
                continue
            cur.execute("""
                INSERT INTO branding_contract
                (train_id, advertiser_name, priority_level,
                 exposure_required_hours, exposure_accumulated_hours,
                 window_type, start_date, end_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                train_id,
                bc.get("advertiser_name"),
                bc.get("priority_level"),
                bc.get("exposure_required_hours"),
                bc.get("exposure_accumulated_hours", 0),
                bc.get("window_type", "Daily"),
                parse_date(bc.get("start_date")),
                parse_date(bc.get("end_date"))
            ))

        # --- mileage logs ---
        for ml in data.get("mileage_log", []):
            if not ml.get("log_date"):
                continue
            cur.execute("""
                INSERT INTO mileage_log (train_id, log_date, km_run, cumulative_km)
                VALUES (%s, %s, %s, %s)
            """, (
                train_id,
                parse_date(ml.get("log_date")),
                ml.get("km_run"),
                ml.get("cumulative_km")
            ))

        # --- cleaning schedules ---
        for cs in data.get("cleaning_schedule", []):
            cur.execute("""
                INSERT INTO cleaning_schedule
                (train_id, cleaning_type, required, duration_hours,
                 bay_id, crew_assigned, deadline, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                train_id,
                cs.get("cleaning_type"),
                cs.get("required", True),
                cs.get("duration_hours"),
                cs.get("bay_id"),
                cs.get("crew_assigned"),
                parse_date(cs.get("deadline")),
                cs.get("status", "Scheduled")
            ))

        # --- stabling positions ---
        for sp in data.get("stabling_position", []):
            sp["blocked"] = to_bool(sp.get("blocked", False))
            cur.execute("""
                INSERT INTO stabling_position
                (train_id, bay_id, bay_position_index,
                 distance_to_exit_meters, estimated_shunt_moves, blocked)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                train_id,
                sp.get("bay_id"),
                sp.get("bay_position_index"),
                sp.get("distance_to_exit_meters"),
                sp.get("estimated_shunt_moves"),
                sp["blocked"]  # Fixed: sp.get("blocked") → sp["blocked"] after to_bool
            ))

        conn.commit()
        return jsonify({"success": True, "train_id": train_id})

    except Exception as e:
        if conn:
            conn.rollback()
        print("Error:", e)
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    app.run(debug=True)
