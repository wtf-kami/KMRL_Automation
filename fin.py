import psycopg
import psycopg.rows
from datetime import datetime

def run_induction(required_count=3):
    """
    Perform induction calculation and save to database.
    """
    try:
        create_induction_table()  # ensure table exists
        induction, standby, ibl = generate_induction_list(required_count)
        save_lists_to_db(induction, standby, ibl)
        print("Induction Calculation Completed")
        return True
    except Exception as e:
        print("Induction failed:", e)
        return False

# ---------------- Database connection ----------------
def get_connection():
    return psycopg.connect(
        host="db.pofumrgccrhearjjlwhv.supabase.co",
        dbname="postgres",
        user="postgres",
        password="02496",
        port=5432
    )

# ---------------- Weights (tune these as per KMRL priorities) ----------------
WEIGHTS = {
    "fitness": 5.0,
    "branding": 3.0,
    "mileage": 2.0,
    "cleaning": 1.0,
    "geometry": 1.0
}

# ---------------- Component functions ----------------
def fitness_component(train):
    return 1 if train["fitness_valid"] else 0

def branding_component(train):
    level = train.get("priority_level")
    if level == "High":
        return 1.0
    elif level == "Medium":
        return 0.5
    return 0.0

def mileage_component(train, avg_mileage):
    if train["cumulative_km"] is None:
        return 0.0
    km = float(train["cumulative_km"])
    deviation = abs(km - float(avg_mileage)) / 1000.0
    return max(0.0, 1.0 - deviation)

def cleaning_component(train):
    required = train.get("required", True)
    status = train.get("cleaning_status", "Scheduled")
    if required and status != "Done":
        return 0.0
    return 1.0

def geometry_component(train):
    shunts = train.get("estimated_shunt_moves")
    if shunts is None:
        return 0.5
    score = max(0.0, 1.0 - (float(shunts) / 10.0))
    return score

# ---------------- Main algorithm ----------------
def generate_induction_list(required_count):
    conn = None
    try:
        conn = get_connection()
        curr = conn.cursor(row_factory=psycopg.rows.dict_row)

        curr.execute("""
            SELECT
                t.train_id,
                MAX(CASE WHEN fc.status = 'Valid' THEN 1 ELSE 0 END) AS fitness_valid,
                MAX(CASE WHEN jc.status = 'Open' THEN 1 ELSE 0 END) AS job_card_open,
                bc.priority_level,
                ml.cumulative_km,
                cs.required,
                cs.status AS cleaning_status,
                sp.estimated_shunt_moves
            FROM train t
            LEFT JOIN fitness_certificate fc ON t.train_id = fc.train_id
            LEFT JOIN job_card jc ON t.train_id = jc.train_id
            LEFT JOIN branding_contract bc ON t.train_id = bc.train_id
            LEFT JOIN (
                     SELECT DISTINCT ON (train_id)* 
                     FROM mileage_log
                     ORDER BY train_id, log_date DESC
            ) ml ON t.train_id = ml.train_id
            LEFT JOIN (
                     SELECT DISTINCT ON (train_id)* 
                     FROM cleaning_schedule
                     ORDER BY train_id, deadline DESC
            ) cs ON t.train_id = cs.train_id
            LEFT JOIN stabling_position sp ON t.train_id = sp.train_id
            GROUP BY t.train_id, bc.priority_level, ml.cumulative_km, cs.required, cs.status, sp.estimated_shunt_moves
        """)
        
        trains = curr.fetchall()

        candidates, ibl = [], []

        # average mileage
        mileage_values = [float(t["cumulative_km"]) for t in trains if t["cumulative_km"] is not None]
        avg_mileage = sum(mileage_values) / len(mileage_values) if mileage_values else 0.0

        for t in trains:
            if not t["fitness_valid"] or t["job_card_open"]:
                ibl.append(t)
            else:
                fitness = fitness_component(t)
                branding = branding_component(t)
                mileage  = mileage_component(t, avg_mileage)
                cleaning = cleaning_component(t)
                geometry = geometry_component(t)
                score = (
                    WEIGHTS["fitness"]  * fitness +
                    WEIGHTS["branding"] * branding +
                    WEIGHTS["mileage"]  * mileage +
                    WEIGHTS["cleaning"] * cleaning +
                    WEIGHTS["geometry"] * geometry
                )
                t["score"] = score
                candidates.append(t)

        ranked = sorted(candidates, key=lambda x: x["score"], reverse=True)
        induction = ranked[:required_count]
        standby = ranked[required_count:]

        return induction, standby, ibl
    finally:
        if conn:
            curr.close()
            conn.close()

# ---------------- Database storage ----------------
def create_induction_table():
    conn = None
    try:
        conn = get_connection()
        curr = conn.cursor()
        curr.execute("""
            CREATE TABLE IF NOT EXISTS train_induction_list (
                id SERIAL PRIMARY KEY,
                train_id INT NOT NULL,
                list_type VARCHAR(20) NOT NULL,
                score NUMERIC,
                fitness_valid BOOLEAN,
                job_card_open BOOLEAN,
                branding_level VARCHAR(20),
                cumulative_km NUMERIC,
                cleaning_required BOOLEAN,
                cleaning_status VARCHAR(20),
                estimated_shunt_moves NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    finally:
        if conn:
            curr.close()
            conn.close()

def save_lists_to_db(induction, standby, ibl):
    conn = None
    try:
        conn = get_connection()
        curr = conn.cursor()
        # clear old data
        curr.execute("DELETE FROM train_induction_list")
        conn.commit()

        def insert_train(t, list_type):
            curr.execute("""
                INSERT INTO train_induction_list (
                    train_id, list_type, score, fitness_valid, job_card_open,
                    branding_level, cumulative_km, cleaning_required, cleaning_status,
                    estimated_shunt_moves
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                t["train_id"],
                list_type,
                t.get("score"),
                bool(t.get("fitness_valid")),   # CAST integer to boolean
                bool(t.get("job_card_open")),   # CAST integer to boolean
                t.get("priority_level"),
                t.get("cumulative_km"),
                bool(t.get("required")),        # CAST integer/None to boolean
                t.get("cleaning_status"),
                t.get("estimated_shunt_moves")
            ))

        for t in induction:
            insert_train(t, "Induction")
        for t in standby:
            insert_train(t, "Standby")
        for t in ibl:
            insert_train(t, "IBL")

        conn.commit()
    finally:
        if conn:
            curr.close()
            conn.close()

# ---------------- Main block ----------------
if __name__ == "__main__":
    create_induction_table()
    induction, standby, ibl = generate_induction_list(required_count=3)
    save_lists_to_db(induction, standby, ibl)

    print("\n--- Induction List ---")
    for t in induction:
        print(f"Train {t['train_id']} | Score: {t['score']:.2f}")

    print("\n--- Standby List ---")
    for t in standby:
        print(f"Train {t['train_id']} | Score: {t['score']:.2f}")

    print("\n--- IBL (Maintenance) ---")
    for t in ibl:
        print(f"Train {t['train_id']} | Reason: Fitness={t['fitness_valid']} JobCardOpen={t['job_card_open']}")
