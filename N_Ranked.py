import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="KML_dat",
        user="postgres",
        password="02496",
        port="5432"
    )

# Scoring functions
def branding_score(train):
    level = train.get("priority_level")
    if level == "High":
        return 10
    elif level == "Medium":
        return 5
    return 0

def mileage_score(train, avg_mileage):
    if train["cumulative_km"] is None:
        return 0
    km = float(train["cumulative_km"])
    return -abs(km - float(avg_mileage)) / 100.0  # normalize

def cleaning_score(train):
    required = train.get("required", True)
    status = train.get("cleaning_status", "Scheduled")
    if required and status != "Done":
        return -5
    return 2

def geometry_score(train):
    shunts = train.get("estimated_shunt_moves")
    if shunts is None:
        return 0
    return -float(shunts)

def generate_induction_list(required_count):
    conn = get_connection()
    curr = conn.cursor(cursor_factory=RealDictCursor)

    # Fetch all relevant train info
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

    # compute average mileage safely
    mileage_values = [float(t["cumulative_km"]) for t in trains if t["cumulative_km"] is not None]
    avg_mileage = sum(mileage_values) / len(mileage_values) if mileage_values else 0.0

    # classify trains
    for t in trains:
        if not t["fitness_valid"] or t["job_card_open"]:
            ibl.append(t)
        else:
            score = 0
            score += 10 if t["fitness_valid"] else 0
            score += branding_score(t)
            score += mileage_score(t, avg_mileage)
            score += cleaning_score(t)
            score += geometry_score(t)
            t["score"] = score
            candidates.append(t)

    # rank candidates
    ranked = sorted(candidates, key=lambda x: x["score"], reverse=True)

    induction = ranked[:required_count]
    standby = ranked[required_count:]

    curr.close()
    conn.close()

    return induction, standby, ibl


if __name__ == "__main__":
    induction, standby, ibl = generate_induction_list(required_count=5)

    print("\n--- Induction List ---")
    for t in induction:
        print(f"Train {t['train_id']} | Score: {t['score']:.2f}")

    print("\n--- Standby List ---")
    for t in standby:
        print(f"Train {t['train_id']} | Score: {t['score']:.2f}")

    print("\n--- IBL (Maintenance) ---")
    for t in ibl:
        print(f"Train {t['train_id']} | Score: {t['score']:.2f} | Reason: Fitness={t['fitness_valid']} JobCardOpen={t['job_card_open']}")