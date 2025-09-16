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

# ---------------- Weights (tune these as per KMRL priorities) ----------------
WEIGHTS = {
    "fitness": 5.0,     # importance of valid fitness certificate
    "branding": 3.0,    # importance of branding exposure
    "mileage": 2.0,     # importance of mileage balancing
    "cleaning": 1.0,    # importance of cleaning status
    "geometry": 1.0     # importance of stabling geometry
}

# ---------------- Component functions ----------------
def fitness_component(train):
    return 1 if train["fitness_valid"] else 0   # already binary

def branding_component(train):
    level = train.get("priority_level")
    if level == "High":
        return 1.0   # normalized to 1
    elif level == "Medium":
        return 0.5
    return 0.0

def mileage_component(train, avg_mileage):
    if train["cumulative_km"] is None:
        return 0.0
    km = float(train["cumulative_km"])
    # smaller deviation → closer to 1, larger deviation → closer to 0
    deviation = abs(km - float(avg_mileage)) / 1000.0  
    return max(0.0, 1.0 - deviation)  # normalized between 0 and 1

def cleaning_component(train):
    required = train.get("required", True)
    status = train.get("cleaning_status", "Scheduled")
    if required and status != "Done":
        return 0.0   # bad (not cleaned)
    return 1.0       # good (already cleaned / not required)

def geometry_component(train):
    shunts = train.get("estimated_shunt_moves")
    if shunts is None:
        return 0.5  # neutral if unknown
    # More shunts → lower score, normalize so 0 moves = 1, 10+ moves = 0
    score = max(0.0, 1.0 - (float(shunts) / 10.0))
    return score

# ---------------- Main algorithm ----------------
def generate_induction_list(required_count):
    conn = get_connection()
    curr = conn.cursor(cursor_factory=RealDictCursor)

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
            # compute components
            fitness = fitness_component(t)
            branding = branding_component(t)
            mileage  = mileage_component(t, avg_mileage)
            cleaning = cleaning_component(t)
            geometry = geometry_component(t)

            # weighted score
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

    curr.close()
    conn.close()

    return induction, standby, ibl


if __name__ == "__main__":
    induction, standby, ibl = generate_induction_list(required_count=1)

    print("\n--- Induction List ---")
    for t in induction:
        print(f"Train {t['train_id']} | Score: {t['score']:.2f}")

    print("\n--- Standby List ---")
    for t in standby:
        print(f"Train {t['train_id']} | Score: {t['score']:.2f}")

    print("\n--- IBL (Maintenance) ---")
    for t in ibl:
        print(f"Train {t['train_id']} | Reason: Fitness={t['fitness_valid']} JobCardOpen={t['job_card_open']}")
