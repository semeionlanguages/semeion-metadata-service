import math
import uuid
import asyncio
from wordfreq import zipf_frequency

# ───────────────────────────────
# Helper functions
# ───────────────────────────────

def compute_frequency_label(zipf):
    if zipf is None:
        return "unknown"
    if zipf >= 6.0:
        return "very common"
    if zipf >= 5.0:
        return "common"
    if zipf >= 4.0:
        return "uncommon"
    if zipf >= 3.0:
        return "rare"
    return "very rare"


def normalize_zipf(z):
    if z is None:
        return 0
    n = (z - 2) / 5
    return max(0, min(1, n))


def compute_cefr_level(S):
    if S >= 0.82:
        return "A1"
    if S >= 0.70:
        return "A2"
    if S >= 0.55:
        return "B1"
    if S >= 0.40:
        return "B2"
    if S >= 0.28:
        return "C1"
    return "C2"


def difficulty_from_cefr(level):
    mapping = {
        "A1": ("beginner", 1),
        "A2": ("elementary", 2),
        "B1": ("intermediate", 3),
        "B2": ("upper intermediate", 4),
        "C1": ("advanced", 5),
        "C2": ("mastery", 6),
    }
    return mapping.get(level, ("unknown", None))


def hybrid_dispersion(zipf):
    if zipf is None:
        return 0.15

    if zipf < 2:
        return 0.20
    if zipf < 3:
        return 0.35
    if zipf < 4:
        return 0.55
    if zipf < 5:
        return 0.70
    if zipf < 6:
        return 0.85
    return 0.95


# ───────────────────────────────
# Main metadata pipeline
# ───────────────────────────────

async def run_metadata_pipeline(conn):

    cur = conn.cursor()

    # Step 1 — find entries to update
    cur.execute("""
        SELECT hash_id, en
        FROM canonical_lexicon
        WHERE zipf IS NULL
           OR frequency IS NULL
           OR dispersion_cd IS NULL
           OR level IS NULL
           OR difficulty IS NULL
           OR difficulty_numeric IS NULL
    """)

    rows = cur.fetchall()
    total = len(rows)

    # Create job_id for progress tracking
    job_id = str(uuid.uuid4())

    # Early exit: nothing to process
    if total == 0:
        cur.execute("""
            INSERT INTO metadata_progress (job_id, total, processed, status, finished_at)
            VALUES (%s, 0, 0, 'complete', NOW())
        """, (job_id,))
        conn.commit()

        cur.close()

        print("[PIPELINE] No entries found. Nothing to process.", flush=True)

        return {
            "status": "nothing_to_process",
            "job_id": job_id,
            "processed": 0
        }

    # Step 2 — initialize progress row
    cur.execute("""
        INSERT INTO metadata_progress (job_id, total, processed, status)
        VALUES (%s, %s, %s, 'running')
    """, (job_id, total, 0))
    conn.commit()

    print(f"[PIPELINE] Starting job {job_id}. Total entries: {total}", flush=True)


    # ─────────────────────────────
    # Main processing loop
    # ─────────────────────────────

    for idx, (hash_id, word) in enumerate(rows, start=1):

        # ZIPF
        z = zipf_frequency(word, "en")
        if z == 0:
            z = None

        # Derived fields
        frequency = compute_frequency_label(z)
        dispersion_cd = hybrid_dispersion(z)
        z_norm = normalize_zipf(z)
        S = 0.7 * z_norm + 0.3 * dispersion_cd
        cefr = compute_cefr_level(S)
        difficulty, difficulty_numeric = difficulty_from_cefr(cefr)

        # Update canonical_lexicon
        cur.execute("""
            UPDATE canonical_lexicon
            SET 
                zipf = %s,
                frequency = %s,
                dispersion_cd = %s,
                level = %s,
                difficulty = %s,
                difficulty_numeric = %s,
                updated_at = NOW()
            WHERE hash_id = %s
        """, (z, frequency, dispersion_cd, cefr, difficulty, difficulty_numeric, hash_id))
        conn.commit()

        # Progress update every 25 rows
        if idx % 25 == 0:
            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()
            print(f"[PIPELINE] {idx}/{total} processed...", flush=True)

        await asyncio.sleep(0)


    # Step 3 — mark job complete
    cur.execute("""
        UPDATE metadata_progress
        SET processed = %s,
            status = 'complete',
            finished_at = NOW()
        WHERE job_id = %s
    """, (total, job_id))
    conn.commit()

    cur.close()

    print(f"[PIPELINE] Job {job_id} DONE.", flush=True)

    return {
        "status": "ok",
        "job_id": job_id,
        "processed": total
    }
