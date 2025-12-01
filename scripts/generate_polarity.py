import uuid
import asyncio
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ───────────────────────────────
# Initialize VADER
# ───────────────────────────────

analyzer = SentimentIntensityAnalyzer()


# ───────────────────────────────
# Main polarity pipeline
# ───────────────────────────────

async def run_polarity_pipeline(conn):

    cur = conn.cursor()

    # STEP 1 — fetch ONLY missing polarity entries
    cur.execute("""
        SELECT hash_id, en
        FROM canonical_lexicon
        WHERE polarity IS NULL
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

        print("[POLARITY] No entries found. Nothing to process.", flush=True)

        return {
            "status": "nothing_to_process",
            "job_id": job_id,
            "processed": 0
        }

    # STEP 2 — create progress row
    cur.execute("""
        INSERT INTO metadata_progress (job_id, total, processed, status)
        VALUES (%s, %s, %s, 'running')
    """, (job_id, total, 0))
    conn.commit()

    print(f"[POLARITY] Starting job {job_id}. Total entries: {total}", flush=True)


    # ─────────────────────────────
    # MAIN LOOP — generate polarity
    # ─────────────────────────────

    for idx, (hash_id, word) in enumerate(rows, start=1):

        # Compute VADER polarity (compound = -1 to +1)
        compound = analyzer.polarity_scores(word)["compound"]

        # Update canonical_lexicon
        cur.execute("""
            UPDATE canonical_lexicon
            SET polarity = %s,
                updated_at = NOW()
            WHERE hash_id = %s
        """, (compound, hash_id))

        # Batch commit + progress update
        if idx % 25 == 0:
            conn.commit()

            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()

            print(f"[POLARITY] {idx}/{total} processed...", flush=True)

        await asyncio.sleep(0)

    # Final commit for leftovers
    conn.commit()

    # STEP 3 — mark job completed
    cur.execute("""
        UPDATE metadata_progress
        SET processed = %s,
            status = 'complete',
            finished_at = NOW()
        WHERE job_id = %s
    """, (total, job_id))
    conn.commit()

    cur.close()

    print(f"[POLARITY] Job {job_id} DONE.", flush=True)

    return {
        "status": "ok",
        "job_id": job_id,
        "processed": total
    }
