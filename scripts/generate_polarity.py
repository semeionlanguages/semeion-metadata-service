import uuid
import asyncio
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ───────────────────────────────
# Initialize VADER
# ───────────────────────────────

analyzer = SentimentIntensityAnalyzer()

# ───────────────────────────────
# Helper functions
# ───────────────────────────────

def compute_polarity_label(compound_score):
    """
    Convert VADER compound score to polarity label.
    Compound score ranges from -1 (most negative) to +1 (most positive)
    """
    if compound_score >= 0.05:
        return "positive"
    elif compound_score <= -0.05:
        return "negative"
    else:
        return "neutral"


def compute_polarity_numeric(compound_score):
    """
    Convert compound score to 1-5 scale:
    1 = very negative
    2 = negative
    3 = neutral
    4 = positive
    5 = very positive
    """
    if compound_score <= -0.6:
        return 1
    elif compound_score <= -0.2:
        return 2
    elif compound_score < 0.2:
        return 3
    elif compound_score < 0.6:
        return 4
    else:
        return 5


# ───────────────────────────────
# Main polarity pipeline
# ───────────────────────────────

async def run_polarity_pipeline(conn):

    cur = conn.cursor()

    # Step 1 — find entries to update
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

    # Step 2 — initialize progress row
    cur.execute("""
        INSERT INTO metadata_progress (job_id, total, processed, status)
        VALUES (%s, %s, %s, 'running')
    """, (job_id, total, 0))
    conn.commit()

    print(f"[POLARITY] Starting job {job_id}. Total entries: {total}", flush=True)


    # ─────────────────────────────
    # Main processing loop
    # ─────────────────────────────

    for idx, (hash_id, word) in enumerate(rows, start=1):

        # Analyze sentiment using VADER
        scores = analyzer.polarity_scores(word)
        compound = scores['compound']

        # Derived fields
        polarity = compute_polarity_label(compound)
        polarity_numeric = compute_polarity_numeric(compound)

        # Update canonical_lexicon
        cur.execute("""
            UPDATE canonical_lexicon
            SET 
                polarity = %s,
                updated_at = NOW()
            WHERE hash_id = %s
        """, (polarity, hash_id))

        # Batch commit every 25 rows instead of every row
        if idx % 25 == 0:
            conn.commit()
            
            # Progress update
            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()
            print(f"[POLARITY] {idx}/{total} processed...", flush=True)

        await asyncio.sleep(0)
    
    # Final commit for remaining rows
    conn.commit()


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

    print(f"[POLARITY] Job {job_id} DONE.", flush=True)

    return {
        "status": "ok",
        "job_id": job_id,
        "processed": total
    }
