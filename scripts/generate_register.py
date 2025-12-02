import uuid
import asyncio
from openai import OpenAI
import os
import time
from typing import Optional

# ───────────────────────────────
# Initialize OpenAI client
# ───────────────────────────────

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

ALLOWED_FORMALITY = ["formal", "neutral", "informal", "slang", "archaic"]

SYSTEM_PROMPT = """
You are a linguistic classifier for a lexicon.
Your task is to classify the *formality level* of a single word or expression.

Return ONLY ONE of these labels:
- formal
- neutral
- informal
- slang
- archaic

Rules:
- Do NOT return multiple labels.
- Do NOT explain your answer.
- If the word is slang, classify it as "slang" (do NOT also consider informal).
- If the word is archaic/obsolete/literary-old, classify as "archaic".
- If none of the marked categories apply, classify as "neutral".
- Do not include quotation marks or any other element other than the chosen option itself. 
""".strip()


# ───────────────────────────────
# Helper function
# ───────────────────────────────

def classify_formality(lemma: str, max_retries: int = 5) -> Optional[str]:
    """
    Returns one of:
    formal, neutral, informal, slang, archaic
    
    Implements exponential backoff retry logic for robustness.
    Returns None if all retries fail.
    """
    
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": lemma}
                ],
                timeout=30.0  # 30 second timeout
            )

            raw = response.choices[0].message.content.strip().lower()

            # Safety check: enforce valid output
            if raw not in ALLOWED_FORMALITY:
                # fallback to neutral if GPT gives something unexpected
                return "neutral"

            return raw
            
        except Exception as e:
            wait_time = (2 ** attempt) + (0.1 * attempt)  # Exponential backoff
            
            if attempt < max_retries - 1:
                print(f"[RETRY] Attempt {attempt + 1} failed for '{lemma}': {str(e)}. Retrying in {wait_time:.1f}s...", flush=True)
                time.sleep(wait_time)
            else:
                print(f"[ERROR] All {max_retries} attempts failed for '{lemma}': {str(e)}", flush=True)
                return None  # Return None to signal failure


# ───────────────────────────────
# Main register pipeline
# ───────────────────────────────

async def run_register_pipeline(conn, batch_size=500):

    cur = conn.cursor()

    # STEP 1 — fetch ONLY missing register entries (limit to batch_size)
    cur.execute("""
        SELECT hash_id, en
        FROM canonical_lexicon
        WHERE register IS NULL
        LIMIT %s
    """, (batch_size,))

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

        print("[REGISTER] No entries found. Nothing to process.", flush=True)

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

    print(f"[REGISTER] Starting job {job_id}. Total entries: {total}", flush=True)


    # ─────────────────────────────
    # MAIN LOOP — generate register
    # ─────────────────────────────

    failed_entries = []
    
    for idx, (hash_id, word) in enumerate(rows, start=1):

        # Classify formality using GPT-4 with retry logic
        register = classify_formality(word)
        
        # If classification failed after all retries, skip and log
        if register is None:
            failed_entries.append((hash_id, word))
            print(f"[SKIP] Failed to classify '{word}' (hash: {hash_id}). Will retry later.", flush=True)
            continue

        # Update canonical_lexicon
        cur.execute("""
            UPDATE canonical_lexicon
            SET register = %s,
                updated_at = NOW()
            WHERE hash_id = %s
        """, (register, hash_id))

        # Batch commit + progress update
        if idx % 25 == 0:
            conn.commit()

            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()

            print(f"[REGISTER] {idx}/{total} processed... (Failed: {len(failed_entries)})", flush=True)

        # Rate limiting: small delay to avoid hitting API limits
        await asyncio.sleep(0.05)

    # Final commit for leftovers
    conn.commit()

    # STEP 3 — mark job completed
    successful = total - len(failed_entries)
    
    cur.execute("""
        UPDATE metadata_progress
        SET processed = %s,
            status = 'complete',
            finished_at = NOW()
        WHERE job_id = %s
    """, (successful, job_id))
    conn.commit()

    cur.close()

    if failed_entries:
        print(f"[REGISTER] Job {job_id} DONE. Processed: {successful}/{total}. Failed: {len(failed_entries)}", flush=True)
    else:
        print(f"[REGISTER] Job {job_id} DONE. All {total} entries processed successfully.", flush=True)

    return {
        "status": "ok",
        "job_id": job_id,
        "processed": successful,
        "failed": len(failed_entries),
        "failed_entries": failed_entries
    }


# ───────────────────────────────
# Streaming version for real-time logs
# ───────────────────────────────

async def run_register_pipeline_streaming(conn, batch_size=500):
    """
    Generator version that yields log messages in real-time
    """
    cur = conn.cursor()

    # STEP 1 — fetch ONLY missing register entries (limit to batch_size)
    cur.execute("""
        SELECT hash_id, en
        FROM canonical_lexicon
        WHERE register IS NULL
        LIMIT %s
    """, (batch_size,))

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
        
        yield "[REGISTER] No entries found. nothing_to_process"
        return

    # STEP 2 — create progress row
    cur.execute("""
        INSERT INTO metadata_progress (job_id, total, processed, status)
        VALUES (%s, %s, %s, 'running')
    """, (job_id, total, 0))
    conn.commit()

    yield f"[REGISTER] Starting job {job_id}. Total entries: {total}"

    # MAIN LOOP — generate register
    failed_entries = []
    
    for idx, (hash_id, word) in enumerate(rows, start=1):

        # Classify formality using GPT-4 with retry logic
        register = classify_formality(word)
        
        # If classification failed after all retries, skip and log
        if register is None:
            failed_entries.append((hash_id, word))
            yield f"[SKIP] Failed to classify '{word}' (hash: {hash_id}). Will retry later."
            continue

        # Update canonical_lexicon
        cur.execute("""
            UPDATE canonical_lexicon
            SET register = %s,
                updated_at = NOW()
            WHERE hash_id = %s
        """, (register, hash_id))

        # Batch commit + progress update
        if idx % 25 == 0:
            conn.commit()

            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()

            yield f"[REGISTER] {idx}/{total} processed... (Failed: {len(failed_entries)})"

        # Rate limiting: small delay to avoid hitting API limits
        await asyncio.sleep(0.05)

    # Final commit for leftovers
    conn.commit()

    # STEP 3 — mark job completed
    successful = total - len(failed_entries)
    
    cur.execute("""
        UPDATE metadata_progress
        SET processed = %s,
            status = 'complete',
            finished_at = NOW()
        WHERE job_id = %s
    """, (successful, job_id))
    conn.commit()

    cur.close()

    if failed_entries:
        yield f"[REGISTER] Job {job_id} DONE. Processed: {successful}/{total}. Failed: {len(failed_entries)}"
    else:
        yield f"[REGISTER] Job {job_id} DONE. All {total} entries processed successfully."
