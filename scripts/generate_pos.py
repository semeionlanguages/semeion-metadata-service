import uuid
import asyncio
from openai import OpenAI
import os
import time
from typing import Optional, List
import spacy
from nltk.corpus import wordnet as wn
import re
import json

# ───────────────────────────────
# Initialize OpenAI
# ───────────────────────────────

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# Load spaCy English model (ensure installed: en_core_web_sm)
NLP = spacy.load("en_core_web_sm")

# Only these POS can appear in canonical_lexicon for open-class words
ALLOWED_POS = ["noun", "verb", "adjective", "adverb"]


# ───────────────────────────────
# GPT System Prompt – strict to allowed POS only
# ───────────────────────────────

SYSTEM_PROMPT_POS = """
You are an expert linguistic classifier.

Your task: determine ALL valid parts of speech (POS) that a single English lemma can take,
BUT YOU MUST choose ONLY from this allowed set:

["noun", "verb", "adjective", "adverb"]

Return a JSON array, e.g.:
["noun", "verb"]

Rules:
- Return ONLY a JSON array.
- Use ONLY the allowed POS labels.
- No explanation.
""".strip()


# ════════════════════════════════════════════
# spaCy POS detection
# ════════════════════════════════════════════

def spacy_detect_pos(word: str) -> List[str]:
    doc = NLP(word)
    pos_set = set()

    for t in doc:
        tag = t.pos_.lower()

        if tag == "noun":
            pos_set.add("noun")
        elif tag == "verb":
            pos_set.add("verb")
        elif tag == "adj":
            pos_set.add("adjective")
        elif tag == "adv":
            pos_set.add("adverb")

    return list(pos_set)


# ════════════════════════════════════════════
# WordNet POS inference
# ════════════════════════════════════════════

def wordnet_detect_pos(word: str) -> List[str]:
    synsets = wn.synsets(word)
    pos_set = set()

    for syn in synsets:
        p = syn.pos()
        if p == "n":
            pos_set.add("noun")
        elif p == "v":
            pos_set.add("verb")
        elif p in ("a", "s"):
            pos_set.add("adjective")
        elif p == "r":
            pos_set.add("adverb")

    return list(pos_set)


# ════════════════════════════════════════════
# Heuristic POS inference
# ════════════════════════════════════════════

def heuristic_detect_pos(word: str) -> List[str]:
    w = word.lower()
    pos = set()

    # Simple morphological clues
    if re.search(r"(ness|tion|ment|ity|hood|ship|ance|ence)$", w):
        pos.add("noun")

    if re.search(r"(ize|ise|ify|ate)$", w):
        pos.add("verb")

    if re.search(r"(able|ible|ous|ful|less|ive|ic|al|ish)$", w):
        pos.add("adjective")

    if re.search(r"(ly)$", w):
        pos.add("adverb")

    # Capitalized single word → possible noun
    if word[0].isupper() and " " not in word:
        pos.add("noun")

    return list(pos)


# ════════════════════════════════════════════
# GPT fallback
# ════════════════════════════════════════════

def gpt_fallback_pos(word: str, max_retries: int = 5) -> Optional[List[str]]:
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_POS},
                    {"role": "user", "content": word}
                ],
                timeout=45.0,
            )

            raw = response.choices[0].message.content.strip()

            # Must be a JSON array
            try:
                arr = eval(raw) if raw.startswith("[") else None
                if isinstance(arr, list):
                    cleaned = [p for p in arr if p in ALLOWED_POS]
                    return cleaned
            except:
                return None

        except Exception as e:
            wait = 1.5 * (2 ** attempt)
            print(f"[GPT RETRY] {attempt+1} for '{word}': {e}. Waiting {wait:.1f}s.")
            time.sleep(wait)

    return None


# ════════════════════════════════════════════
# WEIGHTED POS AGGREGATION
# ════════════════════════════════════════════

def aggregate_pos(word: str) -> Optional[List[str]]:
    """
    Combined multimethod inference with weighting:
      spaCy:      0.45
      WordNet:    0.25
      Heuristic:  0.15
      GPT:        0.15 (fallback only)
    """

    weights = {
        "spacy": 0.45,
        "wordnet": 0.25,
        "heuristic": 0.15,
        "gpt": 0.15,
    }

    results = {
        "spacy": spacy_detect_pos(word),
        "wordnet": wordnet_detect_pos(word),
        "heuristic": heuristic_detect_pos(word),
        "gpt": None,
    }

    # Use GPT only when the earlier signals fail completely
    if not any(results.values()):
        results["gpt"] = gpt_fallback_pos(word)

    scores = {pos: 0.0 for pos in ALLOWED_POS}

    for method, pos_list in results.items():
        if pos_list:
            for pos in pos_list:
                scores[pos] += weights[method]

    final = [pos for pos, score in scores.items() if score >= 0.28]
    return final or None


# ───────────────────────────────────────────────
# MAIN POS PIPELINE (non-streaming)
# ───────────────────────────────────────────────

async def run_pos_pipeline(conn, batch_size=500):

    cur = conn.cursor()

    cur.execute("""
        SELECT hash_id, en
        FROM canonical_lexicon
        WHERE pos IS NULL
        LIMIT %s
    """, (batch_size,))
    rows = cur.fetchall()
    total = len(rows)
    job_id = str(uuid.uuid4())

    if total == 0:
        cur.execute("""
            INSERT INTO metadata_progress (job_id, total, processed, status, finished_at)
            VALUES (%s, 0, 0, 'complete', NOW())
        """, (job_id,))
        conn.commit()
        cur.close()
        print("[POS] Nothing to process.")
        return {"status": "nothing_to_process", "job_id": job_id}

    # Create progress record
    cur.execute("""
        INSERT INTO metadata_progress (job_id, total, processed, status)
        VALUES (%s, %s, 0, 'running')
    """, (job_id, total))
    conn.commit()

    print(f"[POS] Starting job {job_id}. Total entries: {total}")

    failed = []

    for idx, (hash_id, word) in enumerate(rows, start=1):

        pos = aggregate_pos(word)

        if not pos:
            failed.append((hash_id, word))
            continue

        # Safe update: do not overwrite existing POS
        cur.execute("""
            UPDATE canonical_lexicon
            SET pos = %s::jsonb,
                updated_at = NOW()
            WHERE hash_id = %s
              AND pos IS NULL
        """, (json.dumps(pos), hash_id))

        if idx % 25 == 0:
            conn.commit()
            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()
            print(f"[POS] {idx}/{total} processed. Failed so far: {len(failed)}")

        await asyncio.sleep(0.05)

    conn.commit()

    successful = total - len(failed)

    cur.execute("""
        UPDATE metadata_progress
        SET processed = %s,
            status = 'complete',
            finished_at = NOW()
        WHERE job_id = %s
    """, (successful, job_id))
    conn.commit()

    cur.close()

    print(f"[POS] Job {job_id} COMPLETE. {successful}/{total} succeeded.")
    return {
        "status": "ok",
        "job_id": job_id,
        "processed": successful,
        "failed": len(failed),
        "failed_entries": failed,
    }


# ───────────────────────────────────────────────
# STREAMING VERSION FOR UI LOGS
# ───────────────────────────────────────────────

async def run_pos_pipeline_streaming(conn, batch_size=500):

    cur = conn.cursor()

    cur.execute("""
        SELECT hash_id, en
        FROM canonical_lexicon
        WHERE pos IS NULL
        LIMIT %s
    """, (batch_size,))
    rows = cur.fetchall()
    total = len(rows)
    job_id = str(uuid.uuid4())

    if total == 0:
        cur.execute("""
            INSERT INTO metadata_progress (job_id, total, processed, status, finished_at)
            VALUES (%s, 0, 0, 'complete', NOW())
        """, (job_id,))
        conn.commit()
        cur.close()
        yield "[POS] Nothing to process."
        return

    cur.execute("""
        INSERT INTO metadata_progress (job_id, total, processed, status)
        VALUES (%s, %s, 0, 'running')
    """, (job_id, total))
    conn.commit()

    yield f"[POS] Job {job_id} started. Total entries: {total}"

    failed = []

    for idx, (hash_id, word) in enumerate(rows, start=1):

        pos = aggregate_pos(word)

        if not pos:
            failed.append((hash_id, word))
            yield f"[SKIP] No POS inferred for '{word}' ({hash_id})."
            continue

        # Safe update
        cur.execute("""
            UPDATE canonical_lexicon
            SET pos = %s::jsonb,
                updated_at = NOW()
            WHERE hash_id = %s
              AND pos IS NULL
        """, (json.dumps(pos), hash_id))

        if idx % 25 == 0:
            conn.commit()
            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()
            yield f"[POS] {idx}/{total} processed. Failed: {len(failed)}"

        await asyncio.sleep(0.05)

    conn.commit()

    successful = total - len(failed)

    cur.execute("""
        UPDATE metadata_progress
        SET processed = %s,
            status = 'complete',
            finished_at = NOW()
        WHERE job_id = %s
    """, (successful, job_id))
    conn.commit()

    cur.close()

    yield f"[POS] Job {job_id} COMPLETE. Success {successful}/{total}, Failed {len(failed)}"
