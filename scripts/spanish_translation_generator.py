import uuid
import asyncio
import re
from openai import OpenAI
import os

# ───────────────────────────────
# Initialize OpenAI client
# ───────────────────────────────

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ───────────────────────────────
# Category Sets for Model Selection
# ───────────────────────────────

abstract_domains = {
    "discourse & pragmatics",
    "culture, society & religion",
    "knowledge, education & thought",
    "language & grammar",
    "qualities, states & attributes",
    "actions & events",
    "time & calendars",
    "math & quantities",
    "arts & entertainment",
}

technical_domains = {
    "law & institutions",
    "science & research",
    "computing & software",
    "health & medicine",
    "work, business & finance",
}

literal_domains = {
    "people & relationships",
    "body",
    "emotions",
    "animals",
    "plants",
    "food & drink",
    "nature & environment",
    "places & geography",
    "objects, tools & instruments",
    "clothing",
    "home",
    "daily & home life",
    "shopping & money",
    "travel & transport",
    "emergencies",
    "leisure & sports",
    "materials & substances",
}

# ───────────────────────────────
# Helper Functions
# ───────────────────────────────

def choose_model(word, cat1, freq, level):
    """Adaptive model selection based on word characteristics"""
    text = str(word)
    tokens = len(text.split())
    freq = (freq or "").strip().lower()
    cat1 = (cat1 or "").strip().lower()
    level = (level or "").strip().lower()

    has_hyphen = "-" in text
    has_punct = bool(re.search(r"[.,!?;:''\"—]", text))
    is_titlecase = text.istitle()
    is_allcaps = text.isupper()

    if has_punct or has_hyphen or tokens >= 3:
        return "gpt-4o"
    if is_titlecase or is_allcaps:
        return "SKIP"
    if tokens == 1 and freq in {"very common", "common"}:
        return "gpt-4o-mini"
    if tokens == 1 and (freq in {"uncommon", "rare", "very rare"} or level in {"b2", "c1"}):
        return "gpt-4o"
    if tokens == 2 and freq in {"common", "uncommon"} and cat1 not in abstract_domains:
        return "gpt-4o-mini"
    if tokens == 2 and (freq in {"rare", "very rare"} or cat1 in abstract_domains):
        return "gpt-4o"
    if cat1 in technical_domains and level in {"b2", "c1"}:
        return "gpt-4o"
    if cat1 in literal_domains:
        return "gpt-4o-mini"
    return "gpt-4o-mini"


def build_prompt(word: str, cat1: str, target_lang: str = "Spanish") -> str:
    """Build translation prompt"""
    return (
        f"Translate the following English word into {target_lang} in its sense related to the semantic domain (category). "
        f"If it is an idiom or short sentence, provide its natural equivalent in {target_lang}. "
        f"Preserve the correct part of speech (verb, noun, adjective, etc.). "
        f"Output only the translation itself — no explanations, no synonyms, and no quotation marks.\n\n"
        f"Word: {word}\nCategory: {cat1 if cat1 else 'general'}"
    )


def get_translation(prompt: str, model: str, max_retries: int = 3) -> str:
    """Call GPT with retry logic"""
    import time
    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a precise, context-aware translator."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_tokens=20,
            )
            text = resp.choices[0].message.content.strip()
            return text.strip(' "\'.,;')
        except Exception as e:
            print(f"⚠️ {model} error: {e} (attempt {attempt+1}/{max_retries})", flush=True)
            time.sleep(4 + attempt * 3)
    return ""


# ───────────────────────────────
# Main Spanish Translation Pipeline
# ───────────────────────────────

async def run_spanish_translation_pipeline(conn, batch_size=500):
    """
    Translates English words to Spanish using adaptive GPT model selection
    """
    cur = conn.cursor()

    # STEP 1 — fetch entries needing Spanish translation
    cur.execute("""
        SELECT cl.hash_id, cl.en, cl.cat1a, cl.frequency, cl.level
        FROM canonical_lexicon cl
        LEFT JOIN translations_spanish ts ON cl.hash_id = ts.hash_id
        WHERE ts.translation IS NULL
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
        
        print("[SPANISH] No entries found. Nothing to process.", flush=True)
        
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

    print(f"[SPANISH] Starting job {job_id}. Total entries: {total}", flush=True)

    # Translation cache
    translation_cache = {}

    # MAIN LOOP — translate words
    for idx, (hash_id, word, cat1, freq, level) in enumerate(rows, start=1):
        
        # Strip "to " prefix safely
        word = re.sub(r"^to\s+", "", word, flags=re.IGNORECASE).strip() if word else ""
        
        if not word:
            continue

        cat1 = cat1 or ""
        freq = freq or ""
        level = level or ""

        # Choose model
        model = choose_model(word, cat1, freq, level)

        if model == "SKIP":
            translation = word
        else:
            # Check cache
            key = (word.lower(), cat1.lower())
            if key in translation_cache:
                translation = translation_cache[key]
            else:
                prompt = build_prompt(word, cat1, "Spanish")
                translation = get_translation(prompt, model)
                translation_cache[key] = translation

        # Insert into translations_spanish table
        cur.execute("""
            INSERT INTO translations_spanish (hash_id, translation, word_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (hash_id) DO UPDATE
            SET translation = EXCLUDED.translation
        """, (hash_id, translation, hash_id))

        # Batch commit + progress update
        if idx % 25 == 0:
            conn.commit()

            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()

            print(f"[SPANISH] {idx}/{total} processed...", flush=True)

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

    print(f"[SPANISH] Job {job_id} DONE.", flush=True)

    return {
        "status": "ok",
        "job_id": job_id,
        "processed": total
    }


# ───────────────────────────────
# Streaming version for real-time logs
# ───────────────────────────────

async def run_spanish_translation_pipeline_streaming(conn, batch_size=500):
    """
    Generator version that yields log messages in real-time
    """
    cur = conn.cursor()

    # STEP 1 — fetch entries needing Spanish translation
    cur.execute("""
        SELECT cl.hash_id, cl.en, cl.cat1a, cl.frequency, cl.level
        FROM canonical_lexicon cl
        LEFT JOIN translations_spanish ts ON cl.hash_id = ts.hash_id
        WHERE ts.translation IS NULL
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
        
        yield "[SPANISH] No entries found. nothing_to_process"
        return

    # STEP 2 — create progress row
    cur.execute("""
        INSERT INTO metadata_progress (job_id, total, processed, status)
        VALUES (%s, %s, %s, 'running')
    """, (job_id, total, 0))
    conn.commit()

    yield f"[SPANISH] Starting job {job_id}. Total entries: {total}"

    # Translation cache
    translation_cache = {}

    # MAIN LOOP — translate words
    for idx, (hash_id, word, cat1, freq, level) in enumerate(rows, start=1):
        
        # Strip "to " prefix safely
        word = re.sub(r"^to\s+", "", word, flags=re.IGNORECASE).strip() if word else ""
        
        if not word:
            continue

        cat1 = cat1 or ""
        freq = freq or ""
        level = level or ""

        # Choose model
        model = choose_model(word, cat1, freq, level)

        if model == "SKIP":
            translation = word
        else:
            # Check cache
            key = (word.lower(), cat1.lower())
            if key in translation_cache:
                translation = translation_cache[key]
            else:
                prompt = build_prompt(word, cat1, "Spanish")
                translation = get_translation(prompt, model)
                translation_cache[key] = translation

        # Insert into translations_spanish table
        cur.execute("""
            INSERT INTO translations_spanish (hash_id, translation, word_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (hash_id) DO UPDATE
            SET translation = EXCLUDED.translation
        """, (hash_id, translation, hash_id))

        # Batch commit + progress update
        if idx % 25 == 0:
            conn.commit()

            cur.execute("""
                UPDATE metadata_progress
                SET processed = %s
                WHERE job_id = %s
            """, (idx, job_id))
            conn.commit()

            yield f"[SPANISH] {idx}/{total} processed..."

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

    yield f"[SPANISH] Job {job_id} DONE."
