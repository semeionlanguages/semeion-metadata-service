from fastapi import FastAPI
from pydantic import BaseModel
import os
import psycopg2
import uvicorn
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# NEW IMPORT
from scripts.generate_frequency_metrics import run_metadata_pipeline

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL1")

# Global DB connection (do NOT recreate on each request)
conn = psycopg2.connect(DATABASE_URL)

class MetadataRequest(BaseModel):
    hash_id: str

@app.post("/generate")
async def generate(req: MetadataRequest):
    # Create cursor per request for thread safety
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT en 
            FROM canonical_lexicon
            WHERE hash_id = %s
        """, (req.hash_id,))
        
        row = cursor.fetchone()

        if row:
            word_en = row[0]
        else:
            word_en = None

        return {
            "status": "ok",
            "hash_id": req.hash_id,
            "english_word": word_en
        }
    finally:
        cursor.close()


# ─────────────────────────────────────
# NEW ENDPOINT — RUN METADATA PIPELINE
# ─────────────────────────────────────
@app.post("/run/metadata-pipeline")
async def metadata_pipeline_endpoint():
    print("[API] Trigger received: metadata pipeline", flush=True)
    result = await run_metadata_pipeline(conn)
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
