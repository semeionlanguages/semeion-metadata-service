from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import psycopg2
import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# REAL IMPORT - NO PLACEHOLDER
from scripts.generate_frequency_metrics import run_metadata_pipeline

app = FastAPI(title="Semeion Metadata Service")

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL1")

# Global DB connection
try:
    conn = psycopg2.connect(DATABASE_URL)
    print("[STARTUP] Database connected successfully", flush=True)
except Exception as e:
    print(f"[STARTUP] Database connection failed: {e}", flush=True)
    conn = None

class MetadataRequest(BaseModel):
    hash_id: str

@app.get("/")
async def root():
    return {"service": "semeion-metadata-service", "status": "running"}

@app.get("/health")
async def health():
    db_status = "connected" if conn else "disconnected"
    return {"status": "healthy", "service": "semeion-metadata-service", "database": db_status}

@app.post("/generate")
async def generate(req: MetadataRequest):
    if not conn:
        return {"status": "error", "message": "Database not connected"}
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT en 
            FROM canonical_lexicon
            WHERE hash_id = %s
        """, (req.hash_id,))
        
        row = cursor.fetchone()
        word_en = row[0] if row else None

        return {
            "status": "ok",
            "hash_id": req.hash_id,
            "english_word": word_en
        }
    finally:
        cursor.close()

# REAL ENDPOINT - RUNS ACTUAL PYTHON SCRIPT
@app.post("/run/metadata-pipeline")
async def metadata_pipeline_endpoint():
    """
    Generate frequency metrics: zipf, frequency, dispersion_cd, level, difficulty, difficulty_numeric
    RUNS THE ACTUAL SCRIPT ON REAL DATA
    """
    print("[API] Trigger received: metadata pipeline", flush=True)
    
    if not conn:
        print("[API] ERROR: Database not connected", flush=True)
        return {"status": "error", "message": "Database not connected"}
    
    print("[API] Calling run_metadata_pipeline with real database connection...", flush=True)
    
    # CALL THE REAL FUNCTION
    result = await run_metadata_pipeline(conn)
    
    print(f"[API] Pipeline completed: {result}", flush=True)
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)