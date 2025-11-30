from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import psycopg2
import uvicorn
from dotenv import load_dotenv
import subprocess
import json

load_dotenv()

app = FastAPI(title="Semeion Metadata Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL1")

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

@app.post("/run/metadata-pipeline")
async def metadata_pipeline_endpoint():
    """
    Generate frequency metrics by running the Python script directly
    """
    print("[API] Trigger received: metadata pipeline", flush=True)
    
    if not conn:
        return {"status": "error", "message": "Database not connected"}
    
    try:
        # Run the script as a subprocess
        print("[API] Running generate_frequency_metrics.py...", flush=True)
        result = subprocess.run(
            ["python", "scripts/generate_frequency_metrics.py"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        print(f"[API] Script output: {result.stdout}", flush=True)
        if result.stderr:
            print(f"[API] Script errors: {result.stderr}", flush=True)
        
        if result.returncode == 0:
            return {
                "status": "ok",
                "job_id": "subprocess-run",
                "processed": "check logs",
                "message": "Script executed successfully",
                "output": result.stdout
            }
        else:
            return {
                "status": "error",
                "message": f"Script failed with code {result.returncode}",
                "error": result.stderr
            }
    except Exception as e:
        print(f"[API] ERROR: {str(e)}", flush=True)
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)