from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import psycopg2
import uvicorn
from dotenv import load_dotenv
import uuid

# Load environment variables
load_dotenv()

app = FastAPI(title="Semeion Metadata Service")

# CORS Configuration - Allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative dev port
        "http://localhost:8080",  # Self
        "https://semeion-app.vercel.app",  # Production frontend
        "*",  # Allow all origins (for development)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL1")

# Global DB connection (do NOT recreate on each request)
try:
    conn = psycopg2.connect(DATABASE_URL)
except Exception as e:
    print(f"Database connection failed: {e}")
    conn = None

class MetadataRequest(BaseModel):
    hash_id: str

@app.get("/")
async def root():
    return {"service": "semeion-metadata-service", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "semeion-metadata-service"}

@app.post("/generate")
async def generate(req: MetadataRequest):
    if not conn:
        return {"status": "error", "message": "Database not connected"}
    
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

@app.post("/run/metadata-pipeline")
async def metadata_pipeline_endpoint():
    """
    Generate frequency metrics: zipf, frequency, dispersion_cd, level, difficulty, difficulty_numeric
    """
    print("[API] Trigger received: metadata pipeline", flush=True)
    
    # TODO: Import and call the actual pipeline when ready
    # from scripts.generate_frequency_metrics import run_metadata_pipeline
    # result = await run_metadata_pipeline(conn)
    
    # For now, return a mock response
    job_id = str(uuid.uuid4())
    
    return {
        "status": "ok",
        "job_id": job_id,
        "processed": 0,
        "message": "Frequency metrics pipeline executed successfully (placeholder)"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)