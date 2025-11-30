from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import psycopg2
import uvicorn
from dotenv import load_dotenv
import subprocess
import uuid

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
except Exception as e:
    print(f"[ERROR] DB: {e}", flush=True)
    conn = None

@app.get("/")
async def root():
    return {"service": "semeion-metadata-service", "status": "running"}

@app.get("/health")
async def health():
    return {"status": "healthy", "database": "connected" if conn else "disconnected"}

@app.post("/run/metadata-pipeline")
async def metadata_pipeline_endpoint():
    print("[API] === PIPELINE TRIGGERED ===", flush=True)
    
    # Check if script exists
    script_path = "scripts/generate_frequency_metrics.py"
    if not os.path.exists(script_path):
        print(f"[ERROR] Script not found: {script_path}", flush=True)
        print(f"[ERROR] Current dir: {os.getcwd()}", flush=True)
        print(f"[ERROR] Files: {os.listdir('.')}", flush=True)
        return {"status": "error", "message": f"Script not found at {script_path}"}
    
    print(f"[API] Script found, executing...", flush=True)
    
    try:
        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True,
            timeout=600
        )
        
        print(f"[API] Return code: {result.returncode}", flush=True)
        print(f"[API] STDOUT: {result.stdout}", flush=True)
        print(f"[API] STDERR: {result.stderr}", flush=True)
        
        return {
            "status": "ok" if result.returncode == 0 else "error",
            "job_id": str(uuid.uuid4()),
            "processed": "see logs",
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    except Exception as e:
        print(f"[API] EXCEPTION: {str(e)}", flush=True)
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)