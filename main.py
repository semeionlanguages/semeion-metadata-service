from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import sys
import psycopg2
import uvicorn
from dotenv import load_dotenv
import uuid
import asyncio
from io import StringIO

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
    print("[STARTUP] Database connected", flush=True)
except Exception as e:
    print(f"[STARTUP] DB Error: {e}", flush=True)
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
    
    if not conn:
        print("[API] ERROR: No database connection", flush=True)
        return {"status": "error", "message": "Database not connected"}
    
    try:
        # Import the actual pipeline function
        sys.path.insert(0, os.path.dirname(__file__))
        from scripts.generate_frequency_metrics import run_metadata_pipeline
        
        print("[API] Executing pipeline...", flush=True)
        
        # Capture stdout to return logs to frontend
        captured_output = StringIO()
        old_stdout = sys.stdout
        
        try:
            # Redirect stdout to capture print statements
            sys.stdout = captured_output
            
            # IMPORTANT: Call with await since it's async
            result = await run_metadata_pipeline(conn)
            
        finally:
            # Restore original stdout
            sys.stdout = old_stdout
        
        # Get captured logs
        logs = captured_output.getvalue()
        
        print(f"[API] Pipeline completed: {result}", flush=True)
        
        # Add logs to the result
        if isinstance(result, dict):
            result["stdout"] = logs
            result["logs"] = logs.split('\n')  # Also provide as array for easier frontend parsing
        
        return result
        
    except ImportError as e:
        print(f"[API] Import Error: {e}", flush=True)
        return {"status": "error", "message": f"Script import failed: {str(e)}"}
    except Exception as e:
        print(f"[API] Execution Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

@app.post("/run/polarity-pipeline")
async def polarity_pipeline_endpoint():
    print("[API] === POLARITY PIPELINE TRIGGERED ===", flush=True)
    
    if not conn:
        print("[API] ERROR: No database connection", flush=True)
        return {"status": "error", "message": "Database not connected"}
    
    try:
        # Import the polarity pipeline function
        sys.path.insert(0, os.path.dirname(__file__))
        from scripts.generate_polarity import run_polarity_pipeline
        
        print("[API] Executing polarity pipeline...", flush=True)
        
        # Capture stdout to return logs to frontend
        captured_output = StringIO()
        old_stdout = sys.stdout
        
        try:
            # Redirect stdout to capture print statements
            sys.stdout = captured_output
            
            # IMPORTANT: Call with await since it's async
            result = await run_polarity_pipeline(conn)
            
        finally:
            # Restore original stdout
            sys.stdout = old_stdout
        
        # Get captured logs
        logs = captured_output.getvalue()
        
        print(f"[API] Polarity pipeline completed: {result}", flush=True)
        
        # Add logs to the result
        if isinstance(result, dict):
            result["stdout"] = logs
            result["logs"] = logs.split('\n')  # Also provide as array for easier frontend parsing
        
        return result
        
    except ImportError as e:
        print(f"[API] Import Error: {e}", flush=True)
        return {"status": "error", "message": f"Script import failed: {str(e)}"}
    except Exception as e:
        print(f"[API] Execution Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

@app.post("/run/register-pipeline")
async def register_pipeline_endpoint():
    print("[API] === REGISTER PIPELINE TRIGGERED ===", flush=True)
    
    if not conn:
        print("[API] ERROR: No database connection", flush=True)
        return {"status": "error", "message": "Database not connected"}
    
    try:
        # Import the register pipeline function
        sys.path.insert(0, os.path.dirname(__file__))
        from scripts.generate_register import run_register_pipeline
        
        print("[API] Starting register pipeline in background...", flush=True)
        
        # Start pipeline in background (don't await)
        asyncio.create_task(run_register_pipeline(conn))
        
        # Return immediately with job started message
        return {
            "status": "started",
            "message": "Register pipeline started in background. Check Render logs for progress.",
            "note": "This is a long-running job (~15 hours for 36k entries). Monitor progress via metadata_progress table or Render logs."
        }
        
    except ImportError as e:
        print(f"[API] Import Error: {e}", flush=True)
        return {"status": "error", "message": f"Script import failed: {str(e)}"}
    except Exception as e:
        print(f"[API] Execution Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)