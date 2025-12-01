from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import os
import sys
import psycopg2
import uvicorn
from dotenv import load_dotenv
import uuid
import asyncio
from io import StringIO
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
    print("[STARTUP] Database connected", flush=True)
except Exception as e:
    print(f"[STARTUP] DB Error: {e}", flush=True)
    conn = None

def get_db_connection():
    """Get database connection, reconnecting if necessary"""
    global conn
    try:
        # Check if connection is closed
        if conn is None or conn.closed:
            print("[DB] Reconnecting to database...", flush=True)
            conn = psycopg2.connect(DATABASE_URL)
            print("[DB] Reconnected successfully", flush=True)
        return conn
    except Exception as e:
        print(f"[DB] Connection error: {e}", flush=True)
        return None

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

@app.get("/run/register-pipeline/stream")
async def register_pipeline_stream():
    """Stream register pipeline progress in real-time using Server-Sent Events"""
    
    async def event_generator():
        db_conn = get_db_connection()
        if not db_conn:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Database not connected'})}\n\n"
            return
        
        try:
            sys.path.insert(0, os.path.dirname(__file__))
            from scripts.generate_register import run_register_pipeline_streaming
            
            yield f"data: {json.dumps({'type': 'log', 'message': '[API] === REGISTER PIPELINE STARTED ==='})}\n\n"
            
            batch_count = 0
            total_processed = 0
            
            # Auto-loop: process batches until all done
            while True:
                batch_count += 1
                yield f"data: {json.dumps({'type': 'log', 'message': f'[API] Starting batch {batch_count}...'})}\n\n"
                
                # Process batch and stream logs
                async for log_message in run_register_pipeline_streaming(db_conn, batch_size=500):
                    yield f"data: {json.dumps({'type': 'log', 'message': log_message})}\n\n"
                    
                    # Check if this was the completion message
                    if "nothing_to_process" in log_message or "DONE" in log_message:
                        if "nothing_to_process" in log_message:
                            yield f"data: {json.dumps({'type': 'log', 'message': f'[API] All entries processed! Total batches: {batch_count - 1}'})}\n\n"
                            yield f"data: {json.dumps({'type': 'complete', 'total_processed': total_processed, 'batches': batch_count - 1})}\n\n"
                            return
                        else:
                            total_processed += 500  # Approximate
                            yield f"data: {json.dumps({'type': 'log', 'message': f'[API] Batch {batch_count} complete. Total: {total_processed}'})}\n\n"
                            break
                
                # Small delay between batches
                await asyncio.sleep(0.5)
                
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/run/register-pipeline")
async def register_pipeline_endpoint():
    """Start register pipeline in background - returns immediately"""
    print("[API] === REGISTER PIPELINE TRIGGERED ===", flush=True)
    
    db_conn = get_db_connection()
    if not db_conn:
        print("[API] ERROR: No database connection", flush=True)
        return {"status": "error", "message": "Database not connected"}
    
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from scripts.generate_register import run_register_pipeline
        
        # Start pipeline in background
        asyncio.create_task(register_background_task(db_conn))
        
        return {
            "status": "started",
            "message": "Register pipeline started in background",
            "stream_url": "/run/register-pipeline/stream",
            "note": "Use the stream endpoint to see live progress"
        }
        
    except ImportError as e:
        print(f"[API] Import Error: {e}", flush=True)
        return {"status": "error", "message": f"Script import failed: {str(e)}"}
    except Exception as e:
        print(f"[API] Execution Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

async def register_background_task(db_conn):
    """Background task for register pipeline"""
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from scripts.generate_register import run_register_pipeline
        
        batch_count = 0
        total_processed = 0
        
        while True:
            batch_count += 1
            print(f"[BACKGROUND] Starting batch {batch_count}...", flush=True)
            
            result = await run_register_pipeline(db_conn, batch_size=500)
            
            if result.get("status") == "nothing_to_process":
                print(f"[BACKGROUND] All entries processed! Total batches: {batch_count - 1}", flush=True)
                break
            
            total_processed += result.get("processed", 0)
            print(f"[BACKGROUND] Batch {batch_count} complete. Total: {total_processed}", flush=True)
            
            await asyncio.sleep(0.5)
            
    except Exception as e:
        print(f"[BACKGROUND] Error: {e}", flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)