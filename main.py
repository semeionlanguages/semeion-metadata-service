from fastapi import FastAPI
from pydantic import BaseModel
import os
import psycopg2
import uvicorn

app = FastAPI()

# Load database URL from Render environment variables
DATABASE_URL = os.getenv("DATABASE_URL1")

# Create a single DB connection for the lifetime of the app
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

class MetadataRequest(BaseModel):
    hash_id: str

@app.post("/generate")
async def generate(req: MetadataRequest):
    # Fetch the English word associated with the hash_id
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
