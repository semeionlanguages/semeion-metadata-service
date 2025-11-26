from fastapi import FastAPI
from pydantic import BaseModel
import os
import psycopg2
import uvicorn

app = FastAPI()

# Retrieve DB URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

# Connect once at startup
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

class Request(BaseModel):
    hash_id: str

@app.post("/generate")
async def generate(req: Request):
    # Example: Fetch English word for debugging
    cursor.execute("SELECT en FROM canonical_lexicon WHERE hash_id = %s", (req.hash_id,))
    row = cursor.fetchone()

    return {
        "status": "ok",
        "hash_id": req.hash_id,
        "word_en": row[0] if row else None
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
