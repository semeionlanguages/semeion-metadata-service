from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class GenerateRequest(BaseModel):
    hash_id: str


class GenerateResponse(BaseModel):
    status: str
    hash_id: str


@app.post("/generate", response_model=GenerateResponse)
async def generate(request: GenerateRequest):
    return GenerateResponse(status="ok", hash_id=request.hash_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
