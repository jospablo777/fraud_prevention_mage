from fastapi import APIRouter, HTTPException
from models.schemas import StartRequest, StatusResponse
from services.streamer import Streamer

router = APIRouter()
streamer = Streamer()  # one instance backing the API

@router.get("/health")
def health():
    return {"ok": True}

@router.get("/status", response_model=StatusResponse)
def status():
    return streamer.status()

@router.post("/start", response_model=StatusResponse)
async def start(req: StartRequest):
    try:
        await streamer.start(
            interval_secs=req.interval_secs,
            fraud_rate=req.fraud_rate,
            batch_min=req.batch_min,
            batch_max=req.batch_max,
        )
        return streamer.status()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/stop", response_model=StatusResponse)
async def stop():
    await streamer.stop()
    return streamer.status()
