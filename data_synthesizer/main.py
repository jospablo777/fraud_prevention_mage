from contextlib import asynccontextmanager
from fastapi import FastAPI
from core.settings import settings
from api.routers.stream import router as stream_router, streamer  # reuse the same instance

@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.AUTO_START:
        await streamer.start()
    try:
        yield
    finally:
        await streamer.stop()

app = FastAPI(title="SDV → Kafka Pusher", version="1.0.0", lifespan=lifespan)
app.include_router(stream_router, tags=["stream"])
