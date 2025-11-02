import asyncio, time, uuid
import numpy as np
import pandas as pd
from typing import Optional, List
from core.settings import settings
from models.schemas import StatusResponse, TransactionEvent
from services.kafka import make_producer
from services.sdv_loader import get_synth
from services.sampling import sample_with_base_rate

class Streamer:
    def __init__(self):
        self._rng = np.random.default_rng(settings.RNG_SEED)
        self._synth = get_synth(settings.SYNTH_PATH)
        self._producer = None
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._last_sent_at: Optional[float] = None
        self._last_batch_size: Optional[int] = None

    async def start(self, interval_secs=None, fraud_rate=None, batch_min=None, batch_max=None):
        if self._running: return
        if interval_secs is not None: settings.INTERVAL_SECS = int(interval_secs)
        if fraud_rate   is not None: settings.FRAUD_RATE   = float(fraud_rate)
        if batch_min    is not None: settings.BATCH_MIN    = int(batch_min)
        if batch_max    is not None: settings.BATCH_MAX    = int(batch_max)
        if settings.BATCH_MIN > settings.BATCH_MAX:
            raise ValueError("BATCH_MIN must be <= BATCH_MAX")

        self._producer = make_producer(settings.KAFKA_BOOTSTRAP)
        await self._producer.start()
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass
            self._task = None
        if self._producer:
            await self._producer.stop()
            self._producer = None

    def _rows_to_events(self, df: pd.DataFrame) -> List[TransactionEvent]:
        events: List[TransactionEvent] = []
        v_cols = [f"V{i}" for i in range(1, 29)]  # V1..V28

        for _, r in df.iterrows():
            d = r.to_dict()

            base_kwargs = {
                "transaction_id": str(d["transaction_id"]),
                "event_time": str(d["event_time"]),
                "event_time_ms": int(d["event_time_ms"]),
                "Amount": float(d["Amount"]),
                "Class": int(d["Class"]),
            }
            v_kwargs = {c: float(d[c]) for c in v_cols}

            events.append(TransactionEvent(**base_kwargs, **v_kwargs))

        return events

    async def _run_loop(self):
        try:
            while self._running:
                n = int(self._rng.integers(settings.BATCH_MIN, settings.BATCH_MAX + 1))
                df = sample_with_base_rate(self._synth, n_rows=n, fraud_rate=settings.FRAUD_RATE, rng=self._rng)
                events = self._rows_to_events(df)

                now_ms = int(time.time() * 1000)
                futs = []
                for ev in events:
                    ev.source_ = "sdv"
                    ev.schema_version_ = 1
                    ev.produce_time_ms_ = now_ms
                    key = str(uuid.uuid4())
                    futs.append(self._producer.send_and_wait(settings.KAFKA_TOPIC, key=key, value=ev))
                if futs:
                    await asyncio.gather(*futs)

                self._last_sent_at = time.time()
                self._last_batch_size = len(events)
                await asyncio.sleep(settings.INTERVAL_SECS)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[streamer] error: {e}", flush=True)
            self._running = False

    def status(self) -> StatusResponse:
        return StatusResponse(
            running=self._running,
            last_sent_at_epoch=self._last_sent_at,
            last_batch_size=self._last_batch_size,
            interval_secs=settings.INTERVAL_SECS,
            fraud_rate=settings.FRAUD_RATE,
            batch_min=settings.BATCH_MIN,
            batch_max=settings.BATCH_MAX,
            topic=settings.KAFKA_TOPIC,
            bootstrap=settings.KAFKA_BOOTSTRAP,
        )
