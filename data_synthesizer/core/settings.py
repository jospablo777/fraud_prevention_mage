from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP: str = "localhost:9092"
    KAFKA_TOPIC: str = "creditcard-transactions"
    SYNTH_PATH: str = "artifacts/creditcard_fraud_gc.pkl"
    FRAUD_RATE: float = 0.001727
    INTERVAL_SECS: int = 20
    BATCH_MIN: int = 500
    BATCH_MAX: int = 6000
    RNG_SEED: int | None = None
    AUTO_START: bool = True

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
