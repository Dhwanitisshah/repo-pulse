import os
import uuid

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_addr: str = "localhost:6379"
    cors_origins: str = "http://localhost:5173"
    stream_name: str = "events"
    max_events: int = 50
    consumer_group: str = "pulse-workers"
    consumer_name: str = os.environ.get("HOSTNAME") or f"api-{uuid.uuid4().hex[:8]}"

    # Anomaly detection (see app/anomaly.py): rolling mean/stddev over the
    # existing minute-bucket aggregates, no new raw-event storage.
    anomaly_baseline_minutes: int = 30  # how many past buckets form the baseline
    anomaly_min_baseline: int = 10  # need >= this many non-empty baseline buckets before detecting (warm-up guard)
    anomaly_sigma: float = 3.0  # flag when current > mean + sigma * stddev
    anomaly_min_absolute: int = 5  # ignore spikes below this absolute count (low-traffic noise guard)
    anomaly_cooldown_minutes: int = 5  # suppress re-firing the same (repo, scope, kind) within this window

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


settings = Settings()
