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

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


settings = Settings()
