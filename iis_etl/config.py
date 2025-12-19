from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DB_DSN: str
    API_BASE_URL: str = "https://iis.bsuir.by/api/v1"
    CONCURRENCY_LIMIT: int = 5
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()