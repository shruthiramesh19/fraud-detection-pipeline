from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "Fraud Detection API"
    app_version: str = "0.1.0"

    database_url: str
    kafka_bootstrap_servers: str = "kafka:9092"
    alert_topic: str = "transactions.alerts"
    openai_api_key: str = ""


settings = Settings()