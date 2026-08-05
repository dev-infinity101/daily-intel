from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve .env relative to this file so it loads correctly regardless of CWD
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+asyncpg://intel:intel_dev@localhost:5432/intel"
    tensormux_api_key: str = ""
    tensormux_model: str = "north-mini-code"
    resend_api_key: str = ""
    email_from: str = "intel@example.com"
    email_to: str = ""  # comma-separated list, parsed at send time
    apify_token: str = ""
    apify_token_secondary: str = ""
    apify_webhook_secret: str = ""
    changedetection_webhook_secret: str = ""
    ingest_token: str = "dev-token"
    tz: str = "Asia/Kolkata"
    # V3 — Browserbase
    browserbase_api_key: str = ""
    browserbase_project_id: str = ""
    browserbase_api_key_secondary: str = ""
    browserbase_project_id_secondary: str = ""
    # V3 — monthly budget guards (soft limits; 80% threshold triggers stop)
    apify_monthly_cu_limit: int = 1000          # free tier ~1 000 CU/month
    browserbase_monthly_minutes_limit: int = 60  # free tier ~60 min/month
    # Job lifecycle — mark jobs stale after this many days without re-appearing
    job_stale_days: int = 21
    # News module — changedetection.io (Module 2)
    changedetection_url: str = "http://localhost:5000"
    changedetection_api_key: str = ""
    # Host/port FastAPI is reachable at FROM the changedetection.io container.
    # In Docker Compose on Linux use "host-gateway"; on Mac/Windows use "host.docker.internal".
    changedetection_webhook_host: str = "host.docker.internal"
    changedetection_webhook_port: int = 8000
    # News module — Apify actor IDs (Modules 3 & 3b), swappable without code changes
    apify_twitter_actor_id: str = "apidojo/twitter-user-scraper"
    apify_linkedin_actor_id: str = "harvestapi/linkedin-post-search"
    # Adzuna API credentials
    adzuna_app_id: str = ""
    adzuna_app_key: str = ""
    # LinkedIn Jobs Apify scraper (valig/linkedin-jobs-scraper)
    linkedin_jobs_apify_actor_id: str = "valig/linkedin-jobs-scraper"
    linkedin_jobs_apify_title: str = "deputy manager"
    linkedin_jobs_apify_location: str = "india"
    linkedin_jobs_apify_date_posted: str = "r604800"
    linkedin_jobs_apify_limit: int = 50


settings = Settings()
