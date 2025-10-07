import os, pytz
from dataclasses import dataclass

@dataclass
class Settings:
    mongodb_uri: str
    mongodb_db: str
    batch_size: int = int(os.getenv("PRICES_BATCH", "500"))
    page_size: int = int(os.getenv("MONGO_PAGE_SIZE", "200"))
    sample_limit: int = int(os.getenv("SAMPLE_LIMIT", "0"))
    timezone: str = os.getenv("TIMEZONE", "Europe/Rome")
    fx_api_url: str | None = os.getenv("FX_API_URL")
    providers_module: str | None = os.getenv("PROVIDERS_MODULE")

def load_settings() -> Settings:
    uri = os.environ["MONGODB_URI"]
    db = os.environ["MONGODB_DB"]
    return Settings(mongodb_uri=uri, mongodb_db=db)

ROME = pytz.timezone("Europe/Rome")