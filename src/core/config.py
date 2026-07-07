import os, pytz
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Settings:
    mongodb_uri: str
    mongodb_db: str
    mongodb_sales_uri: str
    mongodb_sales_db: str
    batch_size: int = int(os.getenv("PRICES_BATCH", "500"))
    page_size: int = int(os.getenv("MONGO_PAGE_SIZE", "200"))
    sample_limit: int = int(os.getenv("SAMPLE_LIMIT", "0"))
    timezone: str = os.getenv("TIMEZONE", "Europe/Rome")
    fx_api_url: str | None = os.getenv("FX_API_URL")
    providers_module: str | None = os.getenv("PROVIDERS_MODULE")

def load_settings() -> Settings:
    uri = os.environ["MONGODB_URI"]
    db = os.environ["MONGODB_DB"]
    sales_uri = os.environ.get("MONGODB_SALES_URI") or uri
    sales_db = os.environ.get("MONGODB_SALES_DB") or db
    return Settings(mongodb_uri=uri, mongodb_db=db, mongodb_sales_uri=sales_uri, mongodb_sales_db=sales_db)

ROME = pytz.timezone("Europe/Rome")