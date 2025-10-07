from __future__ import annotations
import os, time, random, json
from typing import Any, Optional, Tuple, Dict
import requests

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = rate_per_sec
        self.capacity = burst
        self.tokens = burst
        self.ts = time.monotonic()

    def acquire(self):
        now = time.monotonic()
        delta = now - self.ts
        self.ts = now
        self.tokens = min(self.capacity, self.tokens + delta * self.rate)
        if self.tokens < 1:
            sleep_for = (1 - self.tokens) / self.rate
            time.sleep(sleep_for)
            self.tokens = 0
        else:
            self.tokens -= 1
            time.sleep(random.uniform(0.02, 0.08))

def get_fx_eur_usd(fx_api_url: Optional[str]) -> float:
    if not fx_api_url:
        return float(os.getenv("FX_FIXED_RATE", "1.15"))
    try:
        r = requests.get(fx_api_url, timeout=20)
        r.raise_for_status()
        data = r.json()
        # aspettiamo un payload tipo {"rates":{"USD":1.07}} o simile
        rates = data.get("rates") or {}
        return float(rates.get("USD"))
    except Exception:
        return float(os.getenv("FX_FIXED_RATE", "1.15"))

def eur_to_usd(v: Optional[float], fx: float) -> Optional[float]:
    if v is None:
        return None
    return round(float(v) * fx, 4)

def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x)
    try:
        return float(s)
    except Exception:
        return None