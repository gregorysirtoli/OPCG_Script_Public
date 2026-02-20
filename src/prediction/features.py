from __future__ import annotations

import re
import numpy as np
import pandas as pd

def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = name.lower()
    s = re.sub(r"\[.*?\]", " ", s)          # rimuove [..]
    s = re.sub(r"[^a-z0-9]+", " ", s)       # solo alfanumerico
    s = re.sub(r"\s+", " ", s).strip()
    return s

def first_or_empty(x):
    if isinstance(x, list):
        return x[0] if x else ""
    return x if x is not None else ""

def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    b2 = b.replace(0, np.nan)
    return a / b2

def assign_tier(price: float, low_max: float, mid_max: float) -> str:
    if price < low_max:
        return "low"
    if price <= mid_max:
        return "mid"
    return "high"
