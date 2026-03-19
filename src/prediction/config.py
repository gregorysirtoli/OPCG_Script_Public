"""
INPUT (MongoDB esistente)
- Cards: dati statici carta (id, name, rarityName, printing, alternate, color, setId, releaseDate, ...)
- Prices: storico prezzi giornaliero (itemId, createdAt, pricePrimary, sellers, listings, ...)

OBIETTIVO
- Predire l'andamento a 28 giorni (4 settimane) in termini di rendimento atteso:
  return_28d = (price_t+28 - price_t) / price_t
- Non si predice il prezzo assoluto: si predice la distribuzione del rendimento (quantili).

PRE-PROCESSING
1) Prices -> daily time series:
   - raggruppa per itemId + giorno (date), prende ultimo record del giorno
   - reindex giornaliero continuo + forward-fill limitato
2) Esclusione carte con storico insufficiente:
   - min 12 settimane ~ 84 giorni di dati
3) Feature Engineering (da Prices):
   - ret_7d, ret_14d, ret_28d, ret_56d
   - vol_28d (volatilita su log returns)
   - mom_14d (momentum)
   - sellers_chg_28d, listings_chg_28d
   - price_to_listings, sellers_to_listings
   - days_since_observed / is_observed
4) Feature statiche (da Cards):
   - rarityName, printing, alternate, color, setId, card_age_weeks (da releaseDate)

DNA CLUSTERING
- Clustering carte per famiglie simili usando: rarityName, printing, alternate, color, setId, age
- Output: clusterId (usato anche come feature del modello)
- Il fit va fatto su carte uniche, non sulle righe giornaliere duplicate del training set.

MODELLO (per fasce prezzo)
- Tier (calcolato su priceNow):
  - low: < 5 EUR
  - mid: 5 - 150 EUR
  - high: > 150 EUR
- Modelli separati per tier.
- Tipo modello: Quantile Regression (LightGBM) per stimare la distribuzione del rendimento a 28 giorni:
  - q20 = downside realistico (scenario negativo plausibile)
  - q50 = mediana (scenario tipico)
  - q80 = upside realistico (scenario positivo plausibile)
"""


from dataclasses import dataclass
from dotenv import load_dotenv
import os


@dataclass(frozen=True)
class MongoConfig:
    load_dotenv(".env.local")
    load_dotenv()

    REQUIRED = ["MONGODB_URI", "MONGODB_DB"]
    missing = [k for k in REQUIRED if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    uri: str = os.environ["MONGODB_URI"]
    db_name: str = os.environ["MONGODB_DB"]
    col_cards: str = "Cards"
    col_prices: str = "Prices"
    col_sets: str = "Sets"
    col_pred: str = "ml_predictions_daily"
    col_rank: str = "ml_rankings"
    col_meta: str = "ml_models_meta"


@dataclass(frozen=True)
class MLConfig:
    horizon_days: int = 28
    min_history_days: int = 84
    n_clusters: int = 30
    max_ffill_days: int = 7

    low_max: float = 5.0
    mid_max: float = 150.0

    quantiles: tuple[float, ...] = (0.2, 0.5, 0.8)

    win_ret_1: int = 7
    win_ret_2: int = 14
    win_ret_3: int = 28
    win_ret_4: int = 56

    win_vol: int = 28
    win_mom: int = 14
    win_liq: int = 28
