"""
INPUT (MongoDB esistente)
- Cards: dati statici carta (id, name, rarityName, printing, alternate, color, setId, releaseDate, ...)
- Prices: storico prezzi giornaliero (itemId, createdAt, pricePrimary, sellers, listings, ...)

OBIETTIVO
- Predire l’andamento a 28 giorni (4 settimane) in termini di “rendimento” atteso:
  return_28d = (price_t+28 - price_t) / price_t
- Non si predice il prezzo assoluto: si predice la distribuzione del rendimento (quantili).

PRE-PROCESSING
1) Prices -> daily time series:
   - raggruppa per itemId + giorno (date), prende ultimo record del giorno
   - reindex giornaliero continuo + forward-fill (gestione buchi)
2) Esclusione carte con storico insufficiente:
   - min 12 settimane ~ 84 giorni di dati
3) Feature Engineering (da Prices):
   - ret_7d, ret_14d, ret_28d, ret_56d
   - vol_28d (volatilità su log returns)
   - mom_14d (momentum)
   - sellers_chg_28d, listings_chg_28d
   - price_to_listings, sellers_to_listings
4) Feature statiche (da Cards):
   - rarityName, printing, alternate, color, setId, card_age_weeks (da releaseDate)

DNA CLUSTERING
- Clustering carte per “famiglie” simili usando: rarityName, printing, alternate, color, setId, age
- Output: clusterId (usato anche come feature del modello)

MODELLO (per fasce prezzo)
- Tier (calcolato su priceNow):
  - low: < 5€
  - mid: 5 - 150€
  - high: > 150€
- Modelli separati per tier.
- Tipo modello: Quantile Regression (LightGBM) per stimare la distribuzione del rendimento a 28 giorni:
  - q20 = downside realistico (scenario negativo plausibile)
  - q50 = mediana (scenario tipico)
  - q80 = upside realistico (scenario positivo plausibile)

OUTPUT (nuove collection MongoDB, create automaticamente al primo insert)
1) ml_predictions_daily (1 doc per cardId, aggiornato ad ogni run):
   - cardId: ID carta (Cards.id / Prices.itemId)
   - asOfDate: timestamp generazione predizione
   - lastPriceDate: data ultimo prezzo usato
   - tier: low|mid|high
   - priceNow: ultimo prezzo usato (da Prices.pricePrimary)
   - clusterId: DNA cluster della carta
   - pred_q20_28d: quantile 20% rendimento 28d (es. -0.08 = -8%)
   - pred_q50_28d: quantile 50% rendimento 28d
   - pred_q80_28d: quantile 80% rendimento 28d

2) ml_rankings (1 doc per run, per la UI):
   - asOfDate: timestamp run
   - top_up: [cardId...] ordinati per pred_q80_28d DESC (migliori)
   - top_down: [cardId...] ordinati per pred_q20_28d ASC (peggiori)
   - meta: { horizon_days=28, min_history_days=84, tiers... }

UTILIZZO SITO
- Pagina “Top”: legge ml_rankings (ultimo doc) + join con Cards.
- Pagina “Carta”: legge ml_predictions_daily per cardId e mostra upside/mediana/downside a 28 giorni.
"""


from dataclasses import dataclass
from dotenv import load_dotenv
import os
import traceback

@dataclass(frozen=True)
class MongoConfig:
    
   # ============================================================
   # ENV
   # ============================================================
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

   # nuove collezioni
   col_pred: str = "ml_predictions_daily"
   col_rank: str = "ml_rankings"
   col_meta: str = "ml_models_meta"

@dataclass(frozen=True)
class MLConfig:
    horizon_days: int = 28 # 4 weeks
    min_history_days: int = 84 # 12 weeks
    n_clusters: int = 30

    # tier boundaries
    low_max: float = 5.0
    mid_max: float = 150.0

    # quantili
    quantiles: tuple[float, ...] = (0.2, 0.5, 0.8)

    # rolling windows (in giorni)
    win_ret_1: int = 7
    win_ret_2: int = 14
    win_ret_3: int = 28
    win_ret_4: int = 56

    win_vol: int = 28
    win_mom: int = 14
    win_liq: int = 28
