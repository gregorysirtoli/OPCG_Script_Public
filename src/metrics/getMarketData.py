from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from math import exp, log1p, sqrt, tanh
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from pymongo import InsertOne, UpdateOne
from pymongo.database import Database
from pymongo.errors import ExecutionTimeout

from src.core.prices_graded import attach_graded
from src.trackrecord.ledger import (
    TRACK_RECORD_CALLS_COLLECTION,
    TRACK_RECORD_MIN_PRICE_USD,
    accept_call,
    build_call_event,
    insert_append_only,
    recent_calls_index,
)

GRADING_FEES = 80
SET_TREND_DAYS = 90
SET_PRICE_LOOKBACK_DAYS = max(SET_TREND_DAYS + 15, 45)
SET_HISTORY_LOOKBACK_DAYS = 45
SET_TOP_BASE_PRICE = 20.0
SET_MARKET_HISTORY_COLLECTION = "SetsMarketData"
CARD_STAMPS_COLLECTION = "CardsStamps"
CARD_STAMP_CHANGES_COLLECTION = "CardsStampChanges"
CHARACTER_POPULARITY_COLLECTION = "wt100"
SET_MARKET_HISTORY_FIELDS = (
    "cardsCount",
    "otherTypesCount",
    "totalCount",
    "totalCardsPrice",
    "totalOtherTypesPrice",
    "totalPrice",
    "totalPriceChange7d",
    "totalPriceChange30d",
    "psa10CardsCount",
    "psa10TotalPrice",
    "bgs10CardsCount",
    "bgs10TotalPrice",
    "profitableCards",
    "expectedValue",
    "realisticExpectedValue",
    "roi",
    "volatility",
)

PRICE_BAND_RULES = (
    {
        "key": "UNDER_10",
        "label": "Under $10 band",
        "min": 0.0,
        "max": 10.0,
        "proofWinRate8w": 75,
        "proofMedianGain8w": 17,
    },
    {
        "key": "USD_10_50",
        "label": "$10-$50 band",
        "min": 10.0,
        "max": 50.0,
        "proofWinRate8w": 80,
        "proofMedianGain8w": 25,
    },
    {
        "key": "USD_50_200",
        "label": "$50-$200 band",
        "min": 50.0,
        "max": 200.0,
        "proofWinRate8w": 82,
        "proofMedianGain8w": 32,
    },
    {
        "key": "USD_200_PLUS",
        "label": "$200+ band",
        "min": 200.0,
        "max": None,
        "proofWinRate8w": 84,
        "proofMedianGain8w": 21,
    },
)

BUY_TIER_RULES = {
    "HIGH_CONVICTION": {
        "label": "High Conviction",
        "proofWinRate8w": 89,
        "proofMedianGain8w": 22.2,
        "side": "buy",
    },
    "EVENT_RUNNER": {
        "label": "Event Runner",
        "proofWinRate8w": 74,
        "proofMedianGain8w": 15.3,
        "side": "buy",
    },
    "RIDING_THE_HIGH": {
        "label": "Riding the High",
        "proofWinRate8w": 78,
        "proofMedianGain8w": 13.8,
        "side": "buy",
    },
    "PREMIUM_CLIMB": {
        "label": "Premium Climb",
        "proofWinRate8w": 81,
        "proofMedianGain8w": 12.1,
        "side": "buy",
    },
    "SUPPLY_SQUEEZE": {
        "label": "Supply Squeeze",
        "proofWinRate8w": None,
        "proofMedianGain8w": None,
        "side": "buy",
    },
    "MOMENTUM": {
        "label": "Momentum",
        "proofWinRate8w": 64,
        "proofMedianGain8w": 11.7,
        "side": "buy",
    },
    "ROLLOVER": {
        "label": "Rollover",
        "proofWinRate8w": None,
        "proofMedianGain8w": None,
        "side": "sell",
    },
    "HARD_DROP": {
        "label": "Hard Drop",
        "proofWinRate8w": None,
        "proofMedianGain8w": None,
        "side": "sell",
    },
}

STAMP_RULES = {
    "ACCUMULATE": {
        "label": "Accumulate",
        "kind": "buy",
    },
    "HOLD": {
        "label": "Hold",
        "kind": "default",
    },
    "SELL_WATCH": {
        "label": "Sell Watch",
        "kind": "warning",
    },
    "REBOUND_WATCH": {
        "label": "Rebound Watch",
        "kind": "speculative",
    },
    "BUY_SETUP": {
        "label": "Buy Setup",
        "kind": "supply",
    },
}


def _normalize_name_for_match(value: Any) -> str:
    # lowercase + solo token alfanumerici, con spazi ai bordi: cosi' "Monkey.D.Luffy"
    # e "Monkey D. Luffy" combaciano e "Ace" non matcha dentro "Space"/"Palace".
    tokens = re.findall(r"[a-z0-9]+", str(value or "").lower())
    return f" {' '.join(tokens)} " if tokens else ""


def _load_character_popularity(db: Database) -> List[Tuple[str, int]]:
    """Ritorna [(nome normalizzato, position)] dalla collection wt100, ordinato per
    position crescente (1 = personaggio piu' popolare)."""
    out: List[Tuple[str, int]] = []
    for doc in db[CHARACTER_POPULARITY_COLLECTION].find({}, {"name": 1, "position": 1}):
        # alias tra parentesi fuori dal match: "Mr.2.Bon.Kurei(Bentham)" -> "Mr.2.Bon.Kurei"
        needle = _normalize_name_for_match(
            re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", str(doc.get("name") or ""))
        )
        position = _to_number(doc.get("position"))
        if not needle or position is None:
            continue
        out.append((needle, int(position)))
    out.sort(key=lambda x: x[1])
    return out


def _match_character_popularity(
    card_name: Any,
    characters: List[Tuple[str, int]],
) -> Optional[int]:
    # Se il nome carta contiene piu' personaggi (es. "Ace & Sabo & Luffy") vince
    # quello con la position migliore (piu' bassa).
    if not characters:
        return None
    # il contenuto delle [] (variante, evento, set di provenienza) non conta per il match
    haystack = _normalize_name_for_match(re.sub(r"\[[^\]]*\]", " ", str(card_name or "")))
    if not haystack:
        return None
    for needle, position in characters:
        if needle in haystack:
            return position
    return None


def _bulk_write_resilient(
    collection: Any,
    operations: List[Any],
    *,
    ordered: bool = False,
    initial_chunk_size: int = 1000,
    min_chunk_size: int = 25,
) -> Tuple[int, int]:
    if not operations:
        return (0, 0)

    chunk_size = max(1, int(initial_chunk_size))
    min_size = max(1, int(min_chunk_size))
    pending = deque(
        operations[i : i + chunk_size] for i in range(0, len(operations), chunk_size)
    )

    modified_total = 0
    upserted_total = 0

    while pending:
        batch = pending.popleft()
        try:
            res = collection.bulk_write(batch, ordered=ordered)
            modified_total += res.modified_count or 0
            upserted_total += res.upserted_count or 0
        except ExecutionTimeout:
            if len(batch) <= min_size:
                raise
            split_at = max(1, len(batch) // 2)
            pending.appendleft(batch[split_at:])
            pending.appendleft(batch[:split_at])

    return (modified_total, upserted_total)

def _clamp(n: Optional[float], low: float, high: float) -> Optional[float]:
    if n is None:
        return None
    return max(low, min(high, float(n)))

def _score_0_100(n: Optional[float]) -> Optional[float]:
    n = _clamp(n, 0.0, 100.0)
    return _round2(n)

def _scale_0_100(value: Optional[float], cap: float) -> Optional[float]:
    if value is None or cap <= 0:
        return None
    return _score_0_100((value / cap) * 100.0)

def _compute_liquidity_score(
    sellers: Any,
    listings: Any,
    listings_sources_count: int,
    spread_pct: Optional[float],
) -> Optional[float]:
    sellers_n = _to_number(sellers)
    listings_n = _to_number(listings)

    # No market depth inputs available: treat as no liquidity.
    if sellers_n is None and listings_n is None:
        return 0.0

    # Explicitly no active supply: liquidity should be zero.
    if sellers_n is not None and listings_n is not None and sellers_n <= 0 and listings_n <= 0:
        return 0.0

    # When listings come from multiple marketplaces, normalize to avoid
    # inflating liquidity just because more sources are summed together.
    sources = max(1, int(listings_sources_count or 1))
    normalized_listings = (
        (listings_n / sqrt(float(sources))) if listings_n is not None else None
    )

    sellers_score = _scale_0_100(sellers_n, 20.0)
    listings_score = _scale_0_100(normalized_listings, 40.0)
    spread_score = None
    has_depth_signal = (sellers_score is not None) or (listings_score is not None)
    if spread_pct is not None and has_depth_signal:
        # lower spread is generally healthier / easier to exit
        spread_score = _score_0_100(100.0 - min(max(spread_pct, 0.0), 40.0) * 2.5)

    weighted_sum = 0.0
    total_weight = 0.0
    for score, weight in (
        (sellers_score, 0.45),
        (listings_score, 0.35),
        (spread_score, 0.20),
    ):
        if score is None:
            continue
        weighted_sum += score * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return _round2(weighted_sum / total_weight)

def _compute_weighted_momentum(
    pct7: Optional[float],
    pct30: Optional[float],
    pct90: Optional[float],
) -> Optional[float]:
    weighted_sum = 0.0
    total_weight = 0.0
    for value, weight in ((pct7, 0.5), (pct30, 0.3), (pct90, 0.2)):
        if value is None:
            continue
        weighted_sum += value * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return _round2(weighted_sum / total_weight)

def _compute_momentum_score(weighted_momentum_pct: Optional[float]) -> Optional[float]:
    if weighted_momentum_pct is None:
        return None
    # map roughly -50%..+50% to 0..100 and clamp
    return _score_0_100(((weighted_momentum_pct + 50.0) / 100.0) * 100.0)

def _compute_grading_attractiveness(
    gp_psa10: Optional[float],
    gp_bsg10: Optional[float],
    liquidity_score: Optional[float],
) -> Optional[float]:
    best_profit = max(
        gp_psa10 if gp_psa10 is not None else float("-inf"),
        gp_bsg10 if gp_bsg10 is not None else float("-inf"),
    )
    if best_profit == float("-inf"):
        best_profit = None

    profit_score = None
    if best_profit is not None:
        # cap at 150% upside for scoring
        profit_score = _score_0_100((max(best_profit, 0.0) / 150.0) * 100.0)

    # If we do not have any grading profit reference, this card should not
    # surface as a grading candidate just because it has good liquidity.
    if profit_score is None:
        return None

    weighted_sum = 0.0
    total_weight = 0.0
    for score, weight in ((profit_score, 0.75), (liquidity_score, 0.25)):
        if score is None:
            continue
        weighted_sum += score * weight
        total_weight += weight

    if total_weight == 0:
        return None
    return _round2(weighted_sum / total_weight)

def _to_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, dict) and "$numberDecimal" in v:
        try:
            return float(v["$numberDecimal"])
        except Exception:
            return None
    try:
        return float(v)
    except Exception:
        return None

def _to_positive_number(v: Any) -> Optional[float]:
    n = _to_number(v)
    if n is None or n <= 0:
        return None
    return n

def _round2(n: Optional[float]) -> Optional[float]:
    if n is None:
        return None
    if not isinstance(n, (int, float)):
        return None
    return float(f"{n:.2f}")


# Con vendite reali trend e medie Cardmarket non coincidono mai tutte: se sono identiche il valore
# e' un riempitivo di Cardmarket per un prodotto senza transazioni (es. promo CS 25-26 a $67 con
# low a $195k e PSA 10 a $130k).
CM_NO_SALES_FIELDS = ("cmPriceTrend", "cmPriceAvg", "cmAvg1d", "cmAvg7d", "cmAvg30d")
# Low e trend Cardmarket oltre questo rapporto (in un senso o nell'altro) non descrivono lo stesso
# mercato (es. Uta Treasure Cup 2025: trend $3.3k da vendite reali, low a $112k): spread null.
# Usato anche da _cardmarket_low_as_price.
CM_LOW_TREND_MAX_RATIO = 10.0
# Gradati (campi di PricesGraded) che possono confermare cmPriceLow in _cardmarket_low_as_price, e
# chiave non salvata in cui _carry_forward_pricecharting mette il piu' alto noto fino a quel giorno.
GRADED_REFERENCE_FIELDS = ("psa10", "bsg10", "cgc10")
GRADED_MAX_KEY = "_gradedMax"
# _cardmarket_stale_contradicted: un gradato da almeno STALE_GRADED_MIN_USD che supera di oltre
# STALE_GRADED_MAX_RATIO volte il valore fermo di Cardmarket lo smentisce. Il minimo in dollari
# protegge le carte economiche, dove un PSA 10 da $50-150 sopra un raw da pochi centesimi e' normale.
STALE_GRADED_MIN_USD = 1000.0
STALE_GRADED_MAX_RATIO = 20.0
# Sotto questo prezzo (USD) il rapporto low/trend e' rumore (0.02 contro 0.25 e' gia' 12x).
CM_LOW_TREND_MIN_PRICE_USD = 5.0
# Scarti tra fonti entro questa cifra (USD) non contano come disaccordo: PriceCharting tiene le carte
# giapponesi da pochi centesimi intorno a $1.5-2.2 (ottobre 2026: con $0.5 finiva sotto 0.40 il 42%
# delle JP, con $2 l'11%; sopra $20 non cambia quasi nulla).
CONFIDENCE_ABS_TOLERANCE_USD = 2.0
# Sotto questo priceConfidenceScore niente segnali: gradingProfit, gradingAttractiveness, priceBand
# (UNKNOWN), buyTier, stamp (Hold) e chiamate del track record.
SIGNALS_MIN_PRICE_CONFIDENCE = 0.40
# Con almeno CONFIDENCE_OUTLIER_MIN_VALUES prezzi lo score puo' venire anche dall'accordo senza la fonte
# piu' lontana dalla mediana, ma cosi' non supera CONFIDENCE_OUTLIER_MAX_SCORE (sotto la soglia
# "Reliable" 0.70 del frontend). Es. Luffy Treasure Booster: Primary/PriceCharting/CardTrader
# $4-8, trend Cardmarket $17.8 -> 0.69 invece di 0.15. Con 3 fonti non si scarta: 2 contro 1 non
# basta a dire chi ha ragione (es. $495/$1.116/$1.526).
CONFIDENCE_OUTLIER_MIN_VALUES = 4
CONFIDENCE_OUTLIER_MAX_SCORE = 0.69


def _cardmarket_has_no_sales(doc: Optional[Dict[str, Any]]) -> bool:
    """True se trend e medie Cardmarket presenti (almeno 3) sono tutte identiche: Cardmarket non conta
    come fonte nello score (resta nella mediana di priceRedLine). cmPriceAvg a volte manca (es. Law
    CS 2023 TPP: trend e medie a $0.78)."""
    values = [v for v in (_to_positive_number((doc or {}).get(key)) for key in CM_NO_SALES_FIELDS) if v is not None]
    return len(values) >= 3 and max(values) - min(values) < 1e-6


def _cardmarket_low_mismatch(doc: Optional[Dict[str, Any]]) -> bool:
    """True se cmPriceLow e cmPriceTrend distano oltre CM_LOW_TREND_MAX_RATIO (spread null)."""
    trend = _to_positive_number((doc or {}).get("cmPriceTrend"))
    low = _to_positive_number((doc or {}).get("cmPriceLow"))
    if trend is None or low is None or max(trend, low) < CM_LOW_TREND_MIN_PRICE_USD:
        return False
    ratio = low / trend
    return ratio > CM_LOW_TREND_MAX_RATIO or ratio < 1.0 / CM_LOW_TREND_MAX_RATIO


def _sources_agreement(values: List[float]) -> float:
    """Accordo 0-1 tra i prezzi: pieno se ognuno sta entro il 25% dalla mediana, nullo dal 100%."""
    mid = median(values)
    dispersion = max(max(abs(v - mid) - CONFIDENCE_ABS_TOLERANCE_USD, 0.0) for v in values) / mid
    return 1.0 - min(max((dispersion - 0.25) / 0.75, 0.0), 1.0)


def _compute_price_confidence_score(
    doc: Optional[Dict[str, Any]],
    price_7d: Optional[float],
    price_30d: Optional[float],
) -> Tuple[Optional[float], Optional[str]]:
    """Punteggio 0-1 di affidabilita' di priceRedLine e motivo principale della perdita di punti.
    0.85 dalle fonti indipendenti (copertura fino a 3 fonti x accordo tra loro), 0.15 dallo storico
    a 7 e 30 giorni. Con almeno CONFIDENCE_OUTLIER_MIN_VALUES prezzi vale anche lo score senza la fonte
    piu' lontana dalla mediana, fino a CONFIDENCE_OUTLIER_MAX_SCORE.
    Cardmarket conta come una sola fonte; se senza vendite non conta nella copertura ma entra
    nell'accordo, perche' resta nella mediana di priceRedLine.
    0 = l'unico prezzo e' Cardmarket senza vendite (bassa affidabilita'); None = nessun prezzo.
    Motivo (priceConfidenceReason): cm_no_sales, few_sources, sources_disagree, no_history, None
    se lo score e' pieno.
    """
    doc = doc or {}
    cm_no_sales = _cardmarket_has_no_sales(doc)
    cardmarket = _to_positive_number(doc.get("cmPriceTrend")) or _to_positive_number(doc.get("cmAvg30d"))
    if cm_no_sales:
        cardmarket_stale, cardmarket = cardmarket, None
    else:
        cardmarket_stale = None
    sources = [
        v for v in (
            _to_positive_number(doc.get("pricePrimary")),
            cardmarket,
            _to_positive_number(doc.get("priceYuyuTei")),
            _to_positive_number(doc.get("pricePriceCharting")),
            _to_positive_number(doc.get("priceCardTrader")),
        )
        if v is not None
    ]
    if not sources:
        if cm_no_sales:
            return 0.0, "cm_no_sales"
        # solo cmPriceLow: priceRedLine c'e' ma nessuna fonte da valutare
        return None, ("few_sources" if _to_positive_number(doc.get("cmPriceLow")) is not None else None)

    priced = sources + ([cardmarket_stale] if cardmarket_stale is not None else [])
    coverage = min(len(sources), 3) / 3.0
    history = (int(_to_positive_number(price_7d) is not None) + int(_to_positive_number(price_30d) is not None)) / 2.0
    score = 0.85 * coverage * _sources_agreement(priced) + 0.15 * history
    if len(priced) >= CONFIDENCE_OUTLIER_MIN_VALUES:
        # senza la fonte piu' lontana la mediana si sposta: si tiene il migliore dei due, mai sotto lo score pieno
        mid = median(priced)
        rest = list(priced)
        rest.remove(max(priced, key=lambda v: abs(v - mid)))
        rest_score = 0.85 * coverage * _sources_agreement(rest) + 0.15 * history
        score = max(score, min(rest_score, CONFIDENCE_OUTLIER_MAX_SCORE))

    # motivo = componente che costa piu' punti
    losses = {
        "few_sources": 0.85 * (1.0 - coverage),
        "sources_disagree": 0.85 * coverage + 0.15 * history - score,
        "no_history": 0.15 * (1.0 - history),
    }
    reason = max(losses, key=losses.get)
    if losses[reason] < 1e-9:
        reason = None
    return _round2(score), reason


def _graded_reference(doc: Dict[str, Any]) -> Optional[float]:
    """Gradato piu' alto noto per la riga: GRADED_MAX_KEY se c'e', altrimenti psa10/bsg10/cgc10 della riga."""
    own = [v for v in (_to_positive_number(doc.get(key)) for key in GRADED_REFERENCE_FIELDS) if v is not None]
    return _to_positive_number(doc.get(GRADED_MAX_KEY)) or (max(own) if own else None)


def _cardmarket_only_and_stale(doc: Dict[str, Any]) -> bool:
    """Cardmarket senza vendite e nessun'altra fonte."""
    return _cardmarket_has_no_sales(doc) and not any(
        _to_positive_number(doc.get(key)) is not None
        for key in ("pricePrimary", "priceYuyuTei", "pricePriceCharting", "priceCardTrader")
    )


def _cardmarket_stale_contradicted(doc: Optional[Dict[str, Any]], price: Optional[float]) -> bool:
    """
    True se Cardmarket senza vendite e' l'unica fonte e un gradato lo smentisce (vedi STALE_GRADED_*):
    priceRedLine null. Es. Zoro CS 25-26 3rd Place: 60 EUR fermi (gli stessi della 1st Place), nessun
    annuncio, PSA 10 $35k. Con un gradato in linea (es. Law Treasure Cup 2025: $3k, PSA 10 $12.6k) il
    valore resta.
    """
    doc = doc or {}
    if price is None or not _cardmarket_only_and_stale(doc):
        return False
    graded = _graded_reference(doc)
    return graded is not None and graded >= STALE_GRADED_MIN_USD and graded > price * STALE_GRADED_MAX_RATIO


def _cardmarket_low_as_price(doc: Optional[Dict[str, Any]]) -> Optional[float]:
    """
    cmPriceLow come prezzo quando e' l'unico dato vivo: Cardmarket senza vendite, nessun'altra fonte,
    low oltre CM_LOW_TREND_MAX_RATIO volte il trend e confermato da un gradato (PSA 10, BGS 10 o CGC 10)
    non inferiore a meta' del low. Es. Zoro CS 25-26 1st Place: trend 60 EUR di riempimento, low
    175.000 EUR, PSA 10 $130k -> low. Senza conferma un low altissimo e' un annuncio da vetrina
    (es. Otama Online Regional 2023: low 90.000 EUR tondi, PSA 10 $2k) e resta la mediana.
    """
    doc = doc or {}
    if not _cardmarket_only_and_stale(doc):
        return None
    trend = _to_positive_number(doc.get("cmPriceTrend")) or _to_positive_number(doc.get("cmAvg30d"))
    low = _to_positive_number(doc.get("cmPriceLow"))
    if trend is None or low is None or low <= trend * CM_LOW_TREND_MAX_RATIO:
        return None
    graded = _graded_reference(doc)
    if graded is None or graded < low / 2.0:
        return None
    return low


def _compute_price_redline(doc: Optional[Dict[str, Any]]) -> Optional[float]:
    # Mediana di tutte le fonti, senza scarti: quanto il prezzo sia affidabile lo dice priceConfidenceScore.
    # Eccezioni solo con Cardmarket senza vendite come unica fonte: _cardmarket_low_as_price e
    # _cardmarket_stale_contradicted.
    low_price = _cardmarket_low_as_price(doc)
    if low_price is not None:
        return _round2(low_price)
    redline_values = [
        _to_positive_number((doc or {}).get("pricePrimary")),
        _to_positive_number((doc or {}).get("cmPriceTrend")),
        _to_positive_number((doc or {}).get("cmPriceLow")),
        _to_positive_number((doc or {}).get("cmAvg30d")),
        _to_positive_number((doc or {}).get("priceYuyuTei")),
        _to_positive_number((doc or {}).get("pricePriceCharting")),
        _to_positive_number((doc or {}).get("priceCardTrader")),
    ]
    redline_values = [value for value in redline_values if value is not None]
    price = _round2(median(redline_values)) if redline_values else None
    if _cardmarket_stale_contradicted(doc, price):
        return None
    return price


def _safe_round2(n: float) -> float:
    return float(f"{n:.2f}")


def _median(nums: List[float]) -> float:
    if not nums:
        return 0.0
    arr = sorted(nums)
    size = len(arr)
    mid = size // 2
    if size % 2:
        return float(arr[mid])
    return float((arr[mid - 1] + arr[mid]) / 2.0)


def _mean(nums: List[float]) -> float:
    if not nums:
        return 0.0
    return float(sum(nums) / len(nums))


def _quantile(nums: List[float], q: float) -> float:
    if not nums:
        return 0.0
    arr = sorted(nums)
    pos = (len(arr) - 1) * q
    base = int(pos)
    rest = pos - base
    nxt = min(len(arr) - 1, base + 1)
    return float(arr[base] + (arr[nxt] - arr[base]) * rest)


def _iqr(nums: List[float]) -> float:
    return _quantile(nums, 0.75) - _quantile(nums, 0.25)


def _weighted_median(pairs: List[Dict[str, float]]) -> float:
    if not pairs:
        return 0.0
    arr = sorted(pairs, key=lambda x: x["value"])
    total = sum(p["weight"] for p in arr)
    if total <= 0:
        return 0.0
    acc = 0.0
    for pair in arr:
        acc += pair["weight"]
        if acc >= total / 2.0:
            return float(pair["value"])
    return float(arr[-1]["value"])


def _pick_market_price(md: Optional[Dict[str, Any]]) -> Optional[float]:
    # priceConfidenceScore 0: l'unico prezzo e' Cardmarket senza vendite, la carta non entra nei totali
    if not isinstance(md, dict) or md.get("priceConfidenceScore") == 0:
        return None
    for key in (
        "price",
        "priceTrend",
        "price7d",
        "price30d",
        "price90d",
        "price1d",
        "priceSecondary",
        "priceTertiary",
        "pricePrimary",
        "priceCardTrader",
    ):
        value = _to_number(md.get(key))
        if value is not None and value > 0:
            return _safe_round2(value)
    return None


def _normalize_bucket_key(value: Optional[str]) -> str:
    text = (value or "").strip()
    lowered = text.lower()
    if lowered in ("", "unknown", "don!!"):
        return "DON!!"
    return text


def _norm_to_list(value: Any, normalize: bool = False) -> List[str]:
    raw = value if isinstance(value, list) else ([value] if value else [])
    out = [str(x).strip() for x in raw if x is not None and str(x).strip()]
    if normalize:
        return [_normalize_bucket_key(x) for x in out]
    return out


def _card_attribute(doc: Dict[str, Any], *keys: str) -> Any:
    """
    Valore di un attributo della carta, cercato per ogni key prima a top-level e poi
    dentro customAttributes (dict dinamico: le key cambiano per gioco/carta e il valore
    puo' essere lista, stringa o None). Ritorna il primo valore non vuoto, altrimenti None.
    """
    custom = doc.get("customAttributes")
    if not isinstance(custom, dict):
        custom = {}
    custom_lower: Optional[Dict[str, Any]] = None
    for key in keys:
        for value in (doc.get(key), custom.get(key)):
            if value not in (None, "", []):
                return value
        # stessa key scritta con case diverso (es. "cardtype" vs "cardType")
        if custom_lower is None:
            custom_lower = {str(k).lower(): v for k, v in custom.items()}
        value = custom_lower.get(key.lower())
        if value not in (None, "", []):
            return value
    return None


def _bucket_add(
    bucket_map: Dict[str, Dict[str, float]],
    key: str,
    value_to_add: float,
    has_price: bool,
) -> None:
    if key not in bucket_map:
        bucket_map[key] = {"cards": 0.0, "priced": 0.0, "total": 0.0}
    bucket_map[key]["cards"] += 1.0
    if has_price:
        bucket_map[key]["priced"] += 1.0
        bucket_map[key]["total"] += value_to_add


def _finalize_buckets(bucket_map: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    entries = list(bucket_map.items())
    total_cards = sum(v["cards"] for _, v in entries)
    total_value = sum(v["total"] for _, v in entries)
    rows: List[Dict[str, Any]] = []
    for key, value in entries:
        priced = int(value["priced"])
        rows.append(
            {
                "key": key,
                "label": key,
                "cards": int(value["cards"]),
                "priced": priced,
                "totalUSD": _safe_round2(value["total"]),
                "avgPrice": _safe_round2(value["total"] / priced) if priced > 0 else 0.0,
                "pctByCount": _safe_round2((value["cards"] / total_cards) * 100.0) if total_cards else 0.0,
                "pctByValue": _safe_round2((value["total"] / total_value) * 100.0) if total_value else 0.0,
            }
        )
    rows.sort(key=lambda x: x["totalUSD"], reverse=True)
    return rows


def _price_weight(usd: float) -> float:
    return 1.0 / (1.0 + exp(-(usd - 2.0) / 3.0))


def _liq_weight(listings: float) -> float:
    return tanh((listings or 0.0) / 180.0)


def _card_weight(usd: Optional[float], listings: Optional[float]) -> float:
    price = max(0.0, usd or 0.0)
    liq = max(0.0, listings or 0.0)
    return max(0.0, min(1.0, _price_weight(price) * _liq_weight(liq)))


def _lift(x: float, gamma: float) -> float:
    x = max(0.0, min(1.0, x))
    return max(0.0, min(1.0, x**gamma))


def _infer_pack_divisor(set_doc: Dict[str, Any]) -> int:
    language = str(set_doc.get("language") or "").lower()
    set_id = str(set_doc.get("id") or "").upper()
    if language == "en":
        return 24
    if language == "jp":
        return 12
    return 12 if set_id.endswith("JP") else 24

def _calc_pct_change(now: Optional[float], base: Optional[float]) -> Optional[float]:
    if now is None or base is None or base == 0:
        return None
    return _round2(((now - base) / base) * 100.0)

@dataclass
class SeriesInfo:
    price_now_usd: Optional[float] # USD

@dataclass
class Baselines:
    b1: Optional[float]
    b7: Optional[float]
    b30: Optional[float]
    b90: Optional[float]
    b180: Optional[float]
    b365: Optional[float]

def _get_closest_around(
    prices: List[Dict[str, Any]],
    target_dt: datetime,
    max_days: Optional[float] = None, # es. 3, 7, 30, ecc.
) -> Optional[Dict[str, Any]]:
    closest_doc = None
    closest_diff_days = None

    for p in prices:
        cad = _parse_created_at(p)
        if cad is None:
            continue

        diff_days = abs((cad - target_dt).total_seconds()) / 86400.0

        if max_days is not None and diff_days > max_days:
            continue

        if closest_diff_days is None or diff_days < closest_diff_days:
            closest_doc = p
            closest_diff_days = diff_days

    return closest_doc


def _get_closest_at_or_before(
    prices: List[Dict[str, Any]],
    target_dt: datetime,
) -> Optional[Dict[str, Any]]:
    best_doc = None
    best_created_at = None
    for price_doc in prices:
        created_at = _parse_created_at(price_doc)
        if created_at is None or created_at > target_dt:
            continue
        if best_created_at is None or created_at > best_created_at:
            best_created_at = created_at
            best_doc = price_doc
    return best_doc

def _pick_series_now(latest: Optional[Dict[str, Any]]) -> SeriesInfo:
    return SeriesInfo(price_now_usd=_compute_price_redline(latest))

def _pick_baselines(
    b1doc: Optional[Dict[str, Any]],
    b7doc: Optional[Dict[str, Any]],
    b30doc: Optional[Dict[str, Any]],
    b90doc: Optional[Dict[str, Any]],
    b180doc: Optional[Dict[str, Any]],
    b365doc: Optional[Dict[str, Any]],
) -> Baselines:
    return Baselines(
        b1=_compute_price_redline(b1doc),
        b7=_compute_price_redline(b7doc),
        b30=_compute_price_redline(b30doc),
        b90=_compute_price_redline(b90doc),
        b180=_compute_price_redline(b180doc),
        b365=_compute_price_redline(b365doc),
    )

def _parse_created_at(doc: Dict[str, Any]) -> Optional[datetime]:
    cad = doc.get("createdAt")
    if isinstance(cad, dict) and "$date" in cad:
        try:
            # gestisce ISO + Z
            return datetime.fromisoformat(cad["$date"].replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(cad, datetime):
        return cad
    return None

def _as_number_or_none(v: Any) -> Optional[float]:
    n = _to_number(v)
    return _round2(n) if n is not None else None


def _effective_price_from_snapshot(doc: Optional[Dict[str, Any]]) -> Optional[float]:
    return _compute_price_redline(doc)


def _median_redline_window(prices: List[Dict[str, Any]], now: datetime, days: int) -> Optional[float]:
    """Mediana di priceRedLine delle righe degli ultimi `days` giorni fino a `now` (None se non ce ne sono)."""
    since = now - timedelta(days=days)
    values = []
    for snap in prices:
        created_at = _parse_created_at(snap)
        if created_at is None or created_at < since or created_at > now:
            continue
        px = _compute_price_redline(snap)
        if px is not None and px > 0:
            values.append(px)
    return _round2(median(values)) if values else None


def _carry_forward_pricecharting(snaps_desc: List[Dict[str, Any]]) -> None:
    """
    Le righe senza pricePriceCharting (rotazione o blocco dello scraping) prendono l'ultimo
    valore tracciato prima di loro. Senza, la mediana di priceRedLine cambia composizione da
    un giorno all'altro e falsa le variazioni % anche a prezzi fermi.
    Allo stesso modo ogni riga prende in GRADED_MAX_KEY il gradato piu' alto noto fino a quel giorno
    (per _cardmarket_low_as_price), senza toccare psa10/bsg10/cgc10 della riga.
    snaps_desc: righe di una sola carta, ordinate per createdAt discendente. Modifica in place.
    """
    last_value: Any = None
    last_graded: Dict[str, float] = {}
    for doc in reversed(snaps_desc):
        value = doc.get("pricePriceCharting")
        if _to_positive_number(value) is not None:
            last_value = value
        elif last_value is not None:
            doc["pricePriceCharting"] = last_value
        for key in GRADED_REFERENCE_FIELDS:
            graded = _to_positive_number(doc.get(key))
            if graded is not None:
                last_graded[key] = graded
        if last_graded:
            doc[GRADED_MAX_KEY] = max(last_graded.values())


def _extract_latest_and_previous_numeric(
    docs: List[Dict[str, Any]],
    field: str,
) -> Tuple[Optional[float], Optional[float]]:
    latest: Optional[float] = None
    previous: Optional[float] = None
    for doc in docs:
        value = _to_number(doc.get(field))
        if value is None:
            continue
        if latest is None:
            latest = value
            continue
        previous = value
        break
    return latest, previous


def _pick_baseline_value_around(
    docs: List[Dict[str, Any]],
    field: str,
    target_dt: datetime,
    max_days: float,
) -> Optional[float]:
    docs_with_value = [doc for doc in docs if _to_number(doc.get(field)) is not None]
    if not docs_with_value:
        return None
    baseline_doc = _get_closest_around(docs_with_value, target_dt, max_days=max_days)
    if not baseline_doc:
        return None
    return _to_number(baseline_doc.get(field))


def _extract_grade10_total_count(doc: Dict[str, Any]) -> Optional[float]:
    if not isinstance(doc, dict):
        return None
    grade10 = doc.get("grade10")
    if not isinstance(grade10, dict):
        return None
    return _to_number(grade10.get("totalCount"))


def _pick_grade10_count_baseline_around(
    docs: List[Dict[str, Any]],
    target_dt: datetime,
    max_days: float,
) -> Optional[float]:
    docs_with_count = [doc for doc in docs if _extract_grade10_total_count(doc) is not None]
    if not docs_with_count:
        return None
    baseline_doc = _get_closest_around(docs_with_count, target_dt, max_days=max_days)
    if not baseline_doc:
        return None
    return _extract_grade10_total_count(baseline_doc)


def _extract_latest_psa_gem_rate(docs: List[Dict[str, Any]]) -> Optional[float]:
    for doc in docs:
        provider = str(doc.get("gradingProvider") or "").strip().lower()
        if provider and provider != "psa":
            continue
        gem_rate = _to_number(doc.get("gemRate"))
        if gem_rate is not None:
            return _round2(gem_rate)
    return None


def _extract_cards_metrics_growth_value(doc: Dict[str, Any]) -> Optional[float]:
    value = _to_number(doc.get("growthRate"))
    return _round2(value) if value is not None else None


def _classify_price_band(price30d: Optional[float], price_now: Optional[float]) -> Dict[str, Any]:
    ref_price = price30d if price30d is not None and price30d > 0 else price_now
    if ref_price is None or ref_price <= 0:
        return {
            "key": "UNKNOWN",
            "label": "Unknown band",
            "reference": {
                "basis": "price30dMedianThenSpot",
                "price30d": _as_number_or_none(price30d),
                "priceNow": _as_number_or_none(price_now),
                "selected": None,
            },
            "proof": {
                "qualifiedHigher8wPct": None,
                "medianGain8wPct": None,
            },
        }

    for band in PRICE_BAND_RULES:
        band_min = float(band["min"])
        band_max = band["max"]
        if ref_price < band_min:
            continue
        if band_max is not None and ref_price >= float(band_max):
            continue
        return {
            "key": band["key"],
            "label": band["label"],
            "reference": {
                "basis": "price30dMedianThenSpot",
                "price30d": _as_number_or_none(price30d),
                "priceNow": _as_number_or_none(price_now),
                "selected": _as_number_or_none(ref_price),
            },
            "proof": {
                "qualifiedHigher8wPct": band["proofWinRate8w"],
                "medianGain8wPct": band["proofMedianGain8w"],
            },
        }

    return {
        "key": "UNKNOWN",
        "label": "Unknown band",
        "reference": {
            "basis": "price30dMedianThenSpot",
            "price30d": _as_number_or_none(price30d),
            "priceNow": _as_number_or_none(price_now),
            "selected": _as_number_or_none(ref_price),
        },
        "proof": {
            "qualifiedHigher8wPct": None,
            "medianGain8wPct": None,
        },
    }


def _snapshot_total_listings(doc: Optional[Dict[str, Any]]) -> Optional[float]:
    if not doc:
        return None
    cm_listings = _to_number(doc.get("listings"))
    ct_listings = _to_number(doc.get("ctListings"))
    if cm_listings is not None and ct_listings is not None:
        return cm_listings + ct_listings
    if cm_listings is not None:
        return cm_listings
    if ct_listings is not None:
        return ct_listings
    return None


def _compute_listing_pct_changes(
    prices: List[Dict[str, Any]],
    now_utc: datetime,
) -> Tuple[Optional[float], Optional[float]]:
    latest = prices[0] if prices else None
    listings_now = _snapshot_total_listings(latest)
    if listings_now is None or listings_now <= 0:
        return (None, None)

    doc7 = _get_closest_around(prices, now_utc - timedelta(days=7), max_days=3.5)
    doc30 = _get_closest_around(prices, now_utc - timedelta(days=30), max_days=7)
    listings_7d = _snapshot_total_listings(doc7)
    listings_30d = _snapshot_total_listings(doc30)

    pct7 = _calc_pct_change(listings_now, listings_7d)
    pct30 = _calc_pct_change(listings_now, listings_30d)
    return (pct7, pct30)


def _compute_price_position(price_now: Optional[float], prices: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    series = []
    for snap in prices:
        px = _effective_price_from_snapshot(snap)
        if px is not None and px > 0:
            series.append(px)

    if not series or price_now is None or price_now <= 0:
        return {
            "ath": None,
            "drawdownFromAthPct": None,
            "distanceToAthPct": None,
            "nearAth": None,
        }

    ath = max(series)
    drawdown = _round2(((price_now - ath) / ath) * 100.0) if ath > 0 else None
    distance = _round2(((ath - price_now) / ath) * 100.0) if ath > 0 else None
    near_ath = distance is not None and distance <= 8.0
    return {
        "ath": _as_number_or_none(ath),
        "drawdownFromAthPct": drawdown,
        "distanceToAthPct": distance,
        "nearAth": near_ath,
    }


def _compute_avg_gain_window_pct(
    prices: List[Dict[str, Any]],
    window_days: int,
) -> Optional[float]:
    if window_days <= 0:
        return None

    samples: List[Tuple[datetime, float]] = []
    for snap in prices:
        created_at = _parse_created_at(snap)
        px = _effective_price_from_snapshot(snap)
        if created_at is None or px is None or px <= 0:
            continue
        samples.append((created_at, px))

    if len(samples) < 2:
        return None

    samples.sort(key=lambda x: x[0])
    pct_changes: List[float] = []
    max_slippage_days = max(3.0, window_days * 0.25)

    for idx, (start_dt, start_px) in enumerate(samples):
        target_dt = start_dt + timedelta(days=window_days)
        best_j = -1
        best_diff = None
        for j in range(idx + 1, len(samples)):
            end_dt, end_px = samples[j]
            if end_dt <= start_dt:
                continue
            diff_days = abs((end_dt - target_dt).total_seconds()) / 86400.0
            if diff_days > max_slippage_days:
                continue
            if best_diff is None or diff_days < best_diff:
                best_diff = diff_days
                best_j = j

        if best_j == -1:
            continue

        end_px = samples[best_j][1]
        pct = _calc_pct_change(end_px, start_px)
        if pct is not None:
            pct_changes.append(float(pct))

    if not pct_changes:
        return None
    return _round2(_mean(pct_changes))


def _compute_sales_estimate(
    prices: List[Dict[str, Any]],
    now: datetime,
    window_days: int,
) -> Optional[int]:
    """
    STIMA (non un conteggio reale di transazioni) delle unita' vendute negli ultimi
    `window_days`, sommando giorno per giorno il calo dei listing: un calo tra un
    giorno e il successivo viene trattato come "venduto", un aumento come 0. Stessa
    euristica gia' usata per il totale di catalogo in getSalesVolume.py e per gli
    Sets in _build_set_market_history_snapshot, qui applicata a una singola carta.
    Nessuna scalatura (SALES_SCALING_FACTOR) applicata: e' il conteggio grezzo delle
    unita' "sparite" dai listing osservati, non calibrato contro vendite reali.
    Ritorna None se non ci sono abbastanza dati sui listing per stimare nulla.
    """
    if window_days <= 0 or not prices:
        return None

    day_end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    cursor_day = day_end - timedelta(days=window_days - 1)

    total_units = 0.0
    has_listing_data = False

    while cursor_day <= day_end:
        prev_day_end = cursor_day - timedelta(days=1)
        snap_today = _get_closest_at_or_before(prices, cursor_day)
        snap_prev = _get_closest_at_or_before(prices, prev_day_end)

        listings_today = _snapshot_total_listings(snap_today)
        listings_prev = _snapshot_total_listings(snap_prev)

        if listings_today is not None or listings_prev is not None:
            has_listing_data = True

        if listings_today is not None and listings_prev is not None:
            total_units += max(0.0, listings_prev - listings_today)

        cursor_day += timedelta(days=1)

    if not has_listing_data:
        return None
    return int(round(total_units))


# Set di carte promo / da evento / da torneo, mai ristampate (EVENT_RUNNER).
EVENT_SET_IDS = ("OPP", "OPPJP", "OPPR", "JP", "OP03P")
# Carte degli eventi di lancio (OP10RE, ...) e dei tornei di anniversario (OP05ANN, ...),
# compresi i set futuri con la stessa sigla.
EVENT_SET_PATTERN = re.compile(r"^OP\d+(RE|ANN|PRE)$")


def _is_event_set(set_id_norm: str) -> bool:
    return set_id_norm in EVENT_SET_IDS or bool(EVENT_SET_PATTERN.match(set_id_norm))


def _classify_buy_tier(
    set_id: Optional[str],
    price_band_key: str,
    pct1: Optional[float],
    pct7: Optional[float],
    pct30: Optional[float],
    pct90: Optional[float],
    spread_pct: Optional[float],
    listings_pct7: Optional[float],
    listings_pct30: Optional[float],
    near_ath: Optional[bool],
) -> Optional[Dict[str, Any]]:
    p1 = pct1 if pct1 is not None else 0.0
    p7 = pct7 if pct7 is not None else 0.0
    p30 = pct30 if pct30 is not None else 0.0
    p90 = pct90 if pct90 is not None else 0.0
    # spread_pct non entra piu' nei tier: cmPriceLow/cmPriceTrend mancano o sono incoerenti
    # (low sopra il trend) su circa meta' delle carte da $200+. Resta in firma per i chiamanti.
    l7 =listings_pct7 if listings_pct7 is not None else 0.0
    l30 = listings_pct30 if listings_pct30 is not None else 0.0
    set_id_norm = (set_id or "").strip().upper()

    tier_key: Optional[str] = None

    # Sell-warning tiers first so they cannot be masked by buy-side tiers.
    if p30 >= 10.0 and p7 <= -3.0:
        tier_key = "ROLLOVER"
    elif p7 <= -12.0:
        tier_key = "HARD_DROP"

    if (
        not tier_key
        and
        price_band_key == "USD_200_PLUS"
        and p7 >= 3.0
        and p7 <= 18.0
        and p30 >= 8.0
        and p30 <= 40.0
        and p1 > -4.0
    ):
        tier_key = "HIGH_CONVICTION"
    elif (
        not tier_key
        and
        price_band_key == "USD_200_PLUS"
        and bool(near_ath)
        and p7 >= 1.5
        and p30 >= 6.0
    ):
        tier_key = "RIDING_THE_HIGH"
    elif (
        not tier_key
        and
        price_band_key == "USD_200_PLUS"
        and p7 >= 0.5
        and p7 <= 6.5
        and p30 >= 5.0
        and p30 <= 20.0
        and abs(p1) <= 5.0
    ):
        tier_key = "PREMIUM_CLIMB"
    elif (
        not tier_key
        and _is_event_set(set_id_norm)
        and
        p7 >= 4.0
        and p30 >= 10.0
        and p90 >= 18.0
        and l30 <= -10.0
        and p7 <= 22.0
    ):
        tier_key = "EVENT_RUNNER"
    elif not tier_key and l30 <= -15.0 and p30 >= 5.0 and p7 >= 1.0:
        tier_key = "SUPPLY_SQUEEZE"
    elif (
        not tier_key
        and
        price_band_key in ("UNDER_10", "USD_10_50", "USD_50_200")
        and p7 >= 4.0
        and p30 >= 9.0
        and p7 <= 20.0
        and p30 <= 45.0
    ):
        tier_key = "MOMENTUM"

    if not tier_key:
        return None

    proof = BUY_TIER_RULES[tier_key]
    return {
        "key": tier_key,
        "label": proof["label"],
        "side": proof.get("side") or "buy",
        "proof": {
            "qualifiedHigher8wPct": proof["proofWinRate8w"],
            "medianGain8wPct": proof["proofMedianGain8w"],
        },
    }


def _compute_stamp_inputs(
    prices: List[Dict[str, Any]],
    now: datetime,
) -> Dict[str, Optional[float]]:
    """
    Variazioni usate dallo stamp (regole v2), calcolate su mediane di 3 giorni invece che su
    un solo giorno: prezzo attuale = mediana degli ultimi 3 giorni, riferimento a 7 giorni =
    mediana dei giorni 6-8, a 30 giorni = mediana dei giorni 29-31 (stesso per i listing).
    Con un solo giorno per lato il rumore quotidiano del prezzo (+-3%) faceva attraversare le
    soglie avanti e indietro: nello studio di ottobre 2026 il 59% dei cambi di stamp tornava
    allo stamp precedente entro 7 giorni. Le percentageChange* mostrate sul sito restano
    quelle giornaliere. prices: righe di una carta, createdAt discendente.
    """
    anchor = now.astimezone(timezone.utc).date()
    daily_price: Dict[int, float] = {}
    daily_listings: Dict[int, float] = {}
    for snap in prices:  # discendente: la prima riga di ogni giorno e' la piu' recente
        created_at = _parse_created_at(snap)
        if created_at is None:
            continue
        offset = (anchor - created_at.astimezone(timezone.utc).date()).days
        if offset < 0 or offset > 40 or offset in daily_price:
            continue
        px = _compute_price_redline(snap)
        if px is None or px <= 0:
            continue
        daily_price[offset] = px
        listings = _snapshot_total_listings(snap)
        if listings is not None:
            daily_listings[offset] = listings

    def window(series: Dict[int, float], lo: int, hi: int) -> Optional[float]:
        values = [series[d] for d in range(lo, hi + 1) if d in series]
        return median(values) if values else None

    def near(series: Dict[int, float], center: int, tol: int) -> Optional[float]:
        for off in [0] + [x for k in range(1, tol + 1) for x in (-k, k)]:
            if center + off in series:
                return series[center + off]
        return None

    def pct(now_value: Optional[float], base: Optional[float]) -> Optional[float]:
        return _calc_pct_change(now_value, base) if now_value is not None and base else None

    price_now = window(daily_price, 0, 2)
    listings_now = window(daily_listings, 0, 2) or None
    return {
        "pct1": pct(price_now, window(daily_price, 1, 3)),
        "pct7": pct(price_now, window(daily_price, 6, 8) or near(daily_price, 7, 3)),
        "pct30": pct(price_now, window(daily_price, 29, 31) or near(daily_price, 30, 7)),
        "listingsPct30": pct(listings_now, window(daily_listings, 29, 31) or near(daily_listings, 30, 7)),
    }


STAMP_BAND_THRESHOLDS = {
    # slip7: calo a 7 giorni da Sell Watch; rebound30: calo a 30 giorni da Rebound;
    # min7/min30: Accumulate.
    "UNDER_10": {"slip7": -7.0, "rebound30": -22.0, "min7": 6.0, "min30": 12.0},
    "USD_10_50": {"slip7": -6.0, "rebound30": -18.0, "min7": 5.0, "min30": 10.0},
    "USD_50_200": {"slip7": -5.0, "rebound30": -15.0, "min7": 4.0, "min30": 8.0},
    "USD_200_PLUS": {"slip7": -4.0, "rebound30": -12.0, "min7": 3.0, "min30": 6.0},
    "UNKNOWN": {"slip7": -5.5, "rebound30": -16.0, "min7": 4.0, "min30": 9.0},
}

# Sell Watch: prezzo in calo mentre crescono le copie in vendita. Nello studio di ottobre 2026 un
# calo di prezzo da solo non diceva nulla (dopo la chiamata la carta andava come il mercato) e il
# "mese forte che scivola" faceva peggio (la carta tendeva a risalire); con l'offerta in aumento
# la carta fa peggio del mercato nei 2/3 dei casi (-5 punti a 4 settimane, -8 a 8).
SELL_WATCH_MIN_LISTINGS_GROWTH_30D = 15.0


def _stamp_entry_key(t: Dict[str, float], p7: float, p30: float, l30: float) -> str:
    """Stamp che scatta con le soglie di entrata (in ordine di priorita')."""
    if p7 <= t["slip7"] and p30 <= 0.0 and l30 >= SELL_WATCH_MIN_LISTINGS_GROWTH_30D:
        return "SELL_WATCH"
    if p7 >= t["min7"] and p30 >= t["min30"] and p7 <= 25.0 and p30 <= 55.0:
        return "ACCUMULATE"
    if l30 <= -20.0 and p30 >= 5.0 and p7 >= 2.0:
        return "BUY_SETUP"
    if p30 <= t["rebound30"] and p7 >= 4.0:
        return "REBOUND_WATCH"
    return "HOLD"


def _stamp_still_valid(key: str, t: Dict[str, float], p7: float, p30: float, l30: float) -> bool:
    """Isteresi: condizioni di uscita, piu' larghe di quelle di entrata. Si esce dallo stamp
    solo quando il segnale e' davvero finito, non al primo giorno sotto la soglia."""
    if key == "SELL_WATCH":
        return p7 <= 0.0 and l30 >= 5.0
    if key == "ACCUMULATE":
        return p7 >= -2.0 and p30 >= t["min30"] * 0.25 and p7 <= 30.0 and p30 <= 65.0
    if key == "BUY_SETUP":
        return l30 <= -8.0 and p30 >= 0.0 and p7 >= -3.0
    if key == "REBOUND_WATCH":
        return p30 <= t["rebound30"] * 0.25 and p7 >= -1.0
    return False


def _classify_stamp(
    price_band_key: str,
    pct1: Optional[float],
    pct7: Optional[float],
    pct30: Optional[float],
    listings_pct30: Optional[float],
    buy_tier: Optional[Dict[str, Any]],
    previous_stamp: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Regole v2 (SIGNAL_RULES_VERSION in src/trackrecord/ledger.py). Gli input vanno calcolati con
    _compute_stamp_inputs. previous_stamp: stamp attuale della carta, per l'isteresi; un nuovo
    stamp diverso da Hold sostituisce sempre quello attuale.
    Lo stamp e' indipendente dal buy tier (pattern); pct1 e buy_tier restano in firma per i chiamanti.
    """
    t = STAMP_BAND_THRESHOLDS.get(price_band_key, STAMP_BAND_THRESHOLDS["UNKNOWN"])
    p7 = pct7 if pct7 is not None else 0.0
    p30 = pct30 if pct30 is not None else 0.0
    l30 = listings_pct30 if listings_pct30 is not None else 0.0

    key = _stamp_entry_key(t, p7, p30, l30)
    if (
        key == "HOLD"
        and previous_stamp in STAMP_RULES
        and previous_stamp != "HOLD"
        and _stamp_still_valid(previous_stamp, t, p7, p30, l30)
    ):
        key = previous_stamp
    rule = STAMP_RULES[key]
    return {"key": key, "label": rule["label"], "kind": rule["kind"]}

def compute_market_data_for_item(
    prices: List[Dict[str, Any]],
    graded_first: Dict[str, Any],
    psa10_count: Optional[float],
    psa10_count_previous: Optional[float],
    psa10_growth_rate: Optional[float] = None,
    population_docs: Optional[List[Dict[str, Any]]] = None,
    previous_stamp: Optional[str] = None,
) -> Dict[str, Any]:
    """previous_stamp: chiave dello stamp attuale della carta (CardsStamps), per l'isteresi."""
    latest = prices[0] if prices else None

    # Current series (USD)
    s = _pick_series_now(latest)

    # Baselines (USD): ancorati alla data dell'ultima osservazione disponibile, non al
    # wall-clock reale. Altrimenti un ingest in orario insolito (es. un retry job che
    # gira più tardi del solito) può far combaciare "adesso" e "N giorni fa" sullo
    # stesso documento, azzerando percentageChange1d/7d/... senza che il prezzo sia
    # davvero rimasto invariato.
    now = _parse_created_at(latest) if latest else None
    if now is None:
        now = datetime.now(timezone.utc)
    b1doc = _get_closest_around(prices, now - timedelta(days=1), max_days=1.75)
    b7doc = _get_closest_around(prices, now - timedelta(days=7), max_days=3.5)
    b14doc = _get_closest_around(prices, now - timedelta(days=14), max_days=5)
    b30doc = _get_closest_around(prices, now - timedelta(days=30), max_days=7)
    b60doc = _get_closest_around(prices, now - timedelta(days=60), max_days=10)
    b90doc = _get_closest_around(prices, now - timedelta(days=90), max_days=14)
    b180doc = _get_closest_around(prices, now - timedelta(days=180), max_days=56)
    b365doc = _get_closest_around(prices, now - timedelta(days=365), max_days=112)
    baselines = _pick_baselines(b1doc, b7doc, b30doc, b90doc, b180doc, b365doc)
    price_14d_redline = _compute_price_redline(b14doc)
    price_60d_redline = _compute_price_redline(b60doc)

    # Prices reference
    price_1d   = _as_number_or_none(baselines.b1)
    price_7d   = _as_number_or_none(baselines.b7)
    price_30d  = _as_number_or_none(baselines.b30)
    price_90d  = _as_number_or_none(baselines.b90)
    price_180d = _as_number_or_none(baselines.b180)
    price_365d = _as_number_or_none(baselines.b365)

    # Delta prices (USD)
    def _delta(now_: Optional[float], base_: Optional[float]) -> Optional[float]:
        if now_ is None or base_ is None:
            return None
        return _round2(now_ - base_)

    price_change_1d   = _delta(s.price_now_usd, baselines.b1)
    price_change_7d   = _delta(s.price_now_usd, baselines.b7)
    price_change_30d  = _delta(s.price_now_usd, baselines.b30)
    price_change_90d  = _delta(s.price_now_usd, baselines.b90)
    price_change_180d = _delta(s.price_now_usd, baselines.b180)
    price_change_365d = _delta(s.price_now_usd, baselines.b365)

    # Delta percent (USD)
    pct1   = _calc_pct_change(s.price_now_usd, baselines.b1)
    pct7   = _calc_pct_change(s.price_now_usd, baselines.b7)
    pct30  = _calc_pct_change(s.price_now_usd, baselines.b30)
    pct90  = _calc_pct_change(s.price_now_usd, baselines.b90)
    pct180 = _calc_pct_change(s.price_now_usd, baselines.b180)
    pct365 = _calc_pct_change(s.price_now_usd, baselines.b365)

    # Momentum come accelerazione: differenza tra il rendimento della finestra corrente
    # e quello della finestra immediatamente precedente. Positivo = il trend si sta
    # rafforzando, negativo = si sta indebolendo/invertendo (utile per distinguere un
    # rialzo che accelera da una bolla che si sta sgonfiando).
    pct7_prior = _calc_pct_change(price_7d, price_14d_redline)
    pct30_prior = _calc_pct_change(price_30d, price_60d_redline)
    momentum_7d = _round2(pct7 - pct7_prior) if pct7 is not None and pct7_prior is not None else None
    momentum_30d = _round2(pct30 - pct30_prior) if pct30 is not None and pct30_prior is not None else None

    # sellers & listings from doc (latest)
    cm_sellers = _to_number((latest or {}).get("sellers"))
    ct_sellers = _to_number((latest or {}).get("ctSellers"))
    if cm_sellers is not None and ct_sellers is not None:
        sellers: Any = cm_sellers + ct_sellers
    elif cm_sellers is not None:
        sellers = cm_sellers
    elif ct_sellers is not None:
        sellers = ct_sellers
    else:
        sellers = None
    cm_listings = _to_number((latest or {}).get("listings"))
    ct_listings = _to_number((latest or {}).get("ctListings"))
    if cm_listings is not None and ct_listings is not None:
        listings: Any = cm_listings + ct_listings
    elif cm_listings is not None:
        listings = cm_listings
    elif ct_listings is not None:
        listings = ct_listings
    else:
        listings = None
    listings_sources_count = int(cm_listings is not None) + int(ct_listings is not None)

    # priceSecondary USD
    price_secondary = None
    for key in ("cmPriceTrend", "cmPriceLow", "cmPriceAvg", "cmAvg1d", "cmAvg7d", "cmAvg30d"):
        v = _to_number((latest or {}).get(key))
        if v is not None:
            price_secondary = v
            break
    price_tertiary = _to_number((latest or {}).get("priceYuyuTei"))
    price_cardtrader = _to_number((latest or {}).get("priceCardTrader"))

    # Spread between cmPriceTrend and cmPriceLow
    cm_trend = _to_number((latest or {}).get("cmPriceTrend"))
    cm_low   = _to_number((latest or {}).get("cmPriceLow"))

    if cm_trend is not None and cm_trend > 0 and cm_low is not None and not _cardmarket_has_no_sales(latest) and not _cardmarket_low_mismatch(latest):
        # quanto il low è "scontato" rispetto al trend, in %
        low_vs_trend_discount_pct = _round2(((cm_trend - cm_low) / cm_trend) * 100.0)
    else:
        low_vs_trend_discount_pct = None

    # graded (USD)
    latest_psa10_usd, prev_psa10_usd = _extract_latest_and_previous_numeric(prices, "psa10")
    latest_bsg10_usd, _ = _extract_latest_and_previous_numeric(prices, "bsg10")
    latest_sgc10_usd, _ = _extract_latest_and_previous_numeric(prices, "sgc10")
    latest_cgc10_usd, _ = _extract_latest_and_previous_numeric(prices, "cgc10")
    latest_cgc10pristine_usd, _ = _extract_latest_and_previous_numeric(prices, "cgc10pristine")
    latest_bsg10black_usd, _ = _extract_latest_and_previous_numeric(prices, "bsg10black")
    latest_tag10_usd, _ = _extract_latest_and_previous_numeric(prices, "tag10")
    latest_ace10_usd, _ = _extract_latest_and_previous_numeric(prices, "ace10")

    psa10_usd = latest_psa10_usd if latest_psa10_usd is not None else _to_number(graded_first.get("psa10"))
    bsg10_usd = latest_bsg10_usd if latest_bsg10_usd is not None else _to_number(graded_first.get("bsg10"))
    sgc10_usd = latest_sgc10_usd if latest_sgc10_usd is not None else _to_number(graded_first.get("sgc10"))
    cgc10_usd = latest_cgc10_usd if latest_cgc10_usd is not None else _to_number(graded_first.get("cgc10"))
    cgc10pristine_usd = latest_cgc10pristine_usd if latest_cgc10pristine_usd is not None else _to_number(graded_first.get("cgc10pristine"))
    bsg10black_usd = latest_bsg10black_usd if latest_bsg10black_usd is not None else _to_number(graded_first.get("bsg10black"))
    tag10_usd = latest_tag10_usd if latest_tag10_usd is not None else _to_number(graded_first.get("tag10"))
    ace10_usd = latest_ace10_usd if latest_ace10_usd is not None else _to_number(graded_first.get("ace10"))

    price_redline = _compute_price_redline(latest)

    # eBay: se manca nello snapshot più recente, usa l'ultimo valore disponibile andando a ritroso
    price_ebay, _ = _extract_latest_and_previous_numeric(prices, "priceEbay")
    ebay_listings, _ = _extract_latest_and_previous_numeric(prices, "ebayListings")

    psa10_30d_usd = _pick_baseline_value_around(
        prices,
        "psa10",
        now - timedelta(days=30),
        max_days=7,
    )
    psa10_90d_usd = _pick_baseline_value_around(
        prices,
        "psa10",
        now - timedelta(days=90),
        max_days=14,
    )
    psa10_180d_usd = _pick_baseline_value_around(
        prices,
        "psa10",
        now - timedelta(days=180),
        max_days=56,
    )

    psa10_percentage_change_30d = _calc_pct_change(psa10_usd, psa10_30d_usd)
    psa10_percentage_change_90d = _calc_pct_change(psa10_usd, psa10_90d_usd)
    psa10_percentage_change_180d = _calc_pct_change(psa10_usd, psa10_180d_usd)

    count_docs = population_docs or []
    psa10_gem_rate = _extract_latest_psa_gem_rate(count_docs)
    psa10_count_30d = _pick_grade10_count_baseline_around(
        count_docs,
        now - timedelta(days=30),
        max_days=7,
    )
    psa10_count_90d = _pick_grade10_count_baseline_around(
        count_docs,
        now - timedelta(days=90),
        max_days=14,
    )
    psa10_count_180d = _pick_grade10_count_baseline_around(
        count_docs,
        now - timedelta(days=180),
        max_days=56,
    )

    psa10_count_percentage_change_30d = _calc_pct_change(psa10_count, psa10_count_30d)
    psa10_count_percentage_change_90d = _calc_pct_change(psa10_count, psa10_count_90d)
    psa10_count_percentage_change_180d = _calc_pct_change(psa10_count, psa10_count_180d)
    psa10_count_percentage_change = _calc_pct_change(psa10_count, psa10_count_previous)

    # Segnali (grading, priceBand, buyTier, stamp) solo con un prezzo affidabile: con score basso
    # escono numeri assurdi (es. Zoro CS 25-26 1st Place a $67 con PSA 10 a $130k: gradingProfit +88.245%).
    price_confidence_score, price_confidence_reason = _compute_price_confidence_score(latest, price_7d, price_30d)
    signals_ok = price_confidence_score is not None and price_confidence_score >= SIGNALS_MIN_PRICE_CONFIDENCE

    # gradingProfit percent (%)
    if s.price_now_usd and signals_ok:
        effective_cost = s.price_now_usd + (GRADING_FEES or 0.0)
        if effective_cost > 0:
            gp_psa10 = _round2(((psa10_usd - effective_cost) / effective_cost) * 100) if psa10_usd is not None else None
            gp_bsg10 = _round2(((bsg10_usd - effective_cost) / effective_cost) * 100) if bsg10_usd is not None else None
        else:
            gp_psa10 = None
            gp_bsg10 = None
    else:
        gp_psa10 = None
        gp_bsg10 = None

    liquidity_score = _compute_liquidity_score(
        sellers=sellers,
        listings=listings,
        listings_sources_count=listings_sources_count,
        spread_pct=low_vs_trend_discount_pct,
    )
    momentum_weighted_pct = _compute_weighted_momentum(pct7, pct30, pct90)
    momentum_score = _compute_momentum_score(momentum_weighted_pct)
    grading_attractiveness_score = _compute_grading_attractiveness(
        gp_psa10=gp_psa10,
        gp_bsg10=gp_bsg10,
        liquidity_score=liquidity_score,
    )

    listings_pct7, listings_pct30 = _compute_listing_pct_changes(prices, now)
    if signals_ok:
        price_band = _classify_price_band(_median_redline_window(prices, now, days=30), s.price_now_usd)
    else:
        price_band = _classify_price_band(None, None)
    price_position = _compute_price_position(s.price_now_usd, prices)
    buy_tier = _classify_buy_tier(
        set_id=graded_first.get("setId"),
        price_band_key=price_band.get("key", "UNKNOWN"),
        pct1=pct1,
        pct7=pct7,
        pct30=pct30,
        pct90=pct90,
        spread_pct=low_vs_trend_discount_pct,
        listings_pct7=listings_pct7,
        listings_pct30=listings_pct30,
        near_ath=price_position.get("nearAth"),
    ) if signals_ok else None
    avg_gain_4w_pct = _compute_avg_gain_window_pct(prices, window_days=28)
    avg_gain_8w_pct = _compute_avg_gain_window_pct(prices, window_days=56)
    if buy_tier is not None:
        buy_tier_proof = buy_tier.get("proof") if isinstance(buy_tier.get("proof"), dict) else {}
        buy_tier_proof["avgGain4wPct"] = avg_gain_4w_pct
        buy_tier["proof"] = buy_tier_proof
    if isinstance(price_band.get("proof"), dict):
        price_band["proof"]["avgGain4wPct"] = avg_gain_4w_pct
        price_band["proof"]["avgGain8wPct"] = avg_gain_8w_pct

    sales_7d_estimate = _compute_sales_estimate(prices, now, window_days=7)
    sales_30d_estimate = _compute_sales_estimate(prices, now, window_days=30)

    stamp_inputs = _compute_stamp_inputs(prices, now)
    if signals_ok:
        stamp = _classify_stamp(
            price_band_key=price_band.get("key", "UNKNOWN"),
            pct1=stamp_inputs["pct1"],
            pct7=stamp_inputs["pct7"],
            pct30=stamp_inputs["pct30"],
            listings_pct30=stamp_inputs["listingsPct30"],
            buy_tier=buy_tier,
            previous_stamp=previous_stamp,
        )
    else:
        stamp = {"key": "HOLD", "label": STAMP_RULES["HOLD"]["label"], "kind": STAMP_RULES["HOLD"]["kind"]}

    return {
        "updatedAt": datetime.now(timezone.utc),
        # marketData
        "sellers": sellers if sellers is not None else None,
        "listings": listings if listings is not None else None,
        "price": _as_number_or_none(price_redline), # USD
        "pricePrimary": _as_number_or_none((latest or {}).get("pricePrimary")), # USD
        "pricePriceCharting": _as_number_or_none((latest or {}).get("pricePriceCharting")), # USD
        "priceSecondary": _as_number_or_none(price_secondary), # USD
        "priceTertiary": _as_number_or_none(price_tertiary), # USD
        "priceCardTrader": _as_number_or_none(price_cardtrader), # USD

        "priceTrend": _as_number_or_none(cm_trend), # USD
        "priceLow": _as_number_or_none(cm_low), # USD
        "cmPriceTrend": _as_number_or_none((latest or {}).get("cmPriceTrend")), # USD
        "cmPriceAvg": _as_number_or_none((latest or {}).get("cmPriceAvg")), # USD
        "cmPriceLow": _as_number_or_none((latest or {}).get("cmPriceLow")), # USD
        "cmAvg1d": _as_number_or_none((latest or {}).get("cmAvg1d")), # USD
        "cmAvg7d": _as_number_or_none((latest or {}).get("cmAvg7d")), # USD
        "cmAvg30d": _as_number_or_none((latest or {}).get("cmAvg30d")), # USD
        "priceEbay": _as_number_or_none(price_ebay), # USD
        "ebayListings": _as_number_or_none(ebay_listings),
        "priceRedLine": _as_number_or_none(price_redline), # USD

        "sgc10": _as_number_or_none(sgc10_usd), # USD
        "cgc10": _as_number_or_none(cgc10_usd), # USD
        "cgc10pristine": _as_number_or_none(cgc10pristine_usd), # USD
        "tag10": _as_number_or_none(tag10_usd), # USD
        "ace10": _as_number_or_none(ace10_usd), # USD
        
        "psa10": _as_number_or_none(psa10_usd), # USD
        "psa10At30d": _as_number_or_none(psa10_30d_usd), # USD
        "psa10At90d": _as_number_or_none(psa10_90d_usd), # USD
        "psa10At180d": _as_number_or_none(psa10_180d_usd), # USD
        
        "psa10PercentageChange30d": psa10_percentage_change_30d, # %
        "psa10PercentageChange90d": psa10_percentage_change_90d, # %
        "psa10PercentageChange180d": psa10_percentage_change_180d, # %
        "psa10Count": _as_number_or_none(psa10_count),
        "psa10GemRate": _as_number_or_none(psa10_gem_rate), # %
        "psa10GrowthRate": _as_number_or_none(psa10_growth_rate), # %
        "psa10Count30d": _as_number_or_none(psa10_count_30d),
        "psa10Count90d": _as_number_or_none(psa10_count_90d),
        "psa10Count180d": _as_number_or_none(psa10_count_180d),
        "psa10CountPercentageChange30d": psa10_count_percentage_change_30d, # %
        "psa10CountPercentageChange90d": psa10_count_percentage_change_90d, # %
        "psa10CountPercentageChange180d": psa10_count_percentage_change_180d, # %
        "psa10CountPercentageChange": psa10_count_percentage_change, # %
        "psa10GradingProfit": gp_psa10, # %

        "bgs10": _as_number_or_none(bsg10_usd), # USD
        "bsg10black": _as_number_or_none(bsg10black_usd), # USD
        "bgs10GradingProfit": gp_bsg10, # %

        # reference prices (baseline)
        "price1d": price_1d,
        "price7d": price_7d,
        "price30d": price_30d,
        "price90d": price_90d,
        "price180d": price_180d,
        "price365d": price_365d,
 
        "priceChange1d": price_change_1d, # USD
        "priceChange7d": price_change_7d, # USD
        "priceChange30d": price_change_30d, # USD
        "priceChange90d": price_change_90d, # USD
        "priceChange180d": price_change_180d, # USD
        "priceChange365d": price_change_365d, # USD

        "percentageChange1d": pct1, # %
        "percentageChange7d": pct7, # %
        "percentageChange30d": pct30, # %
        "percentageChange90d": pct90, # %
        "percentageChange180d": pct180, # %
        "percentageChange365d": pct365, # %

        # spread low vs trend
        "spread": low_vs_trend_discount_pct, # %

        # derived deterministic signals for discovery / ranking
        "liquidityScore": liquidity_score, # 0..100
        "momentumWeighted": momentum_weighted_pct, # %
        "momentumScore": momentum_score, # 0..100
        "momentum7d": momentum_7d, # punti %, accelerazione: pct7 - pct7 della settimana precedente
        "momentum30d": momentum_30d, # punti %, accelerazione: pct30 - pct30 del mese precedente
        "sales7d": sales_7d_estimate, # STIMA unita' vendute, da calo listing (non conteggio reale)
        "sales30d": sales_30d_estimate, # STIMA unita' vendute, da calo listing (non conteggio reale)
        "gradingAttractivenessScore": grading_attractiveness_score, # 0..100
        "priceConfidenceScore": price_confidence_score, # 0..1, sotto SIGNALS_MIN_PRICE_CONFIDENCE niente segnali
        "priceConfidenceReason": price_confidence_reason, # cm_no_sales | few_sources | sources_disagree | no_history | None

        # ledger-style derived calls
        "priceBand": price_band,
        "buyTier": buy_tier,
        "stamp": stamp,
        "stampInputs": {k: _round2(v) for k, v in stamp_inputs.items()},  # % su mediane di 3 giorni
        "listingChange7d": listings_pct7,
        "listingChange30d": listings_pct30,
        "athPrice": price_position.get("ath"),
        "drawdownFromAthPct": price_position.get("drawdownFromAthPct"),
        "distanceToAthPct": price_position.get("distanceToAthPct"),
        "nearAth": price_position.get("nearAth"),
    }


def _build_set_market_data(
    set_doc: Dict[str, Any],
    items: List[Dict[str, Any]],
    sealed_price: Optional[float],
    price_history_by_item: Dict[str, List[Dict[str, Any]]],
    set_history_docs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    cards = [item for item in items if item.get("type") == "Cards"]
    non_cards = [item for item in items if item.get("type") != "Cards"]

    priced_rows = []
    for item in cards:
        market_data = item.get("marketData") or {}
        priced_rows.append(
            {
                "doc": item,
                "price": _pick_market_price(market_data),
                "listings": _to_number(market_data.get("listings")),
                "pct1": _to_number(market_data.get("percentageChange1d")),
                "pct7": _to_number(market_data.get("percentageChange7d")),
                "pct30": _to_number(market_data.get("percentageChange30d")),
                "spread": _to_number(market_data.get("spread")),
            }
        )

    cards_prices = [row["price"] for row in priced_rows if row["price"] is not None and row["price"] > 0]
    non_cards_prices = [
        price
        for price in (_pick_market_price(item.get("marketData")) for item in non_cards)
        if price is not None and price > 0
    ]
    psa10_prices = [
        value
        for value in (
            _to_number((row["doc"].get("marketData") or {}).get("psa10"))
            for row in priced_rows
        )
        if value is not None and value > 0
    ]
    bgs10_prices = [
        value
        for value in (
            _to_number((row["doc"].get("marketData") or {}).get("bgs10"))
            for row in priced_rows
        )
        if value is not None and value > 0
    ]

    cards_count = len(cards)
    non_cards_count = len(non_cards)
    total_count = cards_count + non_cards_count
    total_cards_price = _safe_round2(sum(cards_prices)) if cards_prices else 0.0
    total_other_types_price = _safe_round2(sum(non_cards_prices)) if non_cards_prices else 0.0
    total_price = _safe_round2(total_cards_price + total_other_types_price)

    now_utc = datetime.now(timezone.utc)
    baseline_total_price_7d = _pick_baseline_value_around(
        set_history_docs,
        "totalPrice",
        now_utc - timedelta(days=7),
        max_days=3.5,
    )
    baseline_total_price_30d = _pick_baseline_value_around(
        set_history_docs,
        "totalPrice",
        now_utc - timedelta(days=30),
        max_days=7,
    )
    total_price_change_7d = (
        _round2((total_price - baseline_total_price_7d) / baseline_total_price_7d * 100.0)
        if baseline_total_price_7d is not None and baseline_total_price_7d > 0
        else None
    )
    total_price_change_30d = (
        _round2((total_price - baseline_total_price_30d) / baseline_total_price_30d * 100.0)
        if baseline_total_price_30d is not None and baseline_total_price_30d > 0
        else None
    )

    booster_box_price = _safe_round2(sealed_price) if sealed_price is not None and sealed_price > 0 else None
    booster_pack_price = None
    if booster_box_price is not None:
        booster_pack_price = _safe_round2(booster_box_price / _infer_pack_divisor(set_doc))

    profitable_cards = 0
    if booster_pack_price is not None:
        profitable_cards = sum(1 for price in cards_prices if price >= booster_pack_price)

    hits = {
        "hits5": sum(1 for price in cards_prices if price >= 5.0),
        "hits10": sum(1 for price in cards_prices if price >= 10.0),
        "hits50": sum(1 for price in cards_prices if price >= 50.0),
        "hits100": sum(1 for price in cards_prices if price >= 100.0),
        "hits200": sum(1 for price in cards_prices if price >= 200.0),
        "hits500": sum(1 for price in cards_prices if price >= 500.0),
    }

    color_map: Dict[str, Dict[str, float]] = {}
    rarity_map: Dict[str, Dict[str, float]] = {}
    attribute_map: Dict[str, Dict[str, float]] = {}
    for row in priced_rows:
        doc = row["doc"]
        price = row["price"] if row["price"] is not None and row["price"] > 0 else 0.0
        has_price = price > 0

        colors = _norm_to_list(_card_attribute(doc, "color"), normalize=True) or ["DON!!"]
        rarities = _norm_to_list(_card_attribute(doc, "rarityName", "rarity"), normalize=True) or ["DON!!"]
        attributes = _norm_to_list(_card_attribute(doc, "attribute", "attributes"), normalize=True) or ["DON!!"]

        for values, bucket_map in (
            (colors, color_map),
            (rarities, rarity_map),
            (attributes, attribute_map),
        ):
            share = (price / len(values)) if has_price and values else 0.0
            for value in values:
                _bucket_add(bucket_map, value, share, has_price)

    def rarity_text(doc: Dict[str, Any]) -> str:
        return str(doc.get("rarityName") or doc.get("rarity") or "")

    def price_of(row: Dict[str, Any]) -> Optional[float]:
        price = row["price"]
        return price if price is not None and price > 0 else None

    prices_sr: List[float] = []
    prices_sec: List[float] = []
    prices_alt: List[float] = []
    prices_r: List[float] = []
    for row in priced_rows:
        price = price_of(row)
        if price is None:
            continue
        rarity = rarity_text(row["doc"]).upper()
        if "SR" in rarity:
            prices_sr.append(price)
        if "SEC" in rarity:
            prices_sec.append(price)
        if price >= 10.0 and "SEC" not in rarity and price < 300.0:
            prices_alt.append(price)
        if rarity == "R" or rarity.startswith("R "):
            prices_r.append(price)

    prices_manga = [price for price in cards_prices if price >= 300.0]
    rare_median = min(_safe_round2(_quantile(prices_r, 0.5)) if prices_r else 0.0, 0.4)

    p_sr_real = _safe_round2(_quantile(prices_sr, 0.5)) if prices_sr else 0.0
    p_sec_real = _safe_round2(_quantile(prices_sec, 0.5)) if prices_sec else 0.0
    p_alt_real = _safe_round2(_quantile(prices_alt, 0.5)) if prices_alt else 0.0
    p_mng_real = _safe_round2(_quantile(prices_manga, 0.5)) if prices_manga else 0.0

    p_sr_exp = _safe_round2(_quantile(prices_sr, 0.4)) if prices_sr else 0.0
    p_sec_exp = _safe_round2(_quantile(prices_sec, 0.4)) if prices_sec else 0.0
    p_alt_exp = _safe_round2(_quantile(prices_alt, 0.4)) if prices_alt else 0.0
    p_mng_exp = _safe_round2(_quantile(prices_manga, 0.4)) if prices_manga else 0.0

    realistic_expected_value = _safe_round2(
        (6.5 * p_sr_real)
        + (1.8 * p_alt_real)
        + (0.55 * p_sec_real)
        + (0.0413 * p_mng_real)
        + (24.0 * rare_median)
    )
    expected_value = _safe_round2(
        (5.5 * p_sr_exp)
        + (1.4 * p_alt_exp)
        + (0.25 * p_sec_exp)
        + (0.0138 * p_mng_exp)
        + (24.0 * rare_median * 0.6)
    )

    roi = None
    if booster_box_price is not None and booster_box_price > 0:
        roi = _safe_round2(((realistic_expected_value - booster_box_price) / booster_box_price) * 100.0)

    cap_ultra_cheap = 0.12
    cap_cheap = 0.24
    cap_normal = 0.38

    mix_abs_pairs: List[Dict[str, float]] = []
    mix_abs_values: List[float] = []
    for row in priced_rows:
        price = row["price"] or 0.0
        listings = row["listings"] or 0.0
        weight = _card_weight(price, listings)
        cap = cap_ultra_cheap if price < 1.0 else cap_cheap if price < 5.0 else cap_normal

        parts = []
        if row["pct1"] is not None:
            parts.append(0.25 * min(abs(row["pct1"]) / 100.0, cap))
        if row["pct7"] is not None:
            parts.append(0.50 * min(abs(row["pct7"]) / 100.0, cap))
        if row["pct30"] is not None:
            parts.append(0.35 * min(abs(row["pct30"]) / 100.0, cap))
        if not parts:
            continue

        mix_abs_pct = _median(parts) * 100.0
        mix_abs_pairs.append({"value": mix_abs_pct, "weight": weight})
        mix_abs_values.append(mix_abs_pct)

    mix_wmed = _weighted_median(mix_abs_pairs)
    f_raw = max(0.0, min(1.0, mix_wmed / 18.0))
    f_value = _lift(max(f_raw, 0.05 if mix_abs_pairs else 0.0), 0.88)

    tau = max(8.0, _median(mix_abs_values) + _iqr(mix_abs_values))
    w_mov = 0.0
    w_all = 0.0
    for pair in mix_abs_pairs:
        w_all += pair["weight"]
        if pair["value"] >= tau:
            w_mov += pair["weight"]
    movers_share = (w_mov + 0.75) / (w_all + 3.75) if (w_all + 3.75) > 0 else 0.0
    b_raw = max(0.0, min(1.0, (movers_share - 0.16) / 0.34))
    b_value = _lift(b_raw, 0.88)

    listings_vals = [row["listings"] for row in priced_rows if row["listings"] is not None and row["listings"] >= 0]
    lq50 = _quantile(listings_vals, 0.5) if listings_vals else 0.0
    l_norm = log1p(lq50) / log1p(450.0) if lq50 >= 0 else 0.0
    l_raw = max(0.0, min(1.0, 1.0 - l_norm))
    l_value = _lift(l_raw, 0.92)

    v_vals = [(row["price"] or 0.0) * (max(1.0, row["listings"] or 0.0) ** 0.3) for row in priced_rows]
    v_sum = sum(v_vals)
    hhi = sum((v / v_sum) ** 2 for v in v_vals) if v_sum > 0 else 0.0
    c_raw = max(0.0, min(1.0, (hhi - 0.14) / 0.32))
    c_value = _lift(c_raw, 1.0)

    signed_pairs = [
        {"value": row["pct7"], "weight": _card_weight(row["price"], row["listings"])}
        for row in priced_rows
        if row["pct7"] is not None
    ]
    med_signed_7d = _weighted_median(signed_pairs)
    if med_signed_7d <= -10.0:
        d_value = 0.08
    elif med_signed_7d <= -6.0:
        d_value = 0.04
    elif med_signed_7d >= 10.0:
        d_value = -0.07
    elif med_signed_7d >= 6.0:
        d_value = -0.03
    else:
        d_value = 0.0

    spread_pairs = [
        {"value": min(row["spread"], 30.0), "weight": _card_weight(row["price"], row["listings"])}
        for row in priced_rows
        if row["spread"] is not None
    ]
    spread_wmed = _weighted_median(spread_pairs)
    x_core = max(0.0, min(1.0, spread_wmed / 16.0))
    x_value = max(0.0, min(1.0, x_core * (0.6 + 0.4 * c_raw)))

    top_rows = sorted(
        [row for row in priced_rows if row["price"] is not None],
        key=lambda x: x["price"],
        reverse=True,
    )
    top_count = max(3, int(len(top_rows) * 0.1)) if top_rows else 0
    top_pairs = []
    for row in top_rows[:top_count]:
        if row["pct30"] is None:
            continue
        cap = cap_cheap if (row["price"] or 0.0) < 5.0 else cap_normal
        top_pairs.append(
            {
                "value": min(abs(row["pct30"]) / 100.0, cap) * 100.0,
                "weight": _card_weight(row["price"], row["listings"]),
            }
        )
    h_value = max(0.0, min(1.0, _weighted_median(top_pairs) / 15.0))

    rho_value = 0.0
    corr_rows = [row for row in priced_rows if row["price"] is not None and row["listings"] is not None]
    if len(corr_rows) >= 4:
        px = [row["price"] for row in corr_rows]
        lx = [log1p(max(0.0, row["listings"] or 0.0)) for row in corr_rows]
        mean_x = sum(px) / len(px)
        mean_y = sum(lx) / len(lx)
        num = 0.0
        dx = 0.0
        dy = 0.0
        for price, listings in zip(px, lx):
            vx = price - mean_x
            vy = listings - mean_y
            num += vx * vy
            dx += vx * vx
            dy += vy * vy
        rho = (num / sqrt(dx * dy)) if dx > 0 and dy > 0 else 0.0
        rho_value = max(0.0, min(1.0, (0.2 - rho) / 0.8))

    j_value = sqrt(l_value * c_value)
    score01 = (
        (0.44 * f_value)
        + (0.18 * b_value)
        + (0.15 * l_value)
        + (0.10 * c_value)
        + d_value
        + (0.12 * x_value)
        + (0.08 * h_value)
        + (0.05 * rho_value)
        + (0.05 * j_value)
    )
    score01 = max(0.0, min(1.0, score01 * 1.14))
    volatility_score = round(score01 * 100.0) / 10.0

    day_labels: List[str] = []
    end_day = now_utc
    start_day = end_day - timedelta(days=SET_TREND_DAYS)
    cursor_day = datetime(
        start_day.year,
        start_day.month,
        start_day.day,
        tzinfo=timezone.utc,
    )
    final_day = datetime(
        end_day.year,
        end_day.month,
        end_day.day,
        tzinfo=timezone.utc,
    )
    while cursor_day <= final_day:
        day_labels.append(cursor_day.date().isoformat())
        cursor_day += timedelta(days=1)

    cards_only = [item for item in items if item.get("type") == "Cards"]
    trend_points: List[Dict[str, Any]] = []
    sales_volume: List[Dict[str, Any]] = []
    for iso_date in day_labels:
        day_end = datetime.fromisoformat(f"{iso_date}T23:59:59.999999+00:00")
        prev_day_end = day_end - timedelta(days=1)

        total = 0.0
        priced_count = 0
        units = 0
        usd = 0.0
        price_sum_for_avg = 0.0
        price_count_for_avg = 0
        cards_with_listings = 0

        for card in cards_only:
            card_id = card.get("id")
            if not isinstance(card_id, str):
                continue
            history = price_history_by_item.get(card_id, [])

            trend_snap = _get_closest_at_or_before(history, day_end)
            trend_price = _effective_price_from_snapshot(trend_snap)
            if trend_price is not None and trend_price > 0:
                total += trend_price
                priced_count += 1

            snap_today = trend_snap
            snap_prev = _get_closest_at_or_before(history, prev_day_end)

            listings_today = _to_number((snap_today or {}).get("listings"))
            listings_prev = _to_number((snap_prev or {}).get("listings"))
            price_today = _effective_price_from_snapshot(snap_today)
            price_prev = _effective_price_from_snapshot(snap_prev)

            if listings_today is not None or listings_prev is not None:
                cards_with_listings += 1

            price_ref = None
            if price_today is not None and price_prev is not None:
                price_ref = (price_today + price_prev) / 2.0
            elif price_today is not None:
                price_ref = price_today
            elif price_prev is not None:
                price_ref = price_prev

            if listings_today is not None and listings_prev is not None:
                sold_units = max(0.0, listings_prev - listings_today)
                if sold_units > 0:
                    units += int(sold_units)
                    if price_ref is not None and price_ref > 0:
                        usd += sold_units * price_ref

            if price_ref is not None and price_ref > 0:
                price_sum_for_avg += price_ref
                price_count_for_avg += 1

        trend_points.append(
            {
                "date": iso_date,
                "total": _safe_round2(total),
                "count": priced_count,
            }
        )
        avg_price = (
            _safe_round2(price_sum_for_avg / price_count_for_avg)
            if price_count_for_avg > 0
            else 0.0
        )
        sales_volume.append(
            {
                "date": iso_date,
                "units": units * 3,
                "usd": _safe_round2(usd * 3.0),
                "avgPrice": avg_price,
                "cardsWithListings": cards_with_listings,
            }
        )

    edges = [0.0, 5.0, 15.0, 50.0, 100.0, 250.0, float("inf")]
    bins = [0 for _ in range(len(edges) - 1)]
    for price in cards_prices:
        for idx in range(len(edges) - 1):
            low = edges[idx]
            high = edges[idx + 1]
            if price >= low and price < high:
                bins[idx] += 1
                break

    price_distribution = []
    for idx, count in enumerate(bins):
        low = edges[idx]
        high = edges[idx + 1]
        price_distribution.append(
            {
                "min": low,
                "max": None if high == float("inf") else high,
                "count": count,
                "label": (
                    f">= ${low:.2f}"
                    if high == float("inf")
                    else f"${low:.2f} - ${high:.2f}"
                ),
            }
        )

    top_movers = sorted(
        [
            {
                "id": row["doc"].get("id"),
                "localId": row["doc"].get("localId"),
                "name": row["doc"].get("name"),
                "price": row["price"],
                "score": _to_number(row["doc"].get("score")),
                "priceChange7d": row["doc"].get("marketData", {}).get("priceChange7d"),
                "percentageChange7d": row["doc"].get("marketData", {}).get("percentageChange7d"),
            }
            for row in priced_rows
            if (row["price"] or 0) >= SET_TOP_BASE_PRICE and row["pct7"] is not None
        ],
        key=lambda x: x["percentageChange7d"] or float("-inf"),
        reverse=True,
    )[:6]

    top_score = sorted(
        [
            {
                "id": row["doc"].get("id"),
                "localId": row["doc"].get("localId"),
                "name": row["doc"].get("name"),
                "price": row["price"],
                "score": _to_number(row["doc"].get("score")),
                "priceChange7d": row["doc"].get("marketData", {}).get("priceChange7d"),
                "percentageChange7d": row["doc"].get("marketData", {}).get("percentageChange7d"),
            }
            for row in priced_rows
            if (row["price"] or 0) >= SET_TOP_BASE_PRICE
            and isinstance(_to_number(row["doc"].get("score")), float)
        ],
        key=lambda x: x["score"] or float("-inf"),
        reverse=True,
    )[:6]

    return {
        "updatedAt": datetime.now(timezone.utc),
        "cardsCount": cards_count,
        "otherTypesCount": non_cards_count,
        "totalCount": total_count,
        "totalCardsPrice": total_cards_price,
        "totalOtherTypesPrice": total_other_types_price,
        "totalPrice": total_price,
        "totalPriceChange7d": total_price_change_7d,
        "totalPriceChange30d": total_price_change_30d,
        "psa10CardsCount": len(psa10_prices),
        "psa10TotalPrice": _safe_round2(sum(psa10_prices)) if psa10_prices else 0.0,
        "bgs10CardsCount": len(bgs10_prices),
        "bgs10TotalPrice": _safe_round2(sum(bgs10_prices)) if bgs10_prices else 0.0,
        **hits,
        "boosterBoxPrice": booster_box_price,
        "boosterPackPrice": booster_pack_price,
        "profitableCards": profitable_cards,
        "expectedValue": expected_value,
        "realisticExpectedValue": realistic_expected_value,
        "roi": roi,
        "volatility": volatility_score,
        "marketBreakdown": {
            "byColor": _finalize_buckets(color_map),
            "byRarity": _finalize_buckets(rarity_map),
            "byAttribute": _finalize_buckets(attribute_map),
        },
        "cards": {
            "charts": {
                "trend": trend_points,
                "priceDistribution": price_distribution,
                "salesVolume": sales_volume,
            },
            "top": {
                "movers": top_movers,
                "score": top_score,
            },
        },
    }


def _build_set_market_history_snapshot(
    set_doc: Dict[str, Any],
    market_data: Dict[str, Any],
    captured_at: datetime,
) -> Dict[str, Any]:
    snapshot = {
        field: market_data.get(field)
        for field in SET_MARKET_HISTORY_FIELDS
    }
    snapshot["setId"] = set_doc.get("id")
    snapshot["language"] = set_doc.get("language")
    snapshot["sealedId"] = set_doc.get("sealedId")
    snapshot["date"] = captured_at.date().isoformat()
    snapshot["sourceUpdatedAt"] = market_data.get("updatedAt")
    snapshot["createdAt"] = captured_at
    return snapshot


def update_sets_market_data(db: Database, set_ids: List[str]) -> Tuple[int, int]:
    if not set_ids:
        return (0, 0)

    coll_sets = db["Sets"]
    coll_cards = db["Cards"]
    coll_prices = db["Prices"]
    coll_history = db[SET_MARKET_HISTORY_COLLECTION]

    sets = list(
        coll_sets.find(
            {"id": {"$in": set_ids}},
            {"id": 1, "language": 1, "sealedId": 1},
        )
    )
    if not sets:
        return (0, 0)

    set_id_list = [s["id"] for s in sets if isinstance(s.get("id"), str)]
    now_utc = datetime.now(timezone.utc)
    today_start_utc = datetime(
        now_utc.year,
        now_utc.month,
        now_utc.day,
        tzinfo=timezone.utc,
    )
    tomorrow_start_utc = today_start_utc + timedelta(days=1)
    existing_history_set_ids = {
        doc["setId"]
        for doc in coll_history.find(
            {
                "setId": {"$in": set_id_list},
                "createdAt": {
                    "$gte": today_start_utc,
                    "$lt": tomorrow_start_utc,
                },
            },
            {"_id": 0, "setId": 1},
        )
        if isinstance(doc.get("setId"), str)
    }
    items_by_set: Dict[str, List[Dict[str, Any]]] = {set_id: [] for set_id in set_id_list}

    for item in coll_cards.find(
        {"setId": {"$in": set_id_list}},
        {
            "id": 1,
            "name": 1,
            "localId": 1,
            "setId": 1,
            "type": 1,
            "score": 1,
            "color": 1,
            "rarity": 1,
            "rarityName": 1,
            "attribute": 1,
            "attributes": 1,
            "customAttributes": 1,
            "marketData": 1,
        },
    ):
        set_id = item.get("setId")
        if isinstance(set_id, str):
            items_by_set.setdefault(set_id, []).append(item)

    sealed_ids = [
        s.get("sealedId")
        for s in sets
        if isinstance(s.get("sealedId"), str) and s.get("sealedId")
    ]
    sealed_latest: Dict[str, Dict[str, Any]] = {}
    if sealed_ids:
        sealed_query = {"itemId": {"$in": sealed_ids}}
        sealed_docs = list(coll_prices.find(
            sealed_query,
            {
                "itemId": 1,
                "createdAt": 1,
                "pricePrimary": 1,
                "priceYuyuTei": 1,
                "priceCardTrader": 1,
                "cmPriceTrend": 1,
                "cmAvg30d": 1,
                "cmAvg7d": 1,
                "cmAvg1d": 1,
                "cmPriceAvg": 1,
                "cmPriceLow": 1,
                "priceUngraded": 1,
                "pricePriceCharting": 1,
            },
        ).sort("createdAt", -1))
        attach_graded(db, sealed_docs, sealed_query, fields=("priceUngraded", "pricePriceCharting"))
        sealed_by_item: Dict[str, List[Dict[str, Any]]] = {}
        for doc in sealed_docs:
            sealed_by_item.setdefault(doc.get("itemId"), []).append(doc)
        for item_docs in sealed_by_item.values():
            _carry_forward_pricecharting(item_docs)
        for doc in sealed_docs:
            item_id = doc.get("itemId")
            if isinstance(item_id, str) and item_id not in sealed_latest:
                sealed_latest[item_id] = doc

    card_ids = [
        item.get("id")
        for items in items_by_set.values()
        for item in items
        if item.get("type") == "Cards" and isinstance(item.get("id"), str)
    ]
    price_history_by_item: Dict[str, List[Dict[str, Any]]] = {}
    if card_ids:
        since = datetime.now(timezone.utc) - timedelta(days=SET_PRICE_LOOKBACK_DAYS)
        for doc in coll_prices.find(
            {"itemId": {"$in": card_ids}, "createdAt": {"$gte": since}},
            {
                "itemId": 1,
                "createdAt": 1,
                "pricePrimary": 1,
                "cmPriceTrend": 1,
                "cmPriceLow": 1,
                "priceYuyuTei": 1,
                "priceCardTrader": 1,
                "listings": 1,
                "ctListings": 1,
                "ctSellers": 1,
            },
        ):
            item_id = doc.get("itemId")
            if isinstance(item_id, str):
                price_history_by_item.setdefault(item_id, []).append(doc)
        for snaps in price_history_by_item.values():
            snaps.sort(
                key=lambda doc: doc.get("createdAt") or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )

    set_history_by_set: Dict[str, List[Dict[str, Any]]] = {}
    history_since = now_utc - timedelta(days=SET_HISTORY_LOOKBACK_DAYS)
    for history_doc in coll_history.find(
        {
            "setId": {"$in": set_id_list},
            "createdAt": {"$gte": history_since},
        },
        {
            "setId": 1,
            "createdAt": 1,
            "totalPrice": 1,
        },
    ):
        set_id = history_doc.get("setId")
        if isinstance(set_id, str):
            set_history_by_set.setdefault(set_id, []).append(history_doc)

    ops: List[UpdateOne] = []
    history_ops: List[InsertOne] = []
    touched = 0
    updated = 0
    for set_doc in sets:
        set_id = set_doc.get("id")
        if not isinstance(set_id, str):
            continue
        sealed_doc = sealed_latest.get(set_doc.get("sealedId"))
        sealed_price = _pick_series_now(sealed_doc).price_now_usd if sealed_doc else None
        market_data = _build_set_market_data(
            set_doc=set_doc,
            items=items_by_set.get(set_id, []),
            sealed_price=sealed_price,
            price_history_by_item=price_history_by_item,
            set_history_docs=set_history_by_set.get(set_id, []),
        )
        touched += 1
        ops.append(
            UpdateOne(
                {"id": set_id},
                {"$set": {"marketData": market_data}},
            )
        )
        if set_id not in existing_history_set_ids:
            captured_at = datetime.now(timezone.utc)
            history_snapshot = _build_set_market_history_snapshot(
                set_doc=set_doc,
                market_data=market_data,
                captured_at=captured_at,
            )
            history_ops.append(InsertOne(history_snapshot))

    if ops:
        modified_count, upserted_count = _bulk_write_resilient(
            coll_sets,
            ops,
            ordered=False,
        )
        updated = modified_count + upserted_count
    if history_ops:
        _bulk_write_resilient(
            coll_history,
            history_ops,
            ordered=False,
        )

    return (touched, updated)


def update_cards_market_data(
    db: Database,
    days_back: int = 400,
    limit_ids: Optional[List[str]] = None,
    sales_db: Optional[Database] = None,
    track_record: bool = False,
) -> Tuple[int, int, int, int]:
    """track_record=True registra i cambi di stamp e pattern in TrackRecordCalls (solo One Piece)."""
    _sales_db = sales_db if sales_db is not None else db
    coll_cards = db["Cards"]
    coll_prices = db["Prices"]
    coll_card_stamps = db[CARD_STAMPS_COLLECTION]
    coll_card_stamp_changes = _sales_db[CARD_STAMP_CHANGES_COLLECTION]
    coll_cards_grading_population = _sales_db["CardsGradingPopulation"]

    q_cards: Dict[str, Any] = {}
    #q_cards["setId"] = "OP13"
    if limit_ids:
        q_cards["id"] = {"$in": limit_ids}

    base_cards = list(coll_cards.find(q_cards, {"id": 1, "name": 1, "setId": 1, "type": 1, "marketData": 1}))
    base_cards_by_id: Dict[str, Dict[str, Any]] = {
        card.get("id"): card
        for card in base_cards
        if isinstance(card.get("id"), str)
    }
    ids = [c["id"] for c in base_cards if "id" in c]
    if not ids:
        return (0, 0, 0, 0)

    affected_set_ids = sorted(
        set_id
        for set_id in coll_cards.distinct("setId", q_cards)
        if isinstance(set_id, str) and set_id.strip()
    )

    since = datetime.now(timezone.utc) - timedelta(days=days_back)

    prices_query = {"itemId": {"$in": ids}, "createdAt": {"$gte": since}}
    cur = coll_prices.find(
        prices_query,
        {
            "itemId": 1,
            "createdAt": 1,
            "pricePrimary": 1,
            "priceYuyuTei": 1,
            "priceCardTrader": 1,
            "cmPriceTrend": 1,
            "cmAvg30d": 1,
            "cmAvg7d": 1,
            "cmAvg1d": 1,
            "cmPriceAvg": 1,
            "cmPriceLow": 1,
            "priceUngraded": 1,
            "pricePriceCharting": 1,
            "priceEbay": 1,
            "ebayListings": 1,
            "listings": 1,
            "ctListings": 1,
            "sellers": 1,
            "ctSellers": 1,
            "psa10": 1,
            "bsg10": 1,
            "sgc10": 1,
            "cgc10": 1,
            "cgc10pristine": 1,
            "bsg10black": 1,
            "tag10": 1,
            "ace10": 1,
        },
    )

    price_docs = attach_graded(
        db,
        list(cur),
        prices_query,
        fields=(
            "priceUngraded", "pricePriceCharting",
            "psa10", "bsg10", "sgc10", "cgc10", "cgc10pristine", "bsg10black", "tag10", "ace10",
        ),
    )

    per_item: Dict[str, List[Dict[str, Any]]] = {}
    for p in price_docs:
        per_item.setdefault(p["itemId"], []).append(p)

    # order by createdAt DESC
    for snaps in per_item.values():
        snaps.sort(
            key=lambda doc: doc.get("createdAt") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        _carry_forward_pricecharting(snaps)

    # graded first (psa10/bsg10) - $first on createdAt desc
    graded_first: Dict[str, Dict[str, Any]] = {}

    for cid, snaps in per_item.items():
        best_doc = None
        best_ts = None

        for d in snaps:
            # check graded prices
            has_psa = isinstance(d.get("psa10"), (int, float))
            has_bsg = isinstance(d.get("bsg10"), (int, float))
            has_sgc = isinstance(d.get("sgc10"), (int, float))
            has_cgc = isinstance(d.get("cgc10"), (int, float))
            has_cgc_pristine = isinstance(d.get("cgc10pristine"), (int, float))
            has_bsg_black = isinstance(d.get("bsg10black"), (int, float))
            has_tag10 = isinstance(d.get("tag10"), (int, float))
            has_ace10 = isinstance(d.get("ace10"), (int, float))

            if not (has_psa or has_bsg or has_sgc or has_cgc or has_cgc_pristine or has_bsg_black or has_tag10 or has_ace10):
                continue

            cad = d.get("createdAt")
            if not isinstance(cad, datetime):
                continue

            if best_ts is None or cad > best_ts:
                best_ts = cad
                best_doc = d

        if best_doc:
            graded_first[cid] = {
                "psa10": best_doc.get("psa10"),
                "bsg10": best_doc.get("bsg10"),
                "sgc10": best_doc.get("sgc10"),
                "cgc10": best_doc.get("cgc10"),
                "cgc10pristine": best_doc.get("cgc10pristine"),
                "bsg10black": best_doc.get("bsg10black"),
                "tag10": best_doc.get("tag10"),
                "ace10": best_doc.get("ace10"),
                "setId": (base_cards_by_id.get(cid) or {}).get("setId"),
            }

    for cid in ids:
        graded_first.setdefault(cid, {})["setId"] = (base_cards_by_id.get(cid) or {}).get("setId")

    # CardsMetrics fallback: if a card has no psa10 in Prices, use latestPrice from the most recent CardsMetrics doc.
    coll_cards_metrics = _sales_db["CardsMetrics"]
    cards_metrics_latest: Dict[str, Dict[str, Optional[float]]] = {}
    cards_metrics_latest_ts: Dict[str, datetime] = {}
    for doc in coll_cards_metrics.find(
        {"cardId": {"$in": ids}},
        {
            "cardId": 1,
            "createdAt": 1,
            "latestPrice": 1,
            "growthRate": 1,
        },
    ):
        card_id = doc.get("cardId")
        latest_price = doc.get("latestPrice")
        created_at = doc.get("createdAt")
        if not isinstance(card_id, str):
            continue

        prev_ts = cards_metrics_latest_ts.get(card_id)
        if prev_ts is not None and isinstance(created_at, datetime) and created_at <= prev_ts:
            continue

        if prev_ts is None and card_id in cards_metrics_latest and not isinstance(created_at, datetime):
            continue

        v = _to_number(latest_price)
        cards_metrics_latest[card_id] = {
            "latestPrice": v,
            "psa10GrowthRate": _extract_cards_metrics_growth_value(doc),
        }
        if isinstance(created_at, datetime):
            cards_metrics_latest_ts[card_id] = created_at

    # Merge CardsMetrics latestPrice as psa10 fallback into graded_first.
    for cid in ids:
        if _to_number((graded_first.get(cid) or {}).get("psa10")) is None:
            fallback = (cards_metrics_latest.get(cid) or {}).get("latestPrice")
            if fallback is not None:
                graded_first.setdefault(cid, {})["psa10"] = fallback

    # Il PSA 10 di CardsMetrics vale anche come gradato che conferma cmPriceLow (_cardmarket_low_as_price):
    # alcune promo (es. Zoro CS 25-26 1st Place) non hanno gradati in PricesGraded.
    for cid, snaps in per_item.items():
        metrics_psa10 = _to_positive_number((cards_metrics_latest.get(cid) or {}).get("latestPrice"))
        if metrics_psa10 is None:
            continue
        for d in snaps:
            d[GRADED_MAX_KEY] = max(_to_positive_number(d.get(GRADED_MAX_KEY)) or 0.0, metrics_psa10)

    population_history_by_card: Dict[str, List[Dict[str, Any]]] = {}
    population_latest_prev_count: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    population_cursor = coll_cards_grading_population.find(
        {"cardId": {"$in": ids}},
        {
            "cardId": 1,
            "createdAt": 1,
            "grade10.totalCount": 1,
            "gemRate": 1,
            "gradingProvider": 1,
        },
    )

    for population_doc in population_cursor:
        card_id = population_doc.get("cardId")
        if isinstance(card_id, str):
            population_history_by_card.setdefault(card_id, []).append(population_doc)

    for docs in population_history_by_card.values():
        docs.sort(
            key=lambda d: d.get("createdAt") or datetime.min,
            reverse=True,
        )

    for cid, docs in population_history_by_card.items():
        latest_count: Optional[float] = None
        previous_count: Optional[float] = None
        for doc in docs:
            total_count = _extract_grade10_total_count(doc)
            if total_count is None:
                continue
            if latest_count is None:
                latest_count = total_count
                continue
            previous_count = total_count
            break
        population_latest_prev_count[cid] = (latest_count, previous_count)

    existing_stamps_by_item: Dict[str, Dict[str, Any]] = {}
    for stamp_doc in coll_card_stamps.find(
        {"itemId": {"$in": ids}},
        {
            "itemId": 1,
            "stamp": 1,
            "buyTier": 1,
            "priceBand": 1,
        },
    ):
        item_id = stamp_doc.get("itemId")
        if isinstance(item_id, str):
            existing_stamps_by_item[item_id] = stamp_doc

    character_popularity = _load_character_popularity(db)

    ops: List[UpdateOne] = []
    stamp_snapshot_ops: List[UpdateOne] = []
    stamp_change_ops: List[InsertOne] = []
    track_record_events: List[Dict[str, Any]] = []
    recent_calls = (
        recent_calls_index(db, ids, datetime.now(timezone.utc)) if track_record else {}
    )
    touched = 0
    updated = 0
    for cid in ids:
        prices = per_item.get(cid, [])
        now_utc = datetime.now(timezone.utc)
        base_card = base_cards_by_id.get(cid, {})
        previous_stamp_doc = existing_stamps_by_item.get(cid, {})

        psa10_count, psa10_count_previous = population_latest_prev_count.get(cid, (None, None))
        psa10_growth_rate = (cards_metrics_latest.get(cid) or {}).get("psa10GrowthRate")
        md = compute_market_data_for_item(
            prices,
            graded_first.get(cid, {}),
            psa10_count,
            psa10_count_previous,
            psa10_growth_rate,
            population_history_by_card.get(cid, []),
            previous_stamp=((previous_stamp_doc.get("stamp") or {}).get("key") if isinstance(previous_stamp_doc, dict) else None),
        )
        md["characterPopularity"] = _match_character_popularity(
            base_card.get("name"),
            character_popularity,
        )

        current_band = md.get("priceBand") if isinstance(md.get("priceBand"), dict) else None
        current_stamp = md.get("stamp") if isinstance(md.get("stamp"), dict) else None
        current_tier = md.get("buyTier") if isinstance(md.get("buyTier"), dict) else None

        prev_stamp_key = ((previous_stamp_doc.get("stamp") or {}).get("key") if isinstance(previous_stamp_doc, dict) else None) or ""
        prev_stamp_call = ((previous_stamp_doc.get("stamp") or {}).get("callDate") if isinstance(previous_stamp_doc, dict) else None)
        curr_stamp_key = (current_stamp or {}).get("key") or "HOLD"

        prev_tier_key = ((previous_stamp_doc.get("buyTier") or {}).get("key") if isinstance(previous_stamp_doc, dict) else None) or ""
        prev_tier_call = ((previous_stamp_doc.get("buyTier") or {}).get("callDate") if isinstance(previous_stamp_doc, dict) else None)
        curr_tier_key = (current_tier or {}).get("key") or ""

        stamp_call_date = prev_stamp_call if prev_stamp_key == curr_stamp_key and prev_stamp_call else now_utc
        buy_tier_call_date = prev_tier_call if prev_tier_key == curr_tier_key and prev_tier_call else (now_utc if curr_tier_key else None)

        if current_stamp is None:
            current_stamp = {
                "key": "HOLD",
                "label": STAMP_RULES["HOLD"]["label"],
                "kind": STAMP_RULES["HOLD"]["kind"],
            }
        current_stamp["callDate"] = stamp_call_date
        md["stamp"] = current_stamp

        if current_tier is not None and curr_tier_key:
            current_tier["callDate"] = buy_tier_call_date
            md["buyTier"] = current_tier

        if curr_stamp_key != prev_stamp_key and prev_stamp_key:
            stamp_change_ops.append(
                InsertOne(
                    {
                        "itemId": cid,
                        "setId": base_card.get("setId"),
                        "itemType": base_card.get("type"),
                        "fromStamp": prev_stamp_key,
                        "toStamp": curr_stamp_key,
                        "changedAt": now_utc,
                        "priceBandAtCall": current_band,
                        "buyTierAtCall": md.get("buyTier"),
                    }
                )
            )
            # Si verifica solo lo stamp (la chiamata); il pattern attivo resta come contesto
            # nel campo buyTierAtCall dell'evento. Le carte sotto TRACK_RECORD_MIN_PRICE_USD non si
            # registrano: i costi di vendita superano il movimento del prezzo. Nemmeno quelle con
            # priceConfidenceScore sotto soglia: lo stamp torna Hold perche' il prezzo non e' affidabile.
            if (
                track_record
                and (_to_number(md.get("price")) or 0.0) >= TRACK_RECORD_MIN_PRICE_USD
                and (_to_number(md.get("priceConfidenceScore")) or 0.0) >= SIGNALS_MIN_PRICE_CONFIDENCE
            ):
                event = build_call_event(
                    item_id=cid,
                    signal_type="stamp",
                    from_key=prev_stamp_key,
                    to_signal=md["stamp"],
                    called_at=now_utc,
                    base_card=base_card,
                    md=md,
                )
                if accept_call(recent_calls, event):
                    track_record_events.append(event)

        stamp_snapshot_doc = {
            "itemId": cid,
            "setId": base_card.get("setId"),
            "itemType": base_card.get("type"),
            "priceBand": current_band,
            "stamp": md.get("stamp"),
            "buyTier": md.get("buyTier"),
            "marketDataUpdatedAt": md.get("updatedAt"),
            "updatedAt": now_utc,
        }
        stamp_snapshot_ops.append(
            UpdateOne(
                {"itemId": cid},
                {
                    "$set": stamp_snapshot_doc,
                    "$setOnInsert": {"createdAt": now_utc},
                },
                upsert=True,
            )
        )

        touched += 1
        if any(v is not None for v in md.values()):
            ops.append(UpdateOne({"id": cid}, {"$set": {"marketData": md}}))

    if ops:
        modified_count, upserted_count = _bulk_write_resilient(
            coll_cards,
            ops,
            ordered=False,
        )
        updated = modified_count + upserted_count
    if stamp_snapshot_ops:
        _bulk_write_resilient(
            coll_card_stamps,
            stamp_snapshot_ops,
            ordered=False,
        )
    if stamp_change_ops:
        _bulk_write_resilient(
            coll_card_stamp_changes,
            stamp_change_ops,
            ordered=False,
        )
    if track_record_events:
        insert_append_only(db[TRACK_RECORD_CALLS_COLLECTION], track_record_events)

    sets_touched, sets_updated = update_sets_market_data(db, affected_set_ids)
    return (touched, updated, sets_touched, sets_updated)
