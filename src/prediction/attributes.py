from __future__ import annotations

import pandas as pd

from .io_mongo import load_collection

# Attributi già gestiti come colonne fisse altrove nella pipeline: vanno
# esclusi dal set dinamico anche se presenti nell'anagrafica CardsCustomAttributes.
CORE_ATTRIBUTE_KEYS = {"illustrator", "rarityId", "rarityName", "rarity", "productType"}

FIXED_CAT_COLS = ["rarityName", "rarityId", "illustrator", "setId", "setName"]
FIXED_NUM_COLS = ["card_age_weeks"]


def load_attribute_registry(db, col_name: str = "CardsCustomAttributes") -> pd.DataFrame:
    """
    Anagrafica dinamica degli attributi custom tracciati (collection CardsCustomAttributes).
    Ogni doc: {attributeKey: str, values: list[str]} (valori distinti osservati, sempre come stringhe).
    """
    return load_collection(db, col_name, projection={"attributeKey": 1, "values": 1})


def _looks_numeric(values: list) -> bool:
    if not values:
        return False

    def is_num(v) -> bool:
        try:
            float(v)
            return True
        except (TypeError, ValueError):
            return False

    return sum(is_num(v) for v in values) / len(values) >= 0.9


def resolve_dynamic_attributes(
    registry: pd.DataFrame,
    cards: pd.DataFrame,
    max_categories: int = 300,
) -> tuple[list[str], list[str]]:
    """
    Incrocia l'anagrafica CardsCustomAttributes con le carte effettivamente caricate per
    decidere quali attributeKey usare come feature, e se trattarli come categoriali o numerici.
    - un attributeKey viene incluso solo se compare davvero (top-level o dentro customAttributes)
      su almeno una carta del batch caricato
    - viene classificato numerico se >=90% dei valori noti in anagrafica sono parsabili come float
    - un attributeKey categoriale con troppi valori distinti (probabile testo libero, non
      un'anagrafica reale) viene scartato per evitare un one-hot enorme
    """
    if registry.empty:
        return [], []

    present_keys: set[str] = set(c for c in cards.columns if c != "customAttributes")
    if "customAttributes" in cards.columns:
        for ca in cards["customAttributes"].dropna():
            if isinstance(ca, dict):
                present_keys.update(ca.keys())

    cat_keys: list[str] = []
    num_keys: list[str] = []
    for _, row in registry.iterrows():
        key = row.get("attributeKey")
        if not key or key in CORE_ATTRIBUTE_KEYS or key not in present_keys:
            continue

        values = row.get("values") or []
        numeric = _looks_numeric(values)
        if not numeric and len(values) > max_categories:
            continue

        (num_keys if numeric else cat_keys).append(key)

    return sorted(cat_keys), sorted(num_keys)


def _first_or_none(x):
    if isinstance(x, list):
        return x[0] if x else None
    return x


def _list_to_key(x) -> str:
    if isinstance(x, list):
        vals = [str(v).strip() for v in x if v not in (None, "")]
        return "|".join(sorted(set(vals)))
    return str(x).strip() if x not in (None, "") else ""


def flatten_dynamic_attributes(
    df: pd.DataFrame,
    cat_keys: list[str],
    num_keys: list[str],
) -> pd.DataFrame:
    """
    Materializza le colonne attr_<key> lette da un attributeKey: prima dal campo
    top-level omonimo se esiste (es. 'variant'), altrimenti da customAttributes.<key>
    (es. 'hp', 'stage'). Necessario perché lo stesso attributeKey può vivere in
    posizioni diverse a seconda del gioco/epoca di ingestion della carta.
    """
    out = df.copy()

    if "customAttributes" in out.columns:
        ca = out["customAttributes"].apply(lambda d: d if isinstance(d, dict) else {})
    else:
        ca = pd.Series([{}] * len(out), index=out.index)

    def resolve(key: str) -> pd.Series:
        top = out[key] if key in out.columns else pd.Series([None] * len(out), index=out.index)
        nested = ca.apply(lambda d: d.get(key))
        return top.where(top.notna(), nested)

    for key in cat_keys:
        out[f"attr_{key}"] = resolve(key).apply(_list_to_key)

    for key in num_keys:
        raw = resolve(key).apply(_first_or_none)
        out[f"attr_{key}"] = pd.to_numeric(raw, errors="coerce").fillna(0)

    return out


def build_feature_cols(
    dynamic_cat_keys: list[str],
    dynamic_num_keys: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """
    Unica fonte di verità per le colonne statiche (fisse + dinamiche da customAttributes)
    usate da clustering e modelli. Va richiamata identica in train e predict, con gli
    stessi dynamic_cat_keys/dynamic_num_keys salvati nell'artifact di training.
    """
    dyn_cat_cols = [f"attr_{k}" for k in dynamic_cat_keys]
    dyn_num_cols = [f"attr_{k}" for k in dynamic_num_keys]

    cat_cols = FIXED_CAT_COLS + dyn_cat_cols
    static_num_cols = FIXED_NUM_COLS + dyn_num_cols
    cluster_cols = cat_cols + static_num_cols

    return cat_cols, static_num_cols, cluster_cols
