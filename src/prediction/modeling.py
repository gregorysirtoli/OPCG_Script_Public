from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

@dataclass
class TierModels:
    # quantile -> pipeline
    models: Dict[float, Pipeline]
    feature_cols_num: list[str]
    feature_cols_cat: list[str]

def build_preprocessor(cat_cols: list[str], num_cols: list[str]) -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", "passthrough", num_cols),
        ],
        remainder="drop"
    )

def make_lgbm_quantile(alpha: float) -> LGBMRegressor:
    # Parametri robusti per tabellare / non troppo aggressivi
    return LGBMRegressor(
        objective="quantile",
        alpha=alpha,
        n_estimators=1200,
        learning_rate=0.03,
        num_leaves=63,
        min_child_samples=40,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1
    )

def fit_tier_models(df: pd.DataFrame, y_col: str, cat_cols: list[str], num_cols: list[str], quantiles: tuple[float, ...]) -> TierModels:
    X = df[cat_cols + num_cols]
    y = df[y_col].astype(float).values

    pre = build_preprocessor(cat_cols, num_cols)

    models: Dict[float, Pipeline] = {}
    for q in quantiles:
        reg = make_lgbm_quantile(q)
        pipe = Pipeline([("pre", pre), ("lgbm", reg)])
        pipe.fit(X, y)
        models[q] = pipe

    return TierModels(models=models, feature_cols_num=num_cols, feature_cols_cat=cat_cols)

def predict_tier_models(tier_models: TierModels, df: pd.DataFrame) -> dict[float, np.ndarray]:
    X = df[tier_models.feature_cols_cat + tier_models.feature_cols_num]
    qs = sorted(tier_models.models)
    if not qs:
        return {}
    preds = np.column_stack([tier_models.models[q].predict(X) for q in qs])
    # I modelli sono addestrati indipendentemente per quantile, quindi possono incrociarsi
    # (es. q80 < q50): riordino per riga così che i quantili siano monotoni (rearrangement).
    crossed = int((np.diff(preds, axis=1) < 0).any(axis=1).sum())
    if crossed:
        print(f"[WARN] Quantili incrociati su {crossed}/{len(preds)} righe: riordinati.")
        preds = np.sort(preds, axis=1)
    return {q: preds[:, i] for i, q in enumerate(qs)}
