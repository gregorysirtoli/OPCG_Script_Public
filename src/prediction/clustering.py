from __future__ import annotations

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.cluster import MiniBatchKMeans

def build_cluster_pipeline(n_clusters: int) -> Pipeline:
    cat_cols = ["rarityName", "printing", "color_1", "setId"]
    num_cols = ["alternate", "card_age_weeks"]

    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", "passthrough", num_cols),
        ],
        remainder="drop"
    )

    kmeans = MiniBatchKMeans(
        n_clusters=n_clusters,
        random_state=42,
        batch_size=4096
    )

    pipe = Pipeline([("pre", pre), ("kmeans", kmeans)])
    return pipe

def fit_clusters(cards_df: pd.DataFrame, n_clusters: int) -> tuple[Pipeline, pd.Series]:
    pipe = build_cluster_pipeline(n_clusters)
    X = cards_df[["rarityName", "printing", "color_1", "setId", "alternate", "card_age_weeks"]]
    pipe.fit(X)
    cluster_ids = pipe.predict(X)
    return pipe, pd.Series(cluster_ids, index=cards_df.index, name="clusterId")

def predict_clusters(pipe: Pipeline, cards_df: pd.DataFrame) -> pd.Series:
    X = cards_df[["rarityName", "printing", "color_1", "setId", "alternate", "card_age_weeks"]]
    cluster_ids = pipe.predict(X)
    return pd.Series(cluster_ids, index=cards_df.index, name="clusterId")
