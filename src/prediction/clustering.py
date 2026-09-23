from __future__ import annotations

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.cluster import MiniBatchKMeans

def build_cluster_pipeline(cat_cols: list[str], num_cols: list[str], n_clusters: int) -> Pipeline:
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

def fit_clusters(cards_df: pd.DataFrame, cat_cols: list[str], num_cols: list[str], n_clusters: int) -> tuple[Pipeline, pd.Series]:
    pipe = build_cluster_pipeline(cat_cols, num_cols, n_clusters)
    X = cards_df[cat_cols + num_cols]
    pipe.fit(X)
    cluster_ids = pipe.predict(X)
    return pipe, pd.Series(cluster_ids, index=cards_df.index, name="clusterId")

def predict_clusters(pipe: Pipeline, cards_df: pd.DataFrame, cat_cols: list[str], num_cols: list[str]) -> pd.Series:
    X = cards_df[cat_cols + num_cols]
    cluster_ids = pipe.predict(X)
    return pd.Series(cluster_ids, index=cards_df.index, name="clusterId")
