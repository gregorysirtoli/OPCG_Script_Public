# Prediction Pipeline (OPCG)

## Scopo
Prevedere la distribuzione del rendimento a 28 giorni dei prezzi delle carte (upside/mediana/downside) e pubblicare:
- `ml_predictions_daily`: 1 documento per cardId con tier, clusterId e quantili.
- `ml_rankings`: liste top_up / top_down globali e per tier.

## Dati in ingresso
- **Cards** (collection `Cards`): metadati statici della carta.
- **Prices** (collection `Prices`): storico prezzi e liquidity giornaliera.
- **Sets** (collection `Sets`): nome e releaseDate del set (fallback se manca in Cards).

## Preprocessing
1) **Cards** ? `prep_cards`
   - Normalizza campi stringa, gestisce liste come first/pipe-join.
   - Campi usati: `rarityName`, `rarityId`, `printing`, `color_1` (primo colore), `setId`, `setName`, `illustrator`, `cardType`, `subTypes`, `attribute`, `alternate`, `cost`, `power`.
   - `card_age_weeks` calcolato da `releaseDate`; se null, usa `Sets.releaseDate` per quel `setId`.
2) **Prices** ? `prep_prices_daily`
   - Converte timestamp, media dinamica dei prezzi disponibili, calcola spread (max-min), mantiene `sellers`/`listings`, crea `date` giornaliera, prende ultimo record per itemId+giorno.
   - **Ottimizzazione fetch:** in `trainer.py` il download è limitato agli ultimi 400 giorni e proietta solo i campi necessari (riduce I/O da Mongo).
3) **Reindex** ? `reindex_daily_fill`
   - Reindicizza ogni itemId a frequenza giornaliera continua con forward-fill (price, sellers, listings, spread).
   - **Clamp span:** se l’intervallo supera 400 giorni viene tagliata la coda vecchia per evitare esplosione di righe da date anomale.
4) **Feature engineering** ? `add_features_daily`
   - Log price, ritorni 7/14/28/56d, volatilità 28d, momentum 14d, liquidity (`price_to_listings`, `sellers_to_listings`), variazioni listings/sellers, spread, liquidity index, shock indicator.
5) (Training) **Target** ? `add_target_28d` calcola `future_ret_28d` con shift -28d, clamp [-0.95, 5.0].
6) Filtri: `filter_min_history` richiede almeno 84 giorni (default `ml.min_history_days`).

## Clustering “DNA”
- **Obiettivo**: raggruppare carte con profilo simile (rarità, set, archetipo, illustrator, costo/potenza) per dare al modello un segnale di contesto stabile.
- **Pipeline**: `ColumnTransformer` con `OneHotEncoder` sulle categoriali + passthrough numeriche, seguito da `MiniBatchKMeans` (`n_clusters` in `MLConfig`, default 30).
- **Categoriali**: `rarityName`, `rarityId`, `printing`, `color_1`, `setId`, `setName`, `illustrator`, `cardType`, `subTypes`, `attribute`.
- **Numeriche**: `alternate`, `cost`, `power`, `card_age_weeks`.
- **Output**: `clusterId` scritto su ogni riga e `cluster_pipe` salvato negli artifacts per la prediction.

## Tiering
- **Perché**: la dinamica di prezzo cambia molto tra carte low-cost e chase cards; tier separati evitano pattern incompatibili.
- **Regole default** (`MLConfig`): `low < 5`, `mid 5–150`, `high > 150` (eur-equivalenti del prezzo corrente).
- **Uso**: il tier è calcolato sul prezzo attuale e seleziona il gruppo di modelli quantile.

## Modelli
- **Architettura**: per ogni tier, 3 modelli LightGBM quantile per q ? {0.2, 0.5, 0.8}.
- **Preprocess**: OneHot su cat_cols + passthrough num_cols (elenchi salvati negli artifacts).
- **Feature numeriche chiave**: log_price, ritorni 7/14/28/56d, vol 28d, momentum 14d, liquidity (price_to_listings, sellers_to_listings, variazioni listings/sellers), spread, shock indicator, alternate, cost, power, card_age_weeks, clusterId.
- **Artifacts** (`artifacts/optcg_quantile_artifacts.joblib`):
  {
    "asof": ts,
    "ml_config": MLConfig,
    "mongo_config": MongoConfig,
    "cat_cols": [...],
    "num_cols": [...],
    "tier_models": {tier: TierModels},
    "cluster_pipe": Pipeline
  }

## Flusso di training
`src/prediction/trainer.py::train_all(artifacts_dir="./artifacts")`
1) Carica **Cards**, **Sets**, **Prices** da Mongo (finestra 400 giorni, proiezione campi essenziali).
2) Prepara Cards (fill releaseDate da Sets se manca), Prices?daily, reindex (clamp span) e feature, filtra min_history.
3) Crea target `future_ret_28d`, merge con statiche Cards.
4) Fit clustering ? aggiunge `clusterId`.
5) Per ogni tier con dati sufficienti: fit modelli quantile.
6) Salva tutti gli artifacts.
7) Log di timing per ogni fase (load, prep, reindex, feature, target, merge, clustering, training per tier) per diagnosticare colli di bottiglia.

## Flusso di prediction
`src/prediction/predictor.py::predict_and_store(artifacts_dir="./artifacts")`
1) Carica artifacts e dati aggiornati (Cards, Sets, Prices).
2) Ricostruisce le feature al giorno più recente per ogni itemId.
3) Applica `cluster_pipe`, calcola tier, riempie NaN numerici a 0.
4) Predice quantili per il tier di ciascuna carta, salva in `ml_predictions_daily` (upsert `_id=cardId`).
5) Costruisce ranking top_up/top_down globali e per tier, salva in `ml_rankings` con `_id="latest"`.

## Come eseguire
- Training completo: `python -m src.prediction.run_train` (o import `train_all()`).
- Prediction con artifacts esistenti: `python -m src.prediction.run_predict` (o import `predict_and_store()`).
  - Entrambe le run stampano start/end/durata e inviano email di report con orari e tempo totale (minuti/secondi).

## Note operative
- Aggiorna gli artifacts dopo modifiche a feature/clustering (`train_all`), altrimenti `predict_and_store` userà il vecchio schema.
- I parametri chiave (cluster count, soglie tier, finestre rolling) sono in `src/prediction/config.py::MLConfig`.
