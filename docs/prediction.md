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
1) **Cards** → `prep_cards`  
   - Normalizza campi stringa, gestisce liste come first/pipe-join.  
   - Campi usati: `rarityName`, `rarityId`, `printing`, `color_1` (primo colore), `setId`, `setName`, `illustrator`, `cardType`, `subTypes`, `attribute`, `alternate`, `cost`, `power`.  
   - `card_age_weeks` calcolato da `releaseDate`; se null, usa `Sets.releaseDate` per quel `setId`.
2) **Prices** → `prep_prices_daily`  
   - Converte timestamp, media dinamica dei prezzi disponibili, calcola spread (max-min), mantiene `sellers`/`listings`, crea `date` giornaliera, prende ultimo record per itemId+giorno.
3) **Reindex** → `reindex_daily_fill`  
   - Reindicizza ogni itemId a frequenza giornaliera continua con forward-fill (price, sellers, listings, spread).
4) **Feature engineering** → `add_features_daily`  
   - Log price, ritorni 7/14/28/56d, volatilità 28d, momentum 14d, liquidity (`price_to_listings`, `sellers_to_listings`), variazioni listings/sellers, spread, liquidity index, shock indicator.  
5) (Training) **Target** → `add_target_28d` calcola `future_ret_28d` con shift -28d, clamp [-0.95, 5.0].  
6) Filtri: `filter_min_history` richiede almeno 84 giorni (default `ml.min_history_days`).

## Clustering “DNA”
- **Obiettivo**: raggruppare carte con profilo simile (rarità, set, archetipo, illustrator, costo/potenza) per dare al modello un segnale di contesto stabile e poco rumoroso.
- **Pipeline**: `ColumnTransformer` con `OneHotEncoder` sulle categoriali + passthrough numeriche, seguito da `MiniBatchKMeans` (`n_clusters` in `MLConfig`, default 30) per scalare su dataset grandi.
- **Categoriali**: `rarityName`, `rarityId`, `printing`, `color_1`, `setId`, `setName`, `illustrator`, `cardType`, `subTypes`, `attribute`.
- **Numeriche**: `alternate`, `cost`, `power`, `card_age_weeks`.
- **Output**: `clusterId` viene scritto su ogni riga di training/prediction e il `cluster_pipe` viene salvato negli artifacts (serve per replicare la stessa trasformazione in fase di prediction).

## Tiering
- **Perché**: la dinamica di prezzo cambia drasticamente tra carte low-cost e chase cards. Tier separati evitano che il modello “spalmi” pattern incompatibili.
- **Regole default** (`MLConfig`): `low < 5`, `mid 5–150`, `high > 150` (euro-equivalenti del price corrente).
- **Uso**: il tier viene calcolato sul prezzo attuale (`priceNow`) e decide quale gruppo di modelli quantile usare.

## Modelli
- **Architettura**: per ogni tier, 3 modelli LightGBM in modalità `objective="quantile"` per q ∈ {0.2, 0.5, 0.8}.
- **Preprocess**: OneHot su cat_cols + passthrough num_cols (stessi elenchi salvati negli artifacts).
- **Feature numeriche chiave**: log_price, ritorni 7/14/28/56d, volatilità 28d, momentum 14d, liquidity (price_to_listings, sellers_to_listings, variazioni listings/sellers), spread, shock indicator, alternate, cost, power, card_age_weeks, clusterId.
- **Artifacts** (`artifacts/optcg_quantile_artifacts.joblib`):
  ```
  {
    "asof": ts,
    "ml_config": MLConfig,
    "mongo_config": MongoConfig,
    "cat_cols": [...],
    "num_cols": [...],
    "tier_models": {tier: TierModels},
    "cluster_pipe": Pipeline
  }
  ```

## Flusso di training
`src/prediction/trainer.py::train_all(artifacts_dir="./artifacts")`
1) Carica **Cards**, **Sets**, **Prices** da Mongo.
2) Prepara Cards (fill releaseDate da Sets se manca), Prices→daily, reindex e feature, filtra min_history.
3) Crea target `future_ret_28d`, merge con statiche Cards.
4) Fit clustering → aggiunge `clusterId`.
5) Per ogni tier con dati sufficienti: fit modelli quantile.
6) Salva tutti gli artifacts.

## Flusso di prediction
`src/prediction/predictor.py::predict_and_store(artifacts_dir="./artifacts")`
1) Carica artifacts e dati aggiornati (Cards, Sets, Prices).
2) Ricostruisce le feature al giorno più recente per ogni itemId.
3) Applica `cluster_pipe`, calcola tier, riempie NaN numerici a 0.
4) Predice quantili per il tier di ciascuna carta, salva in `ml_predictions_daily` (upsert `_id=cardId`).
5) Costruisce ranking top_up/top_down globali e per tier, salva in `ml_rankings` con `_id="latest"`.

## Come eseguire
- Training completo: `python -m src.prediction.trainer` (o import `train_all()`).
- Prediction con artifacts esistenti: `python -m src.prediction.predictor` (o import `predict_and_store()`).

## Note operative
- Aggiorna gli artifacts dopo modifiche a feature/clustering (`train_all`), altrimenti `predict_and_store` userà il vecchio schema.
- I parametri chiave (cluster count, soglie tier, finestre rolling) sono in `src/prediction/config.py::MLConfig`.
