# Track record pubblico (stamp e previsioni)

Ogni segnale viene registrato il giorno in cui scatta e valutato a 4 e 8 settimane, errori inclusi:
al netto dei costi di vendita, rispetto al mercato della stessa fascia e con la liquidita' dichiarata.

## Flusso

| Quando | Cosa | Dove |
| --- | --- | --- |
| Ogni giorno, MarketData One Piece | Cambio di stamp -> un evento | `getMarketData.update_cards_market_data(track_record=True)` -> `TrackRecordCalls` |
| Lunedi', Prediction Daily | Fotografia delle previsioni a 28 giorni (Q20/Q50/Q80) | `predictor.snapshot_forecasts` -> `TrackRecordForecasts` |
| Ogni giorno, dopo MarketData | Esiti, calibrazione, riepilogo, impronte | `OnePiece - TrackRecord.yml` -> `src.trackrecord.evaluate` |

Solo One Piece: la registrazione parte solo con `TRACK_RECORD_ENABLED=1`, impostata in
`OnePiece - marketData.yml`; gli altri giochi usano la stessa funzione ma non scrivono nulla.

**Cooldown di 14 giorni** (`REENTRY_COOLDOWN_DAYS` in `ledger.py`): un cambio non viene registrato se la
stessa carta ha gia' una chiamata registrata con lo stesso stamp negli ultimi 14 giorni. La finestra parte dall'ultima chiamata registrata, quindi uno stamp che oscilla
(Buy Setup -> Hold -> Buy Setup...) entra nel registro al massimo una volta ogni 14 giorni. Sulla carta
OP09 Buggy [Alternate Art], un anno di storico: 21 cambi di stamp, 12 registrati.

**Solo lo stamp viene verificato.** Il pattern (buy tier) descrive il movimento che ha portato allo stamp: non e' una
chiamata a se', non genera eventi e resta come contesto nel campo `buyTierAtCall`.

Stamp e previsioni ML sono due parti indipendenti: un evento di stamp non contiene la previsione e
il suo esito dipende solo dal prezzo; le previsioni sono valutate per conto loro.

Tutte le collection stanno nel DB principale (`MONGODB_DB`). `CardsStampChanges` resta com'e' ma non e'
il registro: non ha piu' TTL, ma il backfill delle regole la cancella e la riscrive con le regole attuali.

## Collection

- **TrackRecordCalls** (solo in aggiunta): `_id = itemId|signalType|yyyymmddTHHMMSS`, `calledAt`, carta
  (`itemId`, `name`, `setId`, `itemType`, `condition: RAW`), `signalType` (`stamp`), `buyTierAtCall` (pattern attivo, solo contesto),
  `fromKey` -> `toKey`, `rulesVersion`, `price` (valore, fonte `priceRedLine`, USD, `priceConfidenceScore`),
  `liquidity` (score, livello VERY_HIGH...VERY_LOW come il badge del sito, sellers, listings, vendite stimate
  7/30 gg), `priceBand`, `inputs` (variazioni % che hanno fatto scattare il segnale).
- **TrackRecordForecasts** (solo in aggiunta): `_id = cardId|yyyy-mm-dd`, una riga per carta ogni lunedi'.
- **TrackRecordOutcomes** (solo in aggiunta): `_id = eventId|28d` o `|56d`. Prezzo di uscita, rendimento
  lordo e netto, soglia di costo, rendimento del mercato di fascia, scarto, calo massimo, `success`,
  `thinMarket`, `status` (`evaluated` / `no_price`).
- **TrackRecordForecastOutcomes** (solo in aggiunta): rendimento reale, fascia (`below_q20` / `inside` /
  `above_q80`), errore di Q50 e della previsione "prezzo invariato".
- **TrackRecordBenchmarks** (solo in aggiunta): mediana dei rendimenti di tutte le carte per tipo e fascia,
  per giorno e orizzonte.
- **TrackRecordSummary** (`_id: latest`, riscritto ogni giorno): `headline`, tabella `calls` per versione
  regole x segnale x orizzonte x fascia x liquidita' (con le righe `ALL`), tabella `forecasts` per tier.
- **TrackRecordDigests** (solo in aggiunta): impronte giornaliere, pubblicate ogni giorno in
  `onepiece-digests.csv` del repository pubblico <https://github.com/redline-cards/OPCG-Track-Record>
  (secret `TRACKRECORD_REPO_TOKEN`: token fine-grained con Contents read/write solo su quel repository).

## Metodo

- **Prezzo**: `priceRedLine`, mediana delle fonti della riga di Prices (`pricePrimary`, Cardmarket trend,
  low e media 30 gg, CardTrader, PriceCharting, Yuyu-tei), in USD. Ingresso = prezzo del giorno della chiamata. Uscita = mediana dei prezzi
  giornalieri tra t+h-3 e t+h+3 giorni.
- **Soglia di costo** = `SELL_FEE_PCT` + `SHIPPING_COST_USD` / prezzo. Rendimento netto = lordo - soglia.
- **Mercato**: mediana del rendimento di tutte le carte dello stesso tipo e fascia nello stesso periodo
  (serve un gruppo di almeno 30 carte, altrimenti si usa la fascia senza tipo).
- **Successo**:

| Segnale | Successo |
| --- | --- |
| Accumulate, Buy Setup | netto > 0 e lordo sopra il mercato |
| Rebound Watch | netto > 0 (si guardano anche mediana e 80esimo percentile) |
| Sell Watch | prezzo sceso e sotto il mercato |
| Hold | prezzo entro la soglia di costo, in su o in giu' |

- **Previsioni**: circa 20% sotto Q20, 60% dentro, 20% sopra Q80. Q50 deve avere un errore mediano minore di
  "il prezzo resta uguale" (`beatsNaive`).
- Mediane, non medie. Percentuali solo con almeno 30 valutazioni (`sampleTooSmall` altrimenti).
- `thinMarket`: meno di 3 prezzi nella finestra di uscita o nessuna vendita stimata.

## Regole del registro

1. **Solo in aggiunta**: il codice scrive solo con insert su `_id` deterministici. Per renderlo vincolante
   lato database, il job dovrebbe usare un utente Atlas con un ruolo personalizzato che su `TrackRecordCalls`,
   `TrackRecordForecasts`, `TrackRecordOutcomes`, `TrackRecordForecastOutcomes`, `TrackRecordBenchmarks`
   e `TrackRecordDigests` abbia solo `find` e `insert`.
2. **Correzioni**: un nuovo evento con il campo `corrects: <_id dell'evento sbagliato>`, mai una modifica.
3. **Impronte**: per ogni giorno chiuso, SHA-256 delle righe canoniche (`ledger.canonical_line`: JSON con
   chiavi ordinate, senza spazi, date ISO UTC al secondo con `Z`, numeri con al massimo 6 decimali senza
   zeri finali, righe ordinate e unite da `\n`; stessa regola in `OPCG/lib/trackRecord.ts`) di chiamate e
   previsioni del giorno, piu' `chainSha256 = sha256(chain del giorno prima|giorno|callsSha|forecastsSha)`.
   Ogni giro ricalcola tutte le impronte passate e segnala via mail quelle diverse.
4. **Versioni**: cambiando soglie o logica di stamp, buy tier o fasce si alza `SIGNAL_RULES_VERSION`
   (`src/trackrecord/ledger.py`). Cambiando costi o finestre si alza `EVAL_METHOD_VERSION`
   (`src/trackrecord/evaluate.py`). Nessun reset: le versioni precedenti restano nel riepilogo.
5. **Backtest e dal vivo separati**: i `proof` in `PRICE_BAND_RULES` e `BUY_TIER_RULES` sono numeri di
   backtest. Nel marketing vanno usati solo i numeri di `TrackRecordSummary`, con data e campione.

## Sito

Pagina pubblica `/track-record` nel progetto `OPCG` (`app/track-record`, `lib/trackRecord.ts`): legge
`TrackRecordSummary`, `TrackRecordCalls`, `TrackRecordOutcomes` e `TrackRecordDigests`, ed espone l'export
`/api/track-record/export?day=YYYY-MM-DD&kind=calls|forecasts` (righe canoniche: il loro SHA-256 coincide
con l'impronta) e `?month=YYYY-MM` (CSV con gli esiti). Cambiando `canonical_line` va cambiata anche
`canonicalLine` nel sito.

## Da fare

- Segnali passati sul grafico del prezzo nella pagina della carta.
- Verificare `SELL_FEE_PCT` e `SHIPPING_COST_USD` con le tariffe reali di CardTrader e Cardmarket.
