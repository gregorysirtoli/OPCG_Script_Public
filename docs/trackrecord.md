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
stessa carta ha gia' una chiamata registrata con lo stesso stamp negli ultimi 14 giorni. La finestra
parte dall'ultima chiamata registrata, quindi uno stamp che oscilla (Buy Setup -> Hold -> Buy Setup...)
entra nel registro al massimo una volta ogni 14 giorni.

**Soglia di 5 $** (`TRACK_RECORD_MIN_PRICE_USD`): sotto questo prezzo commissione e spedizione superano il
movimento del prezzo, quindi le chiamate non vengono registrate e le carte non entrano nel benchmark.

## Regole degli stamp v2 (ottobre 2026)

Studio su 2.500 carte e 170 giorni: con le regole v1 una carta cambiava stamp 8,1 volte ogni 90 giorni e
nel 59% dei casi tornava allo stamp precedente entro 7 giorni (soglie secche su variazioni di un solo
giorno, rumore del prezzo +-3%). Le regole v2 (`_compute_stamp_inputs`, `_classify_stamp` in
`getMarketData.py`):

- **Prezzi ammorbiditi**: variazioni calcolate su mediane di 3 giorni (oggi vs giorni 6-8 e 29-31). Un
  crollo di un solo giorno non cambia lo stamp; uno vero viene riconosciuto con circa un giorno di ritardo.
- **Isteresi**: si entra con le soglie di entrata, si esce solo quando il segnale e' davvero finito
  (`_stamp_still_valid`). Un nuovo stamp diverso da Hold sostituisce sempre quello attuale.
- **Soglie vicine allo zero proporzionate alla fascia**: Buy Setup `p7 >= +2%` e `p30 >= +5%` (prima 0% e
  4%); Rebound senza la condizione sul giorno singolo e con `p7 >= +4%`; Accumulate $200+ `p7 >= +3%`.
- **Sell Watch ridefinito**: prezzo in calo a 7 giorni (`slip7`: -7/-6/-5/-4% dalle carte economiche alle
  care) **e** a 30 giorni (`p30 <= 0`) **con** almeno +15% di copie in vendita in 30 giorni. Esce quando
  il prezzo a 7 giorni torna sopra zero o l'offerta scende sotto +5%. Nello studio su 3.000 carte il
  Sell Watch v1 non aveva vantaggio (dopo la chiamata la carta andava come il mercato) e il "mese forte
  che scivola" era controproducente (+2,4 punti sul mercato: la carta tendeva a risalire); il calo con
  offerta in aumento fa peggio del mercato nei 2/3 dei casi.

Risultato (3.000 carte, 170 giorni): da 8,1 a 2,9 cambi di stamp ogni 90 giorni (-64%). A 4 settimane:
Sell Watch giuste 67% (prima 48%), -5,5 punti sul mercato (prima -0,8; -8,3 a 8 settimane); Buy Setup
+8,7 punti sul mercato; Accumulate e Hold invariati. Le `percentageChange*` mostrate sul sito restano
quelle giornaliere; quelle usate dallo stamp sono in `marketData.stampInputs`.

**Solo lo stamp viene verificato.** Il pattern (buy tier) descrive il movimento che ha portato allo stamp: non e' una
chiamata a se', non genera eventi e resta come contesto nel campo `buyTierAtCall`.

Stamp e previsioni ML sono due parti indipendenti: un evento di stamp non contiene la previsione e
il suo esito dipende solo dal prezzo; le previsioni sono valutate per conto loro.

Tutte le collection stanno nel DB principale (`MONGODB_DB`). `CardsStampChanges` sta nel DB vendite (`MONGODB_SALES_DB`), resta com'e' ma non e'
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
4. **Versioni**: cambiando soglie o logica di stamp o fasce si alza `SIGNAL_RULES_VERSION`
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
