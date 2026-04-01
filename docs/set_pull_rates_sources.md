# SetPullRates Sources

Questo file raccoglie le fonti da usare per costruire una collection `SetPullRates` senza toccare ancora la logica EV.

## Regole di affidabilita'

- `official`: composizione prodotto, rarita', release, naming. Non fornisce pull rates esatti.
- `community`: stime aggregate o articoli che esplicitano di basarsi su opening data.
- `video`: utile come evidenza osservazionale, ma da usare solo insieme ad altre fonti.
- `retailer/aggregator`: utile per packaging e sanity check, non per odds ufficiali.

## Official Product Pages OP01-OP13

Le URL ufficiali EN/JP seguono un pattern stabile `.../products/boosters/opXX.php`.
Il pattern e' stato verificato direttamente su OP01, OP02, OP12 e OP13.

| Set | Name | EN official | JP official |
| --- | --- | --- | --- |
| OP01 | Romance Dawn | https://en.onepiece-cardgame.com/products/boosters/op01.php | https://www.onepiece-cardgame.com/products/boosters/op01.php |
| OP02 | Paramount War | https://en.onepiece-cardgame.com/products/boosters/op02.php | https://www.onepiece-cardgame.com/products/boosters/op02.php |
| OP03 | Pillars of Strength | https://en.onepiece-cardgame.com/products/boosters/op03.php | https://www.onepiece-cardgame.com/products/boosters/op03.php |
| OP04 | Kingdoms of Intrigue | https://en.onepiece-cardgame.com/products/boosters/op04.php | https://www.onepiece-cardgame.com/products/boosters/op04.php |
| OP05 | Awakening of the New Era | https://en.onepiece-cardgame.com/products/boosters/op05.php | https://www.onepiece-cardgame.com/products/boosters/op05.php |
| OP06 | Wings of the Captain | https://en.onepiece-cardgame.com/products/boosters/op06.php | https://www.onepiece-cardgame.com/products/boosters/op06.php |
| OP07 | 500 Years in the Future | https://en.onepiece-cardgame.com/products/boosters/op07.php | https://www.onepiece-cardgame.com/products/boosters/op07.php |
| OP08 | Two Legends | https://en.onepiece-cardgame.com/products/boosters/op08.php | https://www.onepiece-cardgame.com/products/boosters/op08.php |
| OP09 | Emperors in the New World | https://en.onepiece-cardgame.com/products/boosters/op09.php | https://www.onepiece-cardgame.com/products/boosters/op09.php |
| OP10 | Royal Blood | https://en.onepiece-cardgame.com/products/boosters/op10.php | https://www.onepiece-cardgame.com/products/boosters/op10.php |
| OP11 | A Fist of Divine Speed | https://en.onepiece-cardgame.com/products/boosters/op11.php | https://www.onepiece-cardgame.com/products/boosters/op11.php |
| OP12 | Legacy of the Master | https://en.onepiece-cardgame.com/products/boosters/op12.php | https://www.onepiece-cardgame.com/products/boosters/op12.php |
| OP13 | Carrying On His Will | https://en.onepiece-cardgame.com/products/boosters/op13.php | https://www.onepiece-cardgame.com/products/boosters/op13.php |

## Community / Estimation Sources

### Global / JP baseline

- Cardcosmos, unofficial JP pull-rate summary:
  https://cardcosmos.de/en/blogs/news/one-piece-card-game-pull-rates-hitrates-der-japanischen-edition
  Notes:
  SR `4-5` per display, SEC `0-1`, parallels `1-2`, manga `0-1` per case.
  Good as a baseline, not as set-specific truth.

### OP13-specific sources

- Samurai Sword Tokyo, OP13 estimated pull-rate article:
  https://samuraiswordtokyo.com/es/blogs/news/op-13-pull-rates-best-cards
  Notes:
  Gives OP13-specific estimated ranges for SR, SEC, SP, Red Super Parallel, anniversary inserts and Demon Pack.
  Commercial source, therefore low confidence.

- YouTube case study:
  https://www.youtube.com/watch?v=Hg33k6F5Pio
  Notes:
  Useful as observed case data, not sufficient alone.

- Reddit case pulls:
  https://www.reddit.com/r/OnePieceTCGFinance/comments/1p2ztma/op13_case_pulls/
  Notes:
  Useful only for coarse case-map discussion and anomalies.

- Reddit hit-rates discussion:
  https://www.reddit.com/r/OnePieceTCGFinance/comments/1okrfoo/op13_hit_rates/
  Notes:
  Useful for weak signals around Gold DON, leader parallels and case-hit interactions.

## Suggested ingest policy

1. Use official EN/JP pages for product composition.
2. Use one generic community baseline per language family.
3. Use set-specific articles/videos only to override or refine a subset of buckets.
4. Save a `confidence` score per bucket, not only per set.
5. Keep `coreEv` and `fullEv` as two separate include-lists.
