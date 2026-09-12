#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from pymongo import MongoClient
from src.core.emailer import send_email


load_dotenv(Path(__file__).resolve().parents[2] / ".env.local")
load_dotenv()
DETAILS_URL_TEMPLATE = os.getenv("PRIMARY_URL_TEMPLATE", "")
CARDS_COLL = "Cards"


def require_env(keys: list[str]) -> None:
    missing = [key for key in keys if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing env vars: " + ", ".join(missing))


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    text = str(value).strip()
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def positive_int(value: Any) -> Optional[int]:
    out = as_int(value)
    if out is None or out <= 0:
        return None
    return out


def to_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def normalize_id_list(*values: Any) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for block in values:
        for raw in to_list(block):
            value = positive_int(raw)
            if value is None or value in seen:
                continue
            seen.add(value)
            out.append(value)
    return out


def as_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def score_matches(value: Any, target: float) -> bool:
    current = as_float(value)
    if current is None:
        return False
    return math.isclose(current, target, rel_tol=0.0, abs_tol=1e-12)


def get_card_details(product_id: int, timeout: int = 30) -> dict[str, Any]:
    # Support both placeholders during transition: {id} and {product_id}.
    url = DETAILS_URL_TEMPLATE.format(id=product_id, product_id=product_id)
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                return {}
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return {}


def unwrap_details(details_json: dict[str, Any]) -> dict[str, Any]:
    results = details_json.get("results")
    if isinstance(results, list) and results:
        head = results[0]
        return head if isinstance(head, dict) else details_json
    if isinstance(results, dict):
        return results
    return details_json


def fetch_score(product_id: int) -> Optional[Any]:
    details_json = get_card_details(product_id)
    if not details_json:
        return None
    details = unwrap_details(details_json)
    if not isinstance(details, dict) or not details:
        return None
    return details.get("score")


def update_scores(cards_col: Any, tcg_player_ids: list[int], workers: int) -> tuple[int, int, int]:
    updated_docs = 0
    touched_ids = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(fetch_score, pid): pid for pid in tcg_player_ids}

        for future in as_completed(future_map):
            product_id = future_map[future]
            try:
                score = future.result()
            except Exception as exc:
                failures += 1
                print(f"[UPDATE-SCORE] tcgPlayerId={product_id} error: {exc}")
                continue

            if score is None:
                continue

            # Base phase: write API score to all rows sharing tcgPlayerId.
            result = cards_col.update_many(
                {"tcgPlayerId": product_id},
                {
                    "$set": {
                        "score": score,
                        "updatedAt": datetime.now(UTC),
                    }
                },
            )
            touched_ids += 1
            updated_docs += int(result.modified_count or 0)

    return updated_docs, touched_ids, failures


def build_related_score_indexes(cards_col: Any) -> tuple[dict[int, float], dict[int, float]]:
    by_card_market_id: dict[int, float] = {}
    by_card_trader_id: dict[int, float] = {}

    source_cursor = cards_col.find(
        {
            "type": "Cards",
            "tcgPlayerId": {"$type": "number"},
        },
        {
            "score": 1,
            "cardMarketId": 1,
            "cardMarketIds": 1,
            "cardTraderId": 1,
        },
    )

    for source in source_cursor:
        score = as_float(source.get("score"))
        if score is None:
            continue

        cmids = normalize_id_list(source.get("cardMarketIds"), source.get("cardMarketId"))
        for cmid in cmids:
            if cmid not in by_card_market_id:
                by_card_market_id[cmid] = score

        card_trader_id = positive_int(source.get("cardTraderId"))
        if card_trader_id is not None and card_trader_id not in by_card_trader_id:
            by_card_trader_id[card_trader_id] = score

    return by_card_market_id, by_card_trader_id


def sync_related_scores(cards_col: Any) -> dict[str, int]:
    stats = {
        "processed": 0,
        "updated": 0,
        "matchedByCardMarketIds": 0,
        "matchedByCardTraderId": 0,
        "fallbackToZero": 0,
    }

    by_cmid, by_ctid = build_related_score_indexes(cards_col)

    target_cursor = cards_col.find(
        {
            "type": "Cards",
            "$or": [
                {"tcgPlayerId": {"$exists": False}},
                {"tcgPlayerId": None},
                {"tcgPlayerId": ""},
                {"tcgPlayerId": 0},
            ],
        },
        {
            "_id": 1,
            "score": 1,
            "cardMarketId": 1,
            "cardMarketIds": 1,
            "cardTraderId": 1,
        },
    )

    for card in target_cursor:
        stats["processed"] += 1

        resolved_score: float = 0.0
        match_type = "fallback"

        cmids = normalize_id_list(card.get("cardMarketIds"), card.get("cardMarketId"))
        for cmid in cmids:
            if cmid in by_cmid:
                resolved_score = by_cmid[cmid]
                match_type = "cardMarketIds"
                break

        if match_type == "fallback":
            card_trader_id = positive_int(card.get("cardTraderId"))
            if card_trader_id is not None and card_trader_id in by_ctid:
                resolved_score = by_ctid[card_trader_id]
                match_type = "cardTraderId"

        if score_matches(card.get("score"), resolved_score):
            continue

        cards_col.update_one(
            {"_id": card["_id"]},
            {
                "$set": {
                    "score": resolved_score,
                    "updatedAt": datetime.now(UTC),
                }
            },
        )
        stats["updated"] += 1

        if match_type == "cardMarketIds":
            stats["matchedByCardMarketIds"] += 1
        elif match_type == "cardTraderId":
            stats["matchedByCardTraderId"] += 1
        else:
            stats["fallbackToZero"] += 1

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Update only score in Cards from TCGPlayer details API")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers for details API calls")
    args = parser.parse_args()
    game = os.getenv("GAME") or "N/A"

    try:
        require_env(["MONGODB_URI", "MONGODB_DB", "PRIMARY_URL_TEMPLATE"])

        mongo_uri = os.environ["MONGODB_URI"]
        mongodb_db = os.environ["MONGODB_DB"]

        client = MongoClient(mongo_uri)
        db = client[mongodb_db]
        cards_col = db[CARDS_COLL]

        tcg_player_ids = sorted(
            {
                int(value)
                for value in cards_col.distinct("tcgPlayerId", {"tcgPlayerId": {"$type": "number"}})
                if as_int(value) is not None
            }
        )

        print(f"[UPDATE-SCORE] tcgPlayerIds discovered: {len(tcg_player_ids)}")

        updated_docs, touched_ids, failures = update_scores(cards_col, tcg_player_ids, workers=args.workers)

        print(
            "[UPDATE-SCORE] API phase completed "
            f"| tcgPlayerIds_with_score={touched_ids} "
            f"| modified_docs={updated_docs} "
            f"| failures={failures}"
        )

        related_stats = sync_related_scores(cards_col)

        summary = (
            "[UPDATE-SCORE] completed "
            f"| api_modified_docs={updated_docs} "
            f"| related_processed={related_stats['processed']} "
            f"| related_updated={related_stats['updated']} "
            f"| related_by_cardMarketIds={related_stats['matchedByCardMarketIds']} "
            f"| related_by_cardTraderId={related_stats['matchedByCardTraderId']} "
            f"| related_fallback_to_zero={related_stats['fallbackToZero']} "
            f"| failures={failures}"
        )
        print(summary)
        send_email("✅ [5/5][WORKFLOW] Update Score: " + game, summary)

        client.close()
    except Exception:
        send_email("🚫 [5/5][WORKFLOW] Update Score: " + game, traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
