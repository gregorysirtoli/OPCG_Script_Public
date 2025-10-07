# src/providers/mock.py
from __future__ import annotations
from typing import Optional, Any
from .base import PriceProvider, PriceTriple, PriceDetails, UpdatesMap

class MockProvider:
    name = "mock"

    def fetch_primary_price(self, item_id: Optional[int]) -> PriceTriple:
        return 12.34, 5, 18

    def fetch_secondary_breakdown(self, card_info: dict[str, Any]) -> tuple[PriceDetails, UpdatesMap]:
        details: PriceDetails = {
            "priceUngraded": 1.23,
            "priceGrade7": 2.34,
            "priceGrade8": 3.45,
            "priceGrade9": 4.56,
            "priceGrade95": 5.67,
            "grade10_a": 6.78,
            "grade10_b": 7.89,
            "priceReference": 8.90,
        }
        updates: UpdatesMap = {"externalUri": "TBD", "externalId": None}
        return details, updates

PROVIDERS = [MockProvider()]