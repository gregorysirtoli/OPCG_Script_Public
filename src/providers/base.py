# src/providers/base.py
from __future__ import annotations
from typing import Optional, Protocol, Any, runtime_checkable

# Alias di tipo (più leggibili)
PriceTriple = tuple[Optional[float], Optional[int], Optional[int]] # (price, sellers, listings)
PriceDetails = dict[str, Optional[float]] # mappa di dettagli prezzo/grading
UpdatesMap = dict[str, Any] # aggiornamenti parziali per la card (es. externalId/Uri)

@runtime_checkable
class PriceProvider(Protocol):
    """
    Interfaccia che ogni provider deve implementare.
    - name: nome leggibile del provider (per logging)
    - fetch_primary_price: fonte "primaria" -> (price, sellers, listings)
    - fetch_secondary_breakdown: fonte "secondaria" -> (grades)
    """
    name: str

    def fetch_primary_price(self, item_id: Optional[int]) -> PriceTriple:
        """Ritorna (price, sellers, listings). Se non disponibili, usa None."""
        ...

    def fetch_secondary_breakdown(self, card_info: dict[str, Any]) -> tuple[PriceDetails, UpdatesMap]:
        """
        Ritorna (price_details_map, updates_map).
        - price_details_map: es. {"priceUngraded": 1.23, "priceGrade9": 4.56, ...}
        - updates_map: es. {"externalId": 12345, "externalUri": "TBD"}
        """
        ...
