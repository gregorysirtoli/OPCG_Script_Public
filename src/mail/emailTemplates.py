"""
Template HTML delle mail accodate in Mail. Stanno nel repo del sito (OPCG, public/email/<nome>.html,
gli stessi di lib/emailTemplates.ts) e si scaricano dal sito a ogni esecuzione: la grafica si cambia
li', senza toccare gli script.

Segnaposto {{chiave}}: i valori vengono inseriti con escape HTML, tranne le chiavi che finiscono in
"Html" (frammenti costruiti dagli script, che fanno loro l'escape dei dati).
"""
from __future__ import annotations

import html
import os
import re
import urllib.request
from datetime import datetime
from typing import Any, Dict

SITE_URL = "https://redline.cards".rstrip("/")
# Da dove scaricare i template; in locale si puo' puntare alla cartella del repo del sito con
# file:///C:/.../OPCG/public/email/
EMAIL_TEMPLATES_URL = f"{SITE_URL}/email/"

_PLACEHOLDER = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")
_cache: Dict[str, str] = {}

# Colori della palette dei template (magic-link.html)
TEXT = "#222222"
MUTED = "#6c757d"
BORDER = "#e6e6e6"
LINK = "#b20710"
POSITIVE = "#198754"
NEGATIVE = "#dc3545"


def load_email_template(name: str) -> str:
    """Scarica public/email/<name>.html dal sito. Solleva un'eccezione se non e' raggiungibile:
    gli script lo caricano prima di accodare, cosi' una mail non parte mai senza grafica."""
    if name not in _cache:
        url = EMAIL_TEMPLATES_URL.rstrip("/") + f"/{name}.html"
        request = urllib.request.Request(url, headers={"User-Agent": "RedLine-Mailer/1.0"})
        with urllib.request.urlopen(request, timeout=20) as response:
            _cache[name] = response.read().decode("utf-8")
    return _cache[name]


def render_email_template(name: str, values: Dict[str, Any]) -> str:
    template = load_email_template(name)

    def replace(match: re.Match) -> str:
        key = match.group(1)
        if key not in values:
            print(f"[WARN] Email template {name!r}: no value for {{{{{key}}}}}")
        value = "" if values.get(key) is None else str(values[key])
        return value if key.endswith("Html") else html.escape(value, quote=True)

    return _PLACEHOLDER.sub(replace, template)


def common_values(now_utc: datetime) -> Dict[str, str]:
    """Valori presenti in tutti i template."""
    return {
        "siteUrl": SITE_URL,
        "year": str(now_utc.year),
        "sentAt": now_utc.strftime("%Y-%m-%d %H:%M UTC"),
    }
