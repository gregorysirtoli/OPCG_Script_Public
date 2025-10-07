# Prices Ingestor (Framework)

Framework di ingestion dati con **provider plug-in** caricati a runtime tramite variabili d'ambiente.

- Nessun riferimento a brand o endpoint: i provider reali vivono in repository/private packages separati.
- Esecuzione orchestrata via **GitHub Actions** con sharding e rate limiting opzionali.

## Esecuzione locale

```bash
python -m pip install -r requirements.txt
export MONGODB_URI="..."
export MONGODB_DB="..."
# Carica provider mock di esempio
python -m src.ingest.main --shard-index 0 --shard-total 1
```
