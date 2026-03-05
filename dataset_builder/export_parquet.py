import os
import json
from datetime import datetime, timezone

LAKE_DIR  = "lake/parquet"
LOG_FILE  = "lake/events_log.jsonl"

os.makedirs(LAKE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def salvar_parquet(records: list[dict]):
    """
    Persiste registros no Data Lake.
    Usa JSONL como formato intermediário (substituível por Parquet real).
    Cada linha é um evento imutável — estilo append-only log.
    """
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
