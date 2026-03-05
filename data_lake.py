"""
DataLake — persistência particionada em CSV + JSON.

Estrutura:
  LOG/
  ├── raw/YYYY/MM/DD/HHh_intent.csv   ← eventos individuais
  ├── sessions/sessions.csv            ← sessões de trabalho
  └── summary/daily_YYYY-MM-DD.json   ← agregado diário
"""

import os
import csv
import json
from datetime import datetime, timezone
from threading import Lock
from paths import RAW_DIR, SUMMARY_DIR, garantir_estrutura

garantir_estrutura()

_lock = Lock()

# Colunas do CSV de eventos — ordem fixa para análise posterior
CSV_COLUMNS = [
    "timestamp",
    "event_type",
    "arquivo",
    "extensao",
    "tamanho_bytes",
    "hash_prefixo",
    "intent",
    "periodo_do_dia",
    "hora",
    "dia_semana",
    "dia_semana_num",
    "action_weight",
    "cadencia_segundos",
    "dado_sensivel",
    "session_id",
    "pasta",
    "caminho",
]


def _partition_csv_path(event_dict: dict) -> str:
    """
    Gera o caminho particionado do CSV.
    LOG/raw/2025/06/15/14h_writing.csv
    """
    ts     = event_dict.get("timestamp", datetime.now(timezone.utc).isoformat())
    dt     = datetime.fromisoformat(ts)
    intent = event_dict.get("intent", "unknown").replace(" ", "_")

    pasta = os.path.join(
        RAW_DIR,
        str(dt.year),
        f"{dt.month:02d}",
        f"{dt.day:02d}",
    )
    os.makedirs(pasta, exist_ok=True)
    return os.path.join(pasta, f"{dt.hour:02d}h_{intent}.csv")


def _daily_summary_path(dt: datetime) -> str:
    return os.path.join(SUMMARY_DIR, f"daily_{dt.date().isoformat()}.json")


def ingerir(event_dict: dict):
    """
    Persiste evento no CSV particionado.
    Cria o header automaticamente se o arquivo for novo.
    """
    with _lock:
        csv_path   = _partition_csv_path(event_dict)
        novo       = not os.path.exists(csv_path)

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=CSV_COLUMNS,
                extrasaction="ignore",   # ignora campos extras silenciosamente
            )
            if novo:
                writer.writeheader()   # header só na primeira linha
            writer.writerow({col: event_dict.get(col, "") for col in CSV_COLUMNS})


def atualizar_resumo_diario(event_dict: dict):
    """
    Resumo diário em JSON — agregado incremental.
    Atualizado a cada evento processado.
    """
    ts  = event_dict.get("timestamp", datetime.now(timezone.utc).isoformat())
    dt  = datetime.fromisoformat(ts)
    path = _daily_summary_path(dt)

    with _lock:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                summary = json.load(f)
        else:
            summary = {
                "data":              dt.date().isoformat(),
                "total_eventos":     0,
                "complexidade_total": 0.0,
                "intents":           {},
                "acoes":             {},
                "extensoes":         {},
                "horas_ativas":      {},
                "dados_sensiveis":   0,
                "sessoes_unicas":    [],
            }

        intent = event_dict.get("intent", "unknown")
        acao   = event_dict.get("event_type", "")
        ext    = event_dict.get("extensao", "")
        hora   = str(event_dict.get("hora", "?"))
        peso   = float(event_dict.get("action_weight", 0.5))
        sid    = event_dict.get("session_id", "")

        summary["total_eventos"]      += 1
        summary["complexidade_total"]  = round(
            summary["complexidade_total"] + peso, 2
        )
        summary["intents"][intent]     = summary["intents"].get(intent, 0) + 1
        summary["acoes"][acao]         = summary["acoes"].get(acao, 0) + 1
        summary["extensoes"][ext]      = summary["extensoes"].get(ext, 0) + 1
        summary["horas_ativas"][hora]  = summary["horas_ativas"].get(hora, 0) + 1

        if event_dict.get("dado_sensivel"):
            summary["dados_sensiveis"] += 1

        if sid and sid not in summary["sessoes_unicas"]:
            summary["sessoes_unicas"].append(sid)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
