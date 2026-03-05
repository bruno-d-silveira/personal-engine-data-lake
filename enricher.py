"""
Enricher — transforma um evento bruto em um registro rico.

Adiciona contexto que o watcher não tem:
  - intenção humana inferida
  - período do dia
  - cadência (tempo desde o último evento)
  - risco de dado sensível
  - peso da ação
  - session_id
"""

import os
import time
from datetime import datetime, timezone
from event_contract import INTENT_MAP, ACTION_WEIGHT

_ultimo_evento_ts: float = 0.0

PERIODOS = {
    range(0,  6):  "madrugada",
    range(6,  9):  "manha_cedo",
    range(9,  12): "manha",
    range(12, 14): "almoco",
    range(14, 18): "tarde",
    range(18, 22): "noite",
    range(22, 24): "noite_tarde",
}

EXTENSOES_SENSIVEIS = {
    ".key", ".pem", ".env", ".pfx", ".p12", ".kdbx", ".wallet"
}

def _periodo_do_dia(hora: int) -> str:
    for r, nome in PERIODOS.items():
        if hora in r:
            return nome
    return "desconhecido"

def _cadencia(agora: float) -> float:
    global _ultimo_evento_ts
    delta = round(agora - _ultimo_evento_ts, 2) if _ultimo_evento_ts else 0.0
    _ultimo_evento_ts = agora
    return delta

def enriquecer(payload: dict, event_type_str: str) -> dict:
    agora    = datetime.now(timezone.utc)
    ts_unix  = time.time()
    arquivo  = payload.get("arquivo", "")
    extensao = payload.get("extensao", "").lower()

    intent   = INTENT_MAP.get(extensao, "unknown")
    peso     = ACTION_WEIGHT.get(event_type_str, 0.5)
    cadencia = _cadencia(ts_unix)
    hora     = agora.hour
    periodo  = _periodo_do_dia(hora)
    sensivel = extensao in EXTENSOES_SENSIVEIS

    return {
        # Identidade do evento
        "timestamp":        agora.isoformat(),
        "event_type":       event_type_str,
        "arquivo":          arquivo,
        "extensao":         extensao,
        "tamanho_bytes":    payload.get("tamanho", 0),
        "hash_prefixo":     payload.get("hash", "")[:12] or None,

        # Contexto humano inferido
        "intent":           intent,
        "periodo_do_dia":   periodo,
        "hora":             hora,
        "dia_semana":       agora.strftime("%A").lower(),
        "dia_semana_num":   agora.weekday(),

        # Métricas comportamentais
        "action_weight":    peso,
        "cadencia_segundos": cadencia,  # tempo desde o evento anterior
        "dado_sensivel":    sensivel,

        # Origem
        "pasta":            payload.get("pasta", ""),
        "caminho":          payload.get("caminho", ""),

        # Preenchido pelo SessionTracker depois
        "session_id":       None,
    }
