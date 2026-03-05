"""
SessionTracker — sessões de trabalho humano persistidas em CSV.
"""

import os
import csv
import time
import uuid
from datetime import datetime, timezone
from threading import Lock
from event_contract import ACTION_WEIGHT
from paths import SESSIONS_CSV, garantir_estrutura

garantir_estrutura()

SESSION_GAP_SECONDS = 120   # 2 min sem evento = nova sessão

SESSION_COLUMNS = [
    "session_id",
    "started_at",
    "ended_at",
    "total_events",
    "dominant_intent",
    "complexity_score",
    "intents_breakdown",
    "extensions_used",
    "actions_breakdown",
]


def _salvar_sessao_csv(data: dict):
    novo = not os.path.exists(SESSIONS_CSV)
    with open(SESSIONS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SESSION_COLUMNS, extrasaction="ignore")
        if novo:
            writer.writeheader()
        # Converte dicts internos para string legível na célula
        row = dict(data)
        row["intents_breakdown"]  = str(data.get("intents_breakdown", {}))
        row["extensions_used"]    = str(data.get("extensions_used", {}))
        row["actions_breakdown"]  = str(data.get("actions_breakdown", {}))
        writer.writerow(row)


class WorkSession:
    def __init__(self):
        self.session_id   = str(uuid.uuid4())[:8]
        self.started_at   = datetime.now(timezone.utc).isoformat()
        self.last_seen    = time.time()
        self.events       = []
        self.intents      = {}
        self.extensions   = {}
        self.action_types = {}
        self.complexity   = 0.0

    def registrar(self, event_dict: dict):
        self.last_seen = time.time()
        self.events.append(event_dict)

        intent = event_dict.get("intent", "unknown")
        ext    = event_dict.get("extensao", "")
        action = event_dict.get("event_type", "")
        weight = float(ACTION_WEIGHT.get(action, 0.5))

        self.intents[intent]      = self.intents.get(intent, 0) + 1
        self.extensions[ext]      = self.extensions.get(ext, 0) + 1
        self.action_types[action] = self.action_types.get(action, 0) + 1
        self.complexity          += weight

    def expirou(self) -> bool:
        return (time.time() - self.last_seen) > SESSION_GAP_SECONDS

    def dominant_intent(self) -> str:
        return max(self.intents, key=self.intents.get) if self.intents else "idle"

    def consolidar(self) -> dict:
        return {
            "session_id":        self.session_id,
            "started_at":        self.started_at,
            "ended_at":          datetime.now(timezone.utc).isoformat(),
            "total_events":      len(self.events),
            "dominant_intent":   self.dominant_intent(),
            "complexity_score":  round(self.complexity, 2),
            "intents_breakdown": self.intents,
            "extensions_used":   self.extensions,
            "actions_breakdown": self.action_types,
        }


class SessionTracker:
    def __init__(self):
        self._lock    = Lock()
        self._session = WorkSession()

    def registrar(self, event_dict: dict) -> str:
        with self._lock:
            if self._session.expirou():
                self._fechar_sessao()
                self._session = WorkSession()
            self._session.registrar(event_dict)
            return self._session.session_id

    def _fechar_sessao(self):
        if not self._session.events:
            return
        _salvar_sessao_csv(self._session.consolidar())

    def fechar(self):
        with self._lock:
            self._fechar_sessao()
