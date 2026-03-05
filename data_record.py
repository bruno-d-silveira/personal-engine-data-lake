from datetime import datetime, timezone
from event_contract import EventType

class DataRecord:
    """
    Unidade atômica de dado no pipeline.
    Transporta o evento do watcher até o data lake.
    """
    def __init__(self, payload: dict, event_type: EventType = None):
        self.payload    = payload
        self.event_type = event_type
        self.timestamp  = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "timestamp":    self.timestamp,
            "event_type":   self.event_type.value if self.event_type else "UNKNOWN",
            "arquivo":      self.payload.get("arquivo", ""),
            "pasta":        self.payload.get("pasta", ""),
            "extensao":     self.payload.get("extensao", ""),
            "hash":         self.payload.get("hash", ""),
            "tamanho":      self.payload.get("tamanho", 0),
            "hour":         datetime.now(timezone.utc).hour,
            "weekday":      datetime.now(timezone.utc).weekday(),
        }
