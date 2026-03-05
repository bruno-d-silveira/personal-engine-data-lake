from collections import defaultdict
from threading import Lock

class EventBus:
    """
    Barramento central de eventos.
    Publish/Subscribe desacoplado entre produtores e consumidores.
    """
    def __init__(self):
        self._subscribers = defaultdict(list)
        self._lock = Lock()

    def subscribe(self, event_type, callback):
        with self._lock:
            self._subscribers[event_type].append(callback)

    def publish(self, event_type, data=None):
        with self._lock:
            callbacks = list(self._subscribers.get(event_type, []))
        for cb in callbacks:
            try:
                cb(data)
            except Exception as e:
                pass  # Nunca deixa o bus travar por erro de subscriber
