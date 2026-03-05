import queue

class MetricsCollector:
    """Coleta e expõe métricas do sistema em tempo real."""
    def __init__(self, fila: queue.Queue, stats: dict):
        self.fila  = fila
        self.stats = stats

    def coletar(self) -> dict:
        return {
            "detectados": self.stats.get("detectados", 0),
            "processados": self.stats.get("movidos", 0),
            "ignorados":  self.stats.get("ignorados", 0),
            "fila":       self.fila.qsize(),
        }
