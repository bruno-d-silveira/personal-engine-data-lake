import queue

class Dashboard:
    """Exibe status do sistema no terminal de forma limpa."""
    def __init__(self, stats: dict, fila: queue.Queue):
        self.stats = stats
        self.fila  = fila

    def mostrar(self):
        s = self.stats
        print(
            f"  ┤ DETECTADOS:{s['detectados']}"
            f"  PROCESSADOS:{s['movidos']}"
            f"  IGNORADOS:{s['ignorados']}"
            f"  FILA:{self.fila.qsize()} ├",
            end="\r"
        )
