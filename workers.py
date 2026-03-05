import threading
import queue
from event_bus import EventBus
from event_contract import EventType

QUEUE_HIGH_WATERMARK = 50

class WorkerPool:
    """
    Pool de workers que consome DataRecords da fila
    e executa o handler em paralelo.
    """
    def __init__(self, n_workers: int, handler, bus: EventBus):
        self.fila      = queue.Queue()
        self.handler   = handler
        self.bus       = bus
        self.n_workers = n_workers
        self._threads  = []
        self._rodando  = False

    def iniciar(self):
        self._rodando = True
        for _ in range(self.n_workers):
            t = threading.Thread(target=self._loop, daemon=True)
            t.start()
            self._threads.append(t)

    def adicionar(self, record):
        self.fila.put(record)
        if self.fila.qsize() >= QUEUE_HIGH_WATERMARK:
            self.bus.publish(EventType.QUEUE_HIGH, {"fila": self.fila.qsize()})
        else:
            self.bus.publish(EventType.QUEUE_LOW,  {"fila": self.fila.qsize()})

    def parar(self):
        self._rodando = False
        for _ in self._threads:
            self.fila.put(None)  # Poison pill
        for t in self._threads:
            t.join(timeout=2)

    def _loop(self):
        while self._rodando:
            try:
                record = self.fila.get(timeout=1)
                if record is None:
                    break
                self.handler(record)
                self.fila.task_done()
            except queue.Empty:
                continue
