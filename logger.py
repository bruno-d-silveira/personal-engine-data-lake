import logging
import sys
from datetime import datetime, timezone
from paths import BRAIN_LOG, garantir_estrutura

garantir_estrutura()

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BRAIN_LOG, encoding="utf-8"),
    ]
)

_logger = logging.getLogger("brain")

ICONS = {
    "FILE_CREATED":  "✚",
    "FILE_DELETED":  "✖",
    "FILE_MODIFIED": "✎",
    "FILE_COPIED":   "⎘",
    "ignorado":      "◌",
    "duplicado":     "⊘",
    "erro":          "⚠",
}

def log_evento(tipo: str, **kwargs):
    icon = ICONS.get(tipo, "·")
    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    info = "  ".join(f"{k}={v}" for k, v in kwargs.items())
    _logger.info(f"  {icon}  [{ts}]  {tipo:<18} {info}")
