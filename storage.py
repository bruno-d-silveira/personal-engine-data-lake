import json
import os
from threading import Lock
from paths import HASHES_FILE, garantir_estrutura

garantir_estrutura()

_lock  = Lock()
_cache = {}

def inicializar():
    global _cache
    if os.path.exists(HASHES_FILE):
        with open(HASHES_FILE, "r", encoding="utf-8") as f:
            _cache = json.load(f)

def hash_existe(h: str) -> bool:
    return h in _cache

def salvar_hash(h: str, caminho: str):
    with _lock:
        _cache[h] = caminho
        with open(HASHES_FILE, "w", encoding="utf-8") as f:
            json.dump(_cache, f, indent=2)
