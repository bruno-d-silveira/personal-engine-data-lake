import os
import hashlib
import time
from event_contract import EventType

class FileEvent:
    def __init__(self, event_type: EventType, payload: dict):
        self.type    = event_type
        self.payload = payload

def _snapshot(pastas: list) -> dict:
    """Captura estado atual do filesystem."""
    estado = {}
    for pasta in pastas:
        if not os.path.isdir(pasta):
            continue
        for arquivo in os.listdir(pasta):
            caminho = os.path.join(pasta, arquivo)
            if os.path.isfile(caminho):
                try:
                    stat = os.stat(caminho)
                    estado[caminho] = {
                        "mtime": stat.st_mtime,
                        "size":  stat.st_size,
                    }
                except OSError:
                    continue
    return estado

def _hash_rapido(caminho: str) -> str | None:
    """Hash dos primeiros 64KB — rápido e suficiente para dedup."""
    try:
        with open(caminho, "rb") as f:
            return hashlib.md5(f.read(65536)).hexdigest()
    except OSError:
        return None

def _extensao(arquivo: str) -> str:
    _, ext = os.path.splitext(arquivo)
    return ext.lower() if ext else "(sem extensão)"

def _payload(pasta: str, arquivo: str, caminho: str, hash_val: str = None) -> dict:
    try:
        tamanho = os.path.getsize(caminho) if os.path.exists(caminho) else 0
    except OSError:
        tamanho = 0
    return {
        "pasta":    pasta,
        "arquivo":  arquivo,
        "extensao": _extensao(arquivo),
        "caminho":  caminho,
        "hash":     hash_val or "",
        "tamanho":  tamanho,
    }

def monitorar(pastas: list, estado_anterior: dict | None):
    """
    Compara snapshot anterior com o atual.
    Retorna lista de FileEvent e o novo estado.
    """
    if estado_anterior is None:
        return [], _snapshot(pastas)

    estado_atual = _snapshot(pastas)
    eventos = []

    anteriores = set(estado_anterior.keys())
    atuais     = set(estado_atual.keys())

    # CRIADOS
    for caminho in (atuais - anteriores):
        pasta, arquivo = os.path.dirname(caminho), os.path.basename(caminho)
        h = _hash_rapido(caminho)
        eventos.append(FileEvent(EventType.FILE_CREATED, _payload(pasta, arquivo, caminho, h)))

    # DELETADOS
    for caminho in (anteriores - atuais):
        pasta, arquivo = os.path.dirname(caminho), os.path.basename(caminho)
        eventos.append(FileEvent(EventType.FILE_DELETED, {
            "pasta":    pasta,
            "arquivo":  arquivo,
            "extensao": _extensao(arquivo),
            "caminho":  caminho,
            "hash":     "",
            "tamanho":  0,
        }))

    # MODIFICADOS
    for caminho in (atuais & anteriores):
        ant = estado_anterior[caminho]
        atu = estado_atual[caminho]
        if ant["mtime"] != atu["mtime"] or ant["size"] != atu["size"]:
            pasta, arquivo = os.path.dirname(caminho), os.path.basename(caminho)
            h = _hash_rapido(caminho)
            eventos.append(FileEvent(EventType.FILE_MODIFIED, _payload(pasta, arquivo, caminho, h)))

    return eventos, estado_atual
