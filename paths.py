"""
paths.py — Âncora de caminhos do sistema.

Todos os módulos importam daqui.
LOG/ sempre fica ao lado do script principal,
independente de onde o terminal está rodando.
"""

import os

# Raiz absoluta = pasta onde o monitore.py está
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Pasta principal de saída
LOG_DIR  = os.path.join(BASE_DIR, "LOG")

# Sub-pastas
RAW_DIR      = os.path.join(LOG_DIR, "raw")
SESSIONS_DIR = os.path.join(LOG_DIR, "sessions")
SUMMARY_DIR  = os.path.join(LOG_DIR, "summary")

# Arquivos fixos
SESSIONS_CSV  = os.path.join(SESSIONS_DIR, "sessions.csv")
HASHES_FILE   = os.path.join(LOG_DIR, "hashes.json")
BRAIN_LOG     = os.path.join(LOG_DIR, "brain.log")

def garantir_estrutura():
    """Cria toda a estrutura de pastas na primeira execução."""
    for pasta in [LOG_DIR, RAW_DIR, SESSIONS_DIR, SUMMARY_DIR]:
        os.makedirs(pasta, exist_ok=True)
