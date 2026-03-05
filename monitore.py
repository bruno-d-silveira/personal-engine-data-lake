# ================================================================
#  BRAIN — Human Work Activity Monitor
#  Flywheel: Watcher → Bus → Worker → Enricher → Lake → Pattern
# ================================================================

import os
import time
import shutil
import hashlib
import logging
from datetime import datetime, timezone

import storage
from logger       import log_evento
from event_bus    import EventBus
from event_contract import EventType
from data_record  import DataRecord
from watcher2     import monitorar
from workers      import WorkerPool
from enricher     import enriquecer
from session_tracker import SessionTracker
from data_lake    import ingerir, atualizar_resumo_diario
from metrics_module import MetricsCollector
from monitor2     import Dashboard

# ──────────────────────────────────────────
# BOOT
# ──────────────────────────────────────────
logging.disable(logging.CRITICAL)
storage.inicializar()

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
PASTAS = [
    r"C:\temp",
    r"C:\Users\Bruno\Downloads",
    r"C:\Users\Bruno\Desktop",
]

DEBOUNCE_SEGUNDOS  = 2.0
POLL_INTERVALO     = 0.8

# ──────────────────────────────────────────
# ESTADO
# ──────────────────────────────────────────
stats          = {"detectados": 0, "movidos": 0, "ignorados": 0}
debounce_cache : dict[str, float] = {}
session        = SessionTracker()

# ──────────────────────────────────────────
# HASH
# ──────────────────────────────────────────
def calcular_hash(caminho: str) -> str | None:
    h = hashlib.md5()
    try:
        with open(caminho, "rb") as f:
            for bloco in iter(lambda: f.read(65536), b""):
                h.update(bloco)
        return h.hexdigest()
    except OSError:
        return None

# ──────────────────────────────────────────
# DEBOUNCE
# ──────────────────────────────────────────
def dentro_do_debounce(chave: str) -> bool:
    agora  = time.time()
    ultima = debounce_cache.get(chave, 0)
    if agora - ultima < DEBOUNCE_SEGUNDOS:
        return True
    debounce_cache[chave] = agora
    return False

# ──────────────────────────────────────────
# PIPELINE DE PROCESSAMENTO
# O coração do sistema: cada record percorre
# todas as etapas até chegar ao data lake.
# ──────────────────────────────────────────
def pipeline(record: DataRecord):
    global stats

    payload  = record.payload
    arquivo  = payload.get("arquivo", "")
    caminho  = payload.get("caminho", "")
    tipo     = record.event_type
    tipo_str = tipo.value

    # ── ETAPA 1: Enriquecimento semântico
    evento_rico = enriquecer(payload, tipo_str)

    # ── ETAPA 2: Deletados não têm arquivo para hash
    #             mas têm valor analítico — vão pro lake
    if tipo == EventType.FILE_DELETED:
        session_id = session.registrar(evento_rico)
        evento_rico["session_id"] = session_id
        ingerir(evento_rico)
        atualizar_resumo_diario(evento_rico)
        stats["movidos"] += 1
        log_evento(
            "FILE_DELETED",
            arquivo=arquivo,
            extensao=payload.get("extensao", ""),
            intent=evento_rico["intent"],
            session=session_id,
        )
        return

    # ── ETAPA 3: Aguarda o arquivo estar estável no disco
    for _ in range(15):
        if os.path.isfile(caminho):
            break
        time.sleep(0.05)

    if not os.path.isfile(caminho):
        stats["ignorados"] += 1
        return

    # ── ETAPA 4: Deduplicação por hash
    file_hash = calcular_hash(caminho)
    if file_hash is None:
        stats["ignorados"] += 1
        return

    if storage.hash_existe(file_hash):
        # É uma cópia detectada — tem valor analítico diferente
        evento_rico["event_type"]    = "FILE_COPIED"
        evento_rico["hash_prefixo"]  = file_hash[:12]
        session_id = session.registrar(evento_rico)
        evento_rico["session_id"]    = session_id
        ingerir(evento_rico)
        atualizar_resumo_diario(evento_rico)
        stats["ignorados"] += 1
        log_evento(
            "FILE_COPIED",
            arquivo=arquivo,
            extensao=payload.get("extensao", ""),
            hash=file_hash[:8],
            session=session_id,
        )
        return

    # ── ETAPA 5: Arquivo novo e único — persiste no lake
    payload["hash"]          = file_hash
    evento_rico["hash_prefixo"] = file_hash[:12]
    evento_rico["tamanho_bytes"] = os.path.getsize(caminho)

    session_id               = session.registrar(evento_rico)
    evento_rico["session_id"] = session_id

    # ── ETAPA 6: Commit no storage de hashes + data lake
    storage.salvar_hash(file_hash, caminho)
    ingerir(evento_rico)
    atualizar_resumo_diario(evento_rico)

    stats["movidos"] += 1
    log_evento(
        tipo_str,
        arquivo=arquivo,
        extensao=payload.get("extensao", ""),
        intent=evento_rico["intent"],
        periodo=evento_rico["periodo_do_dia"],
        weight=evento_rico["action_weight"],
        session=session_id,
    )

# ──────────────────────────────────────────
# INICIALIZAÇÃO
# ──────────────────────────────────────────
bus       = EventBus()
pool      = WorkerPool(n_workers=4, handler=pipeline, bus=bus)
dashboard = Dashboard(stats, pool.fila)
metrics   = MetricsCollector(pool.fila, stats)

bus.subscribe(
    EventType.QUEUE_HIGH,
    lambda d: print(f"\n  ⚠  BACKPRESSURE: {d['fila']} jobs — workers sobrecarregados\n")
)

pool.iniciar()

# ──────────────────────────────────────────
# BOOT SCREEN
# ──────────────────────────────────────────
print("\n" + "─" * 60)
print("  ◉  BRAIN  —  Human Activity Monitor")
print("  ◎  Flywheel Architecture  v2.0")
print("─" * 60)
print("  Pipeline: Watcher → Bus → Worker → Enricher → Lake")
print("─" * 60)
for p in PASTAS:
    print(f"  → {p}")
print("─" * 60 + "\n")

# ──────────────────────────────────────────
# LOOP PRINCIPAL
# ──────────────────────────────────────────
estado = None

try:
    while True:
        eventos, estado = monitorar(PASTAS, estado)

        for evento in eventos:
            payload = evento.payload
            arquivo = payload.get("arquivo", "")
            tipo    = evento.type
            chave   = f"{tipo.value}::{payload.get('caminho', arquivo)}"

            if not arquivo:
                continue

            if dentro_do_debounce(chave):
                continue

            if tipo not in (
                EventType.FILE_CREATED,
                EventType.FILE_MODIFIED,
                EventType.FILE_DELETED,
            ):
                continue

            stats["detectados"] += 1
            record = DataRecord(payload, event_type=tipo)
            pool.adicionar(record)
            dashboard.mostrar()

        metrics.coletar()
        time.sleep(POLL_INTERVALO)

except KeyboardInterrupt:
    session.fechar()
    m = metrics.coletar()
    print(f"\n\n{'─' * 60}")
    print(f"  ◉  BRAIN OFFLINE — Sessão encerrada")
    print(f"{'─' * 60}")
    print(f"  Detectados    : {m['detectados']}")
    print(f"  Processados   : {m['processados']}")
    print(f"  Ignorados     : {m['ignorados']}")
    print(f"  Fila final    : {m['fila']}")
    print(f"{'─' * 60}\n")
    print("\n" + "─" * 60)
print("  📊 GERANDO RELATÓRIO DE PORCENTAGENS...")
df_final = carregar_dados(hoje)
gerar_relatorio_porcentagens(df_final)
print("─" * 60)

    # ─── RELATÓRIO FINAL AUTOMÁTICO ───
from reporter import carregar_dados, gerar_relatorio_completo
hoje = datetime.now(timezone.utc).date().isoformat()
df_final = carregar_dados(hoje)
print("\n" + "─" * 60)
print("  📊 GERANDO RELATÓRIO FINAL DO DIA...")
gerar_relatorio_completo(df_final)
print("─" * 60)

pool.parar()
