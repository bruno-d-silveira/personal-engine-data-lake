import os

# Mapa de extensão → pasta de destino no Data Lake
REGRAS = {
    ".txt":   "lake/documentos",
    ".docx":  "lake/documentos",
    ".pdf":   "lake/documentos",
    ".xlsx":  "lake/planilhas",
    ".csv":   "lake/planilhas",
    ".jpg":   "lake/imagens",
    ".jpeg":  "lake/imagens",
    ".png":   "lake/imagens",
    ".mp4":   "lake/videos",
    ".mp3":   "lake/audio",
    ".zip":   "lake/comprimidos",
    ".py":    "lake/codigo",
    ".js":    "lake/codigo",
}
DEFAULT = "lake/outros"

def decidir_destino(pasta: str, arquivo: str) -> str:
    _, ext = os.path.splitext(arquivo)
    return REGRAS.get(ext.lower(), DEFAULT)
