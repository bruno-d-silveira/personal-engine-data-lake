from enum import Enum

class EventType(Enum):
    FILE_CREATED  = "FILE_CREATED"
    FILE_DELETED  = "FILE_DELETED"
    FILE_MODIFIED = "FILE_MODIFIED"
    FILE_COPIED   = "FILE_COPIED"
    QUEUE_HIGH    = "QUEUE_HIGH"
    QUEUE_LOW     = "QUEUE_LOW"

# Categorias de intenção humana inferidas a partir do tipo de arquivo
INTENT_MAP = {
    # Documentos e escrita
    ".txt":   "writing",
    ".docx":  "writing",
    ".doc":   "writing",
    ".odt":   "writing",
    ".md":    "writing",
    ".rtf":   "writing",
    # Análise e dados
    ".xlsx":  "analysis",
    ".xls":   "analysis",
    ".csv":   "analysis",
    ".json":  "analysis",
    ".xml":   "analysis",
    # Comunicação
    ".pdf":   "communication",
    ".pptx":  "communication",
    ".ppt":   "communication",
    # Código
    ".py":    "development",
    ".js":    "development",
    ".ts":    "development",
    ".html":  "development",
    ".css":   "development",
    ".java":  "development",
    ".cpp":   "development",
    # Mídia
    ".jpg":   "media_consumption",
    ".jpeg":  "media_consumption",
    ".png":   "media_consumption",
    ".gif":   "media_consumption",
    ".mp4":   "media_consumption",
    ".mp3":   "media_consumption",
    ".wav":   "media_consumption",
    # Sistema
    ".zip":   "file_management",
    ".rar":   "file_management",
    ".exe":   "system",
    ".dll":   "system",
    ".log":   "system",
}

# Peso de complexidade da ação (quanto "esforço" humano ela representa)
ACTION_WEIGHT = {
    "FILE_CREATED":  1.0,
    "FILE_MODIFIED": 0.8,
    "FILE_DELETED":  0.5,
    "FILE_COPIED":   0.6,
}
