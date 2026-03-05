"""
reporter.py — Análise e relatórios do Data Lake.

Processa todos os CSVs em LOG/raw/ e responde perguntas específicas:
- Porcentagens de arquivos usados por extensão durante o trabalho.
- Tempo inativo durante uma sessão de trabalho.
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from paths import RAW_DIR, LOG_DIR

# Categorias de extensões e Ações (mesmas do código anterior)
CATEGORIAS = {
    "textos": [".txt", ".docx", ".doc", ".odt", ".md", ".rtf"],
    "musicas": [".mp3", ".wav", ".m4a", ".flac", ".aac"],
    "imagens": [".jpg", ".jpeg", ".png", ".gif", ".bmp"],
    "planilhas": [".xlsx", ".xls", ".csv"],
    "pdfs": [".pdf"],
    "codigo": [".py", ".js", ".ts", ".html", ".css", ".java", ".cpp"],
    "comprimidos": [".zip", ".rar", ".7z"],
    "outros": [],  # Default para não listados
}

ACOES = {
    "FILE_CREATED": "criado",
    "FILE_MODIFIED": "modificado/aberto",
    "FILE_DELETED": "deletado",
    "FILE_COPIED": "copiado",
}


def carregar_dados(data_str: str = None) -> pd.DataFrame:
    """Lê todos os CSVs em LOG/raw/."""
    if data_str:
        ano, mes, dia = data_str.split("-")
        data_path = Path(RAW_DIR) / ano / mes / dia
    else:
        hoje = datetime.now(timezone.utc).date().isoformat()
        ano, mes, dia = hoje.split("-")
        data_path = Path(RAW_DIR) / ano / mes / dia

    if not data_path.exists():
        print(f"Nenhum dado encontrado para {data_str or 'hoje'}.")
        return pd.DataFrame()

    arquivos_csv = list(data_path.glob("*.csv"))
    if not arquivos_csv:
        print(f"Nenhum CSV encontrado em {data_path}.")
        return pd.DataFrame()

    dfs = []
    for csv_file in arquivos_csv:
        df = pd.read_csv(csv_file, encoding="utf-8")
        dfs.append(df)

    df_full = pd.concat(dfs, ignore_index=True)
    df_full = df_full.dropna(subset=["arquivo", "extensao"])
    df_full = df_full.drop_duplicates(subset=["arquivo", "timestamp", "hash_prefixo"], keep="first")
    df_full["extensao"] = df_full["extensao"].str.lower()  # Normaliza

    return df_full


def categorizar_extensao(ext: str) -> str:
    """Atribui categoria à extensão."""
    for cat, exts in CATEGORIAS.items():
        if ext in exts:
            return cat
    return "outros"


def calcular_porcentagens(df: pd.DataFrame):
    """Calcula a porcentagem de arquivos usados por extensão e tempo inativo."""
    total_eventos = len(df)
    
    # Tempo monitorado em segundos
    inicio_sessao = df["timestamp"].min()
    fim_sessao = df["timestamp"].max()
    tempo_total = (pd.to_datetime(fim_sessao) - pd.to_datetime(inicio_sessao)).total_seconds()

    # Contagem de tempo que não houve uso de arquivos (inatividade)
    tempo_inativo = max(0, tempo_total - df["cadencia_segundos"].sum())

    # Categorização para porcentagens
    df["categoria"] = df["extensao"].apply(categorizar_extensao)  # Adiciona coluna de categoria
    usadas = df["categoria"].value_counts()  # Contagem de arquivos usados
    porcentagens = (usadas / total_eventos) * 100  # Porcentagem por extensão

    # Adiciona tempo inativo como porcentagem do total
    porcentagem_inativa = (tempo_inativo / tempo_total) * 100 if tempo_total > 0 else 0.0
    
    return porcentagens, porcentagem_inativa, tempo_total


def gerar_relatorio_porcentagens(df: pd.DataFrame):
    """Gera relatório de porcentagens de arquivos usados por extensão e tempo inativo."""
    if df.empty:
        print("Nenhum dado para analisar.")
        return

    porcentagens, porcentagem_inativa, tempo_total = calcular_porcentagens(df)

    print("\n=== RELATÓRIO DE PORCENTAGENS DE USO DE ARQUIVOS ===")
    for categoria, porcentagem in porcentagens.items():
        print(f"{categoria.capitalize()}: {porcentagem:.2f}%")
    
    print(f"Tempo inativo: {porcentagem_inativa:.2f}%")
    print(f"Tempo total monitorado: {tempo_total:.2f} segundos")

    # Salva em CSV
    relatorio_path = Path(LOG_DIR) / "reports" / "relatorio_porcentagens.csv"
    with open(relatorio_path, "w", encoding="utf-8") as f:
        f.write("Categoria,Porcentagem\n")
        for categoria, porcentagem in porcentagens.items():
            f.write(f"{categoria},{porcentagem:.2f}\n")
        f.write(f"Inativo,{porcentagem_inativa:.2f}\n")

    print(f"Relatório salvo em: {relatorio_path}")


if __name__ == "__main__":
    data = None
    for arg in sys.argv[1:]:
        if arg.startswith("--data="):
            data = arg.split("=")[1]

    df = carregar_dados(data)
    gerar_relatorio_porcentagens(df)
