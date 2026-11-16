# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração comum do ZelaCidade
# MAGIC
# MAGIC Os outros notebooks rodam este com `%run ./_config`. Aqui ficam os nomes do catálogo, do Volume e as regras
# MAGIC de referência (categorias e área piloto), iguais às do MVP (`backend/app/data/area_piloto.json`).

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

CATALOGO, SCHEMA, VOLUME = "workspace", "zelacidade", "lake"
DB = f"{CATALOGO}.{SCHEMA}"
BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
CHEGADA = f"{BASE}/chegada"      # os lotes do Airflow entram aqui pela interface
CONTROLE = f"{BASE}/_controle"   # schema e checkpoint do Auto Loader
EXPORT = f"{BASE}/export"        # ouro_painel_AAAA-MM-DD.json para o MVP

CATEGORIAS = {
    "buraco": "Buraco na via",
    "calcada_danificada": "Calçada danificada",
    "iluminacao": "Iluminação apagada ou falhando",
    "obstaculo": "Obstáculo na passagem",
    "equipamento_danificado": "Equipamento público danificado",
    "lixo_entulho": "Lixo ou entulho",
    "outro": "Outro",
}
STATUS_ABERTOS = ["recebida", "triada", "precisa_info", "em_andamento"]

# Área piloto: Subprefeitura Santana/Tucuruvi (mesmo retângulo do MVP)
AREA = {"sul": -23.5180, "norte": -23.4550, "oeste": -46.6480, "leste": -46.5940}
BAIRROS = {
    "Santana": (-23.5025, -46.6251),
    "Jardim São Paulo": (-23.4947, -46.6194),
    "Mandaqui": (-23.4821, -46.6259),
    "Tucuruvi": (-23.4739, -46.6108),
}
CHUVA_FORTE_MM = 10.0

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {SCHEMA}")


def tabela_existe(nome: str) -> bool:
    return spark.catalog.tableExists(f"{DB}.{nome}")


def registrar_contagem(lote: str, momento: str, tabela: str) -> None:
    """Guarda quantas linhas e arquivos a tabela tem agora (evidência do requisito 05)."""
    if tabela_existe(tabela):
        linha = spark.sql(
            f"SELECT COUNT(*) AS linhas, COUNT(DISTINCT arquivo_origem) AS arquivos FROM {DB}.{tabela}"
        ).first()
        linhas, arquivos = linha["linhas"], linha["arquivos"]
    else:
        linhas, arquivos = 0, 0
    spark.createDataFrame(
        [(datetime.now(timezone.utc), lote, momento, tabela, int(linhas), int(arquivos))],
        "registrado_em timestamp, lote string, momento string, tabela string, linhas long, arquivos long",
    ).write.mode("append").saveAsTable(f"{DB}.controle_contagens")
    print(f"{lote} · {momento} · {tabela}: {linhas} linhas de {arquivos} arquivos")
