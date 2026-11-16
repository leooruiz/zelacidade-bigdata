# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze (Auto Loader, incremental)
# MAGIC
# MAGIC Cópia fiel do que chegou em `chegada/`, uma tabela por fonte. O Auto Loader guarda no checkpoint quais
# MAGIC arquivos já leu: no 2º lote só os arquivos novos entram, e rodar de novo não duplica nada.
# MAGIC
# MAGIC **Como usar:** escolha o lote no widget lá em cima (`lote1`, `lote2` ou `reexecucao`) e rode tudo. A tabela
# MAGIC `controle_contagens` guarda as contagens antes e depois de cada execução: é a evidência do requisito 05.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("lote", "lote1", ["lote1", "lote2", "reexecucao"], "Lote")
LOTE = dbutils.widgets.get("lote")

for tabela in ["bronze_ocorrencias", "bronze_clima"]:
    registrar_contagem(LOTE, "antes", tabela)

# COMMAND ----------


def carregar_bronze(fonte: str, opcoes: dict) -> None:
    """Lê chegada/<fonte> de forma incremental e acrescenta em bronze_<fonte>."""
    consulta = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CONTROLE}/schema_{fonte}")
        .options(**opcoes)
        .load(f"{CHEGADA}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream.option("checkpointLocation", f"{CONTROLE}/checkpoint_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{DB}.bronze_{fonte}")
    )
    consulta.awaitTermination()


# ocorrências: NDJSON (uma ocorrência por linha) · clima: um objeto JSON por arquivo
carregar_bronze("ocorrencias", {})
carregar_bronze("clima", {"multiLine": "true"})

# COMMAND ----------

for tabela in ["bronze_ocorrencias", "bronze_clima"]:
    registrar_contagem(LOTE, "depois", tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidências
# MAGIC Contagens de cada execução (print para o PDF) e quantas linhas vieram de cada arquivo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT registrado_em, lote, momento, tabela, linhas, arquivos
# MAGIC FROM workspace.zelacidade.controle_contagens
# MAGIC ORDER BY registrado_em, tabela;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_extract(arquivo_origem, '[^/]+$', 0) AS arquivo, COUNT(*) AS linhas, MIN(carregado_em) AS carregado_em
# MAGIC FROM workspace.zelacidade.bronze_ocorrencias
# MAGIC GROUP BY arquivo_origem
# MAGIC ORDER BY arquivo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sem duplicação: cada arquivo entra uma vez, mesmo rodando de novo
# MAGIC SELECT COUNT(*) AS linhas, COUNT(DISTINCT arquivo_origem) AS arquivos,
# MAGIC        COUNT(*) - COUNT(DISTINCT arquivo_origem, id) AS linhas_repetidas_do_mesmo_arquivo
# MAGIC FROM workspace.zelacidade.bronze_ocorrencias;
