# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparação
# MAGIC
# MAGIC Cria o schema `workspace.zelacidade`, o Volume `lake` e as pastas onde os lotes do Airflow entram.
# MAGIC Rode uma vez. Depois, envie os arquivos pela interface:
# MAGIC **Catalog → workspace → zelacidade → Volumes → lake → chegada → ocorrencias** (e **clima**) **→ Upload to this volume**.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {DB}.{VOLUME}")
for pasta in ["chegada/ocorrencias", "chegada/clima", "_controle", "export"]:
    dbutils.fs.mkdirs(f"{BASE}/{pasta}")
display(dbutils.fs.ls(BASE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas de referência e de controle

# COMMAND ----------

spark.createDataFrame(
    [(nome, lat, lon) for nome, (lat, lon) in BAIRROS.items()],
    "bairro string, latitude_centro double, longitude_centro double",
).write.mode("overwrite").saveAsTable(f"{DB}.ref_bairros_piloto")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DB}.controle_contagens (
  registrado_em timestamp, lote string, momento string, tabela string, linhas long, arquivos long)
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DB}.prata_qualidade (
  executado_em timestamp, tabela string, regra string, descricao string, lidas long, barradas long)
""")
display(spark.sql(f"SHOW TABLES IN {DB}"))
