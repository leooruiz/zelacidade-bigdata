# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata (tipos, nulos, regras de qualidade e MERGE)
# MAGIC
# MAGIC - **Tipos e nulos:** datas viram timestamp, números viram número, texto vazio vira "Não informado",
# MAGIC   confirmações nulas viram 0.
# MAGIC - **Regras de qualidade, com o total barrado por regra** (tabela `prata_qualidade`):
# MAGIC   - **R1** id e categoria válidos;
# MAGIC   - **R2** coordenada dentro da área piloto, só para registros de cidadãos (o robô Maker e os simulados não
# MAGIC     entram na regra);
# MAGIC   - **R3** datas coerentes: criação presente, não futura e anterior à última atualização;
# MAGIC   - **R4** (clima) dia presente e chuva maior ou igual a zero.
# MAGIC - **Deduplicação e MERGE pela chave:** a mesma ocorrência chega em vários arquivos (a cada dia em que muda);
# MAGIC   fica a versão mais recente, e o MERGE pelo `id` atualiza ou insere. Rodar de novo não muda nada.
# MAGIC - **LGPD:** o e-mail nunca entra na Prata em claro: vira `sha2(e-mail + sal)` e uma máscara (`m***@dominio`).
# MAGIC   O sal é informado no widget e não fica no código. Use sempre o mesmo valor.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql.window import Window

dbutils.widgets.text("sal_lgpd", "", "Sal LGPD (mín. 12 caracteres)")
SAL = dbutils.widgets.get("sal_lgpd")
if len(SAL) < 12:
    raise ValueError("Informe o sal LGPD no widget (pelo menos 12 caracteres, sempre o mesmo valor).")
AGORA = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ocorrências

# COMMAND ----------

bronze = spark.table(f"{DB}.bronze_ocorrencias")


def texto(coluna):
    valor = F.trim(F.col(coluna).cast("string"))
    return F.when(valor == "", None).otherwise(valor)


email = F.lower(texto("autor_email"))
email_valido = email.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$") & F.coalesce(F.col("consentimento").cast("boolean"), F.lit(False))

tipada = bronze.select(
    F.col("id").cast("bigint").alias("id"),
    texto("protocolo").alias("protocolo"),
    F.lower(texto("categoria")).alias("categoria"),
    F.lower(texto("categoria_ia")).alias("categoria_ia"),
    F.col("confianca_ia").cast("double").alias("confianca_ia"),
    texto("severidade_ia").alias("severidade_ia"),
    F.coalesce(texto("descricao"), F.lit("")).alias("descricao"),
    F.coalesce(texto("bairro"), F.lit("Não informado")).alias("bairro"),
    F.col("latitude").cast("double").alias("latitude"),
    F.col("longitude").cast("double").alias("longitude"),
    F.coalesce(F.col("local_aproximado").cast("boolean"), F.lit(False)).alias("local_aproximado"),
    F.lower(texto("status")).alias("status"),
    F.lower(texto("prioridade")).alias("prioridade"),
    texto("prioridade_motivo").alias("prioridade_motivo"),
    F.col("duplicada_de").cast("bigint").alias("duplicada_de"),
    F.coalesce(F.col("confirmacoes").cast("int"), F.lit(0)).alias("confirmacoes"),
    F.lower(texto("origem")).alias("origem"),
    F.coalesce(F.col("simulado").cast("boolean"), F.lit(False)).alias("simulado"),
    F.to_timestamp(texto("criado_em")).alias("criado_em"),
    F.to_timestamp(texto("atualizado_em")).alias("atualizado_em"),
    F.to_timestamp(texto("triado_em")).alias("triado_em"),
    F.to_timestamp(texto("resolvido_em")).alias("resolvido_em"),
    texto("robot_id").alias("robot_id"),
    texto("test_location").alias("test_location"),
    # LGPD: só hash e máscara, nunca o e-mail
    F.when(email_valido, F.sha2(F.concat(email, F.lit(SAL)), 256)).alias("email_hash"),
    F.when(email_valido, F.concat(F.substring(email, 1, 1), F.lit("***@"), F.substring_index(email, "@", -1)))
    .alias("email_mascara"),
    F.coalesce(F.col("consentimento").cast("boolean"), F.lit(False)).alias("consentimento"),
    F.to_date(texto("data_coleta")).alias("data_coleta"),
    F.col("arquivo_origem"),
    F.col("carregado_em"),
)

r1 = F.col("id").isNotNull() & F.col("categoria").isin(list(CATEGORIAS))
r2 = (F.col("origem") != "cidadao") | (
    F.col("latitude").between(AREA["sul"], AREA["norte"]) & F.col("longitude").between(AREA["oeste"], AREA["leste"])
)
r3 = (
    F.col("criado_em").isNotNull()
    & (F.col("criado_em") <= F.col("carregado_em") + F.expr("INTERVAL 10 MINUTES"))
    & (F.col("atualizado_em").isNull() | (F.col("criado_em") <= F.col("atualizado_em")))
)
avaliada = tipada.withColumn(
    "motivo_barrado",
    F.when(~F.coalesce(r1, F.lit(False)), "R1")
    .when(~F.coalesce(r2, F.lit(False)), "R2")
    .when(~F.coalesce(r3, F.lit(False)), "R3"),
)

# COMMAND ----------

REGRAS_OCORRENCIAS = {
    "R1": "id e categoria válidos",
    "R2": "coordenada dentro da área piloto (só cidadãos)",
    "R3": "datas coerentes (criação presente, não futura, antes da atualização)",
}
lidas = avaliada.count()
contagem = {r["motivo_barrado"]: r["count"] for r in avaliada.groupBy("motivo_barrado").count().collect()}
spark.createDataFrame(
    [(AGORA, "prata_ocorrencias", regra, descricao, lidas, int(contagem.get(regra, 0)))
     for regra, descricao in REGRAS_OCORRENCIAS.items()],
    "executado_em timestamp, tabela string, regra string, descricao string, lidas long, barradas long",
).write.mode("append").saveAsTable(f"{DB}.prata_qualidade")

# as barradas ficam guardadas para auditoria, com o motivo
avaliada.filter("motivo_barrado IS NOT NULL").drop("email_hash", "email_mascara").write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(f"{DB}.prata_ocorrencias_barradas")

# uma linha por id: a versão mais recente (a mesma ocorrência aparece no arquivo de cada dia em que mudou)
lote = (
    avaliada.filter("motivo_barrado IS NULL").drop("motivo_barrado")
    .withColumn("_ordem", F.row_number().over(
        Window.partitionBy("id").orderBy(F.col("atualizado_em").desc_nulls_last(), F.col("carregado_em").desc())))
    .filter("_ordem = 1").drop("_ordem")
)
lote.createOrReplaceTempView("lote_prata_ocorrencias")
print(f"Lidas na Bronze: {lidas} · barradas: {sum(v for k, v in contagem.items() if k)} · ocorrências distintas válidas: {lote.count()}")

# COMMAND ----------

if not tabela_existe("prata_ocorrencias"):
    lote.limit(0).write.saveAsTable(f"{DB}.prata_ocorrencias")

spark.sql(f"""
MERGE INTO {DB}.prata_ocorrencias AS alvo
USING lote_prata_ocorrencias AS novo
ON alvo.id = novo.id
WHEN MATCHED AND novo.atualizado_em >= alvo.atualizado_em THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
display(spark.sql(f"SELECT status, COUNT(*) AS ocorrencias FROM {DB}.prata_ocorrencias GROUP BY status ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clima (Open-Meteo)
# MAGIC Cada arquivo traz listas paralelas em `daily` (datas, chuva, temperaturas). `arrays_zip` + `explode` vira
# MAGIC uma linha por dia, e o MERGE é pela data.

# COMMAND ----------

clima = (
    spark.table(f"{DB}.bronze_clima")
    .select(
        F.explode(F.arrays_zip(
            F.col("daily.time").alias("time"),
            F.col("daily.precipitation_sum").alias("precipitation_sum"),
            F.col("daily.temperature_2m_max").alias("temperature_2m_max"),
            F.col("daily.temperature_2m_min").alias("temperature_2m_min"),
        )).alias("d"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("fonte").cast("string").alias("fonte"),
        "arquivo_origem",
        "carregado_em",
    )
    .select(
        F.to_date(F.col("d.time").cast("string")).alias("data"),
        F.col("d.precipitation_sum").cast("double").alias("chuva_mm"),
        F.col("d.temperature_2m_max").cast("double").alias("temp_max"),
        F.col("d.temperature_2m_min").cast("double").alias("temp_min"),
        "latitude", "longitude", "fonte", "arquivo_origem", "carregado_em",
    )
)
r4 = F.col("data").isNotNull() & F.col("chuva_mm").isNotNull() & (F.col("chuva_mm") >= 0)
clima_avaliado = clima.withColumn("motivo_barrado", F.when(~F.coalesce(r4, F.lit(False)), "R4"))
lidas_clima = clima_avaliado.count()
barradas_clima = clima_avaliado.filter("motivo_barrado IS NOT NULL").count()
spark.createDataFrame(
    [(AGORA, "prata_clima", "R4", "dia presente e chuva maior ou igual a zero", lidas_clima, barradas_clima)],
    "executado_em timestamp, tabela string, regra string, descricao string, lidas long, barradas long",
).write.mode("append").saveAsTable(f"{DB}.prata_qualidade")

lote_clima = (
    clima_avaliado.filter("motivo_barrado IS NULL").drop("motivo_barrado")
    .withColumn("_ordem", F.row_number().over(Window.partitionBy("data").orderBy(F.col("carregado_em").desc())))
    .filter("_ordem = 1").drop("_ordem")
)
lote_clima.createOrReplaceTempView("lote_prata_clima")
if not tabela_existe("prata_clima"):
    lote_clima.limit(0).write.saveAsTable(f"{DB}.prata_clima")
spark.sql(f"""
MERGE INTO {DB}.prata_clima AS alvo
USING lote_prata_clima AS novo
ON alvo.data = novo.data
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print(f"Clima: {lidas_clima} dias lidos, {barradas_clima} barrados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidências: total barrado por regra (última execução)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tabela, regra, descricao, lidas, barradas
# MAGIC FROM workspace.zelacidade.prata_qualidade
# MAGIC WHERE executado_em = (SELECT MAX(executado_em) FROM workspace.zelacidade.prata_qualidade)
# MAGIC ORDER BY tabela, regra;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT motivo_barrado, origem, protocolo, categoria, latitude, longitude, criado_em
# MAGIC FROM workspace.zelacidade.prata_ocorrencias_barradas
# MAGIC ORDER BY motivo_barrado, protocolo;
