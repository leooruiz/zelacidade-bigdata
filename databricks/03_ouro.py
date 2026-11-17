# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro (modelo dimensional)
# MAGIC
# MAGIC Estrela com 1 fato e 4 dimensões, mais a tabela `ouro_painel`, que é a que o MVP consome:
# MAGIC
# MAGIC | Tabela | Grão | Para quê |
# MAGIC |---|---|---|
# MAGIC | `fato_ocorrencias` | uma linha por ocorrência | contagens, tempos de triagem e de resolução, confirmações |
# MAGIC | `dim_tempo` | um dia | dia da semana, mês, fim de semana |
# MAGIC | `dim_categoria` | uma categoria | rótulo e peso da categoria na prioridade do robô RPA |
# MAGIC | `dim_bairro` | um bairro | se está na área piloto e o centro aproximado |
# MAGIC | `dim_clima_dia` | um dia | chuva, temperaturas e se foi dia de chuva forte (10 mm ou mais) |
# MAGIC | `ouro_painel` | bairro × categoria | o que a tela **Painel** do MVP mostra |
# MAGIC
# MAGIC **LGPD na Ouro:** não há e-mail em claro, só `email_hash` (sha2 com sal) e `email_mascara`; a coordenada é
# MAGIC arredondada para 3 casas (cerca de 110 m); fotos nunca entram no lake.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

spark.createDataFrame(
    [(chave, rotulo, {"buraco": 3, "obstaculo": 3, "iluminacao": 2}.get(chave, 1)) for chave, rotulo in CATEGORIAS.items()],
    "categoria string, rotulo string, pontos_prioridade int",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DB}.dim_categoria")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.zelacidade.fato_ocorrencias AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   protocolo,
# MAGIC   to_date(from_utc_timestamp(criado_em, 'America/Sao_Paulo')) AS data_registro,
# MAGIC   categoria,
# MAGIC   bairro,
# MAGIC   status,
# MAGIC   prioridade,
# MAGIC   origem,
# MAGIC   simulado,
# MAGIC   confirmacoes,
# MAGIC   confianca_ia,
# MAGIC   severidade_ia,
# MAGIC   duplicada_de,
# MAGIC   status IN ('recebida', 'triada', 'precisa_info', 'em_andamento') AS aberta,
# MAGIC   status = 'resolvida' AS resolvida,
# MAGIC   -- no MVP só dá para marcar como resolvida com foto da solução: toda resolvida tem evidência
# MAGIC   status = 'resolvida' AND resolvido_em IS NOT NULL AS resolvida_com_evidencia,
# MAGIC   round((unix_timestamp(triado_em) - unix_timestamp(criado_em)) / 3600.0, 2) AS horas_ate_triagem,
# MAGIC   round((unix_timestamp(resolvido_em) - unix_timestamp(criado_em)) / 86400.0, 2) AS dias_ate_resolucao,
# MAGIC   round(latitude, 3) AS latitude_aprox,
# MAGIC   round(longitude, 3) AS longitude_aprox,
# MAGIC   email_hash,
# MAGIC   email_mascara,
# MAGIC   consentimento
# MAGIC FROM workspace.zelacidade.prata_ocorrencias;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.zelacidade.dim_tempo AS
# MAGIC WITH limites AS (
# MAGIC   SELECT LEAST(MIN(data_registro), (SELECT MIN(data) FROM workspace.zelacidade.prata_clima)) AS inicio,
# MAGIC          GREATEST(MAX(data_registro), (SELECT MAX(data) FROM workspace.zelacidade.prata_clima)) AS fim
# MAGIC   FROM workspace.zelacidade.fato_ocorrencias
# MAGIC )
# MAGIC SELECT
# MAGIC   d AS data,
# MAGIC   year(d) AS ano,
# MAGIC   month(d) AS mes,
# MAGIC   day(d) AS dia,
# MAGIC   weekofyear(d) AS semana_do_ano,
# MAGIC   CASE dayofweek(d) WHEN 1 THEN 'domingo' WHEN 2 THEN 'segunda' WHEN 3 THEN 'terça' WHEN 4 THEN 'quarta'
# MAGIC        WHEN 5 THEN 'quinta' WHEN 6 THEN 'sexta' ELSE 'sábado' END AS dia_da_semana,
# MAGIC   dayofweek(d) IN (1, 7) AS fim_de_semana
# MAGIC FROM limites
# MAGIC LATERAL VIEW explode(sequence(COALESCE(inicio, fim), COALESCE(fim, inicio), INTERVAL 1 DAY)) AS d;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.zelacidade.dim_bairro AS
# MAGIC SELECT b.bairro, true AS area_piloto, b.latitude_centro, b.longitude_centro
# MAGIC FROM workspace.zelacidade.ref_bairros_piloto b
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT f.bairro, false AS area_piloto, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE)
# MAGIC FROM workspace.zelacidade.fato_ocorrencias f
# MAGIC WHERE f.bairro NOT IN (SELECT bairro FROM workspace.zelacidade.ref_bairros_piloto);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.zelacidade.dim_clima_dia AS
# MAGIC SELECT data, chuva_mm, temp_max, temp_min, chuva_mm >= 10 AS chuva_forte, fonte
# MAGIC FROM workspace.zelacidade.prata_clima;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabela que o MVP consome: `ouro_painel`
# MAGIC Duplicadas ficam de fora dos totais, porque já viraram confirmação na ocorrência original.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.zelacidade.ouro_painel AS
# MAGIC SELECT
# MAGIC   f.bairro,
# MAGIC   f.categoria,
# MAGIC   COUNT(*) AS total,
# MAGIC   SUM(CASE WHEN f.aberta THEN 1 ELSE 0 END) AS abertas,
# MAGIC   SUM(CASE WHEN f.resolvida THEN 1 ELSE 0 END) AS resolvidas,
# MAGIC   SUM(f.confirmacoes) AS confirmacoes,
# MAGIC   round(AVG(f.horas_ate_triagem), 1) AS horas_media_triagem,
# MAGIC   round(AVG(f.dias_ate_resolucao), 1) AS dias_media_resolucao
# MAGIC FROM workspace.zelacidade.fato_ocorrencias f
# MAGIC JOIN workspace.zelacidade.dim_bairro b ON b.bairro = f.bairro AND b.area_piloto
# MAGIC WHERE f.status <> 'duplicada'
# MAGIC GROUP BY f.bairro, f.categoria
# MAGIC ORDER BY f.bairro, total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.zelacidade.ouro_painel ORDER BY bairro, total DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidências de LGPD
# MAGIC A Ouro não tem nenhuma coluna com e-mail em claro, nome ou foto.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT protocolo, bairro, latitude_aprox, longitude_aprox, email_mascara, left(email_hash, 12) AS inicio_do_hash, consentimento
# MAGIC FROM workspace.zelacidade.fato_ocorrencias
# MAGIC WHERE email_hash IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fato_ocorrencias' AS tabela, COUNT(*) AS linhas FROM workspace.zelacidade.fato_ocorrencias
# MAGIC UNION ALL SELECT 'dim_tempo', COUNT(*) FROM workspace.zelacidade.dim_tempo
# MAGIC UNION ALL SELECT 'dim_categoria', COUNT(*) FROM workspace.zelacidade.dim_categoria
# MAGIC UNION ALL SELECT 'dim_bairro', COUNT(*) FROM workspace.zelacidade.dim_bairro
# MAGIC UNION ALL SELECT 'dim_clima_dia', COUNT(*) FROM workspace.zelacidade.dim_clima_dia
# MAGIC UNION ALL SELECT 'ouro_painel', COUNT(*) FROM workspace.zelacidade.ouro_painel;
