# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · As 5 perguntas que o MVP responde
# MAGIC
# MAGIC Cada consulta usa a Ouro. A 5ª é a de **impacto**. A interpretação de cada resultado vai no PDF.
# MAGIC Os registros simulados (`simulado = true`) entram nos totais e aparecem separados onde faz diferença.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Quantas ocorrências estão abertas por bairro e categoria?
# MAGIC Mostra onde a organização responsável deve olhar primeiro. É a mesma visão da tela Painel do MVP.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.bairro, c.rotulo AS categoria, COUNT(*) AS abertas,
# MAGIC        SUM(CASE WHEN f.prioridade = 'alta' THEN 1 ELSE 0 END) AS prioridade_alta
# MAGIC FROM workspace.zelacidade.fato_ocorrencias f
# MAGIC JOIN workspace.zelacidade.dim_categoria c ON c.categoria = f.categoria
# MAGIC JOIN workspace.zelacidade.dim_bairro b ON b.bairro = f.bairro AND b.area_piloto
# MAGIC WHERE f.aberta
# MAGIC GROUP BY f.bairro, c.rotulo
# MAGIC ORDER BY abertas DESC, f.bairro
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quais problemas abertos têm mais confirmações da comunidade?
# MAGIC Confirmação é o sinal do morador de que o problema existe e incomoda. Sem dado pessoal: só protocolo e bairro.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.protocolo, f.bairro, c.rotulo AS categoria, f.confirmacoes, f.prioridade,
# MAGIC        datediff(current_date(), f.data_registro) AS dias_aberta,
# MAGIC        CASE WHEN f.simulado THEN 'simulado' ELSE 'real' END AS tipo
# MAGIC FROM workspace.zelacidade.fato_ocorrencias f
# MAGIC JOIN workspace.zelacidade.dim_categoria c ON c.categoria = f.categoria
# MAGIC WHERE f.aberta
# MAGIC ORDER BY f.confirmacoes DESC, dias_aberta DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quanto tempo a triagem leva, por categoria?
# MAGIC Mede o trabalho do robô RPA: do registro até a triagem.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.rotulo AS categoria,
# MAGIC        COUNT(f.horas_ate_triagem) AS triadas,
# MAGIC        round(AVG(f.horas_ate_triagem), 1) AS horas_media,
# MAGIC        round(percentile_approx(f.horas_ate_triagem, 0.5), 1) AS horas_mediana,
# MAGIC        round(MAX(f.horas_ate_triagem), 1) AS horas_maxima
# MAGIC FROM workspace.zelacidade.fato_ocorrencias f
# MAGIC JOIN workspace.zelacidade.dim_categoria c ON c.categoria = f.categoria
# MAGIC WHERE f.horas_ate_triagem IS NOT NULL
# MAGIC GROUP BY c.rotulo
# MAGIC ORDER BY horas_media DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dias de chuva forte (10 mm ou mais) geram mais registros de buraco?
# MAGIC Cruza a fonte externa (Open-Meteo) com os registros. Justifica a regra do robô RPA que sobe a prioridade de
# MAGIC buraco quando há previsão de chuva forte. Com poucos dias, é um indício, não uma conclusão.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH buracos_por_dia AS (
# MAGIC   SELECT data_registro AS data, COUNT(*) AS buracos
# MAGIC   FROM workspace.zelacidade.fato_ocorrencias
# MAGIC   WHERE categoria = 'buraco' AND status <> 'duplicada'
# MAGIC   GROUP BY data_registro
# MAGIC )
# MAGIC SELECT CASE WHEN cl.chuva_forte THEN 'chuva forte (10 mm ou mais)' ELSE 'sem chuva forte' END AS tipo_de_dia,
# MAGIC        COUNT(*) AS dias,
# MAGIC        SUM(COALESCE(b.buracos, 0)) AS buracos_registrados,
# MAGIC        round(AVG(COALESCE(b.buracos, 0)), 2) AS buracos_por_dia,
# MAGIC        round(AVG(cl.chuva_mm), 1) AS chuva_media_mm
# MAGIC FROM workspace.zelacidade.dim_clima_dia cl
# MAGIC JOIN workspace.zelacidade.dim_tempo t ON t.data = cl.data
# MAGIC LEFT JOIN buracos_por_dia b ON b.data = cl.data
# MAGIC GROUP BY cl.chuva_forte
# MAGIC ORDER BY tipo_de_dia;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Impacto: que porcentagem das ocorrências foi resolvida com evidência, e em quantos dias?
# MAGIC No MVP, a organização só fecha uma ocorrência com foto da solução. Esta é a medida de impacto do ZelaCidade.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COALESCE(f.bairro, 'Total da área piloto') AS bairro,
# MAGIC        COUNT(*) AS ocorrencias,
# MAGIC        SUM(CASE WHEN f.resolvida_com_evidencia THEN 1 ELSE 0 END) AS resolvidas_com_foto,
# MAGIC        round(100.0 * SUM(CASE WHEN f.resolvida_com_evidencia THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_resolvidas,
# MAGIC        round(AVG(f.dias_ate_resolucao), 1) AS dias_medios_ate_resolver,
# MAGIC        SUM(CASE WHEN NOT f.simulado THEN 1 ELSE 0 END) AS reais
# MAGIC FROM workspace.zelacidade.fato_ocorrencias f
# MAGIC JOIN workspace.zelacidade.dim_bairro b ON b.bairro = f.bairro AND b.area_piloto
# MAGIC WHERE f.status <> 'duplicada'
# MAGIC GROUP BY ROLLUP(f.bairro)
# MAGIC ORDER BY GROUPING(f.bairro), f.bairro;
