# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Export da Ouro para o MVP
# MAGIC
# MAGIC O Databricks Free Edition não chama a internet, então a Ouro chega ao MVP assim:
# MAGIC
# MAGIC 1. este notebook grava `export/ouro_painel_AAAA-MM-DD.json` no Volume, com a tabela `ouro_painel` e os KPIs;
# MAGIC 2. você baixa o arquivo pela interface (**Catalog → workspace → zelacidade → Volumes → lake → export → ⋮ → Download**);
# MAGIC 3. coloca o arquivo em `zelacidade-rpa/entrada/ouro/`;
# MAGIC 4. o robô RPA valida e publica com `POST /api/v1/integracao/painel`, e a tela **Painel** do MVP mostra os dados.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import json
from decimal import Decimal


def limpo(valor):
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, float):
        return round(valor, 3)
    return valor


linhas = [{k: limpo(v) for k, v in linha.asDict().items()}
          for linha in spark.table(f"{DB}.ouro_painel").orderBy("bairro", "categoria").collect()]

kpis = spark.sql(f"""
SELECT COUNT(*) AS total,
       SUM(CASE WHEN f.aberta THEN 1 ELSE 0 END) AS abertas,
       SUM(CASE WHEN f.resolvida THEN 1 ELSE 0 END) AS resolvidas,
       round(SUM(CASE WHEN f.resolvida_com_evidencia THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_resolvidas_com_evidencia,
       round(AVG(f.horas_ate_triagem), 1) AS horas_media_triagem,
       round(AVG(f.dias_ate_resolucao), 1) AS dias_media_resolucao,
       SUM(CASE WHEN NOT f.simulado THEN 1 ELSE 0 END) AS ocorrencias_reais
FROM {DB}.fato_ocorrencias f
JOIN {DB}.dim_bairro b ON b.bairro = f.bairro AND b.area_piloto
WHERE f.status <> 'duplicada'
""").first().asDict()
kpis = {k: limpo(v) for k, v in kpis.items()}

referencia = spark.sql(f"SELECT MAX(data_registro) AS d FROM {DB}.fato_ocorrencias").first()["d"]
painel = {
    "fonte": f"databricks:{DB}.ouro_painel",
    "gerado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "data_referencia": str(referencia),
    "linhas": linhas,
    "kpis": kpis,
}
if not linhas:
    raise ValueError("ouro_painel está vazia: rode 02_prata e 03_ouro antes do export.")

nome = f"ouro_painel_{datetime.now(timezone.utc).date().isoformat()}.json"
dbutils.fs.put(f"{EXPORT}/{nome}", json.dumps(painel, ensure_ascii=False, indent=2, default=str), overwrite=True)
print(f"Gravado {EXPORT}/{nome}: {len(linhas)} linhas e {len(kpis)} KPIs")
print(json.dumps(kpis, ensure_ascii=False, indent=2))

# COMMAND ----------

display(dbutils.fs.ls(EXPORT))
