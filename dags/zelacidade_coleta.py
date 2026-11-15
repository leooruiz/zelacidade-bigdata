"""DAG zelacidade_coleta: coleta diária das duas fontes do ZelaCidade para a pasta chegada/.

inicio >> [coletar_ocorrencias, coletar_clima] >> conferir_chegada

- Agendamento diário à meia-noite de São Paulo, desde 01/11/2026, com catchup: cada execução coleta o
  dia que terminou (data_interval_start), então uma reexecução do mesmo dia regrava o mesmo arquivo.
- Chaves e URL ficam nas Variables do Airflow (Admin > Variables), nunca no código:
  ZELA_API_URL, ZELA_API_KEY (a EXPORT_API_KEY do MVP) e, opcionais, ZELA_LATITUDE, ZELA_LONGITUDE
  e ZELA_ALERTA_WEBHOOK.
- 2 novas tentativas por tarefa e alerta de falha no log e no webhook, se houver.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

import coleta_lib as lib

FUSO = "America/Sao_Paulo"
CHEGADA = Path(os.environ.get("ZELA_CHEGADA_DIR", "/opt/airflow/chegada"))
# centro da área piloto do MVP (backend/app/data/area_piloto.json); dá para trocar por Variable
LATITUDE_PADRAO = "-23.4865"
LONGITUDE_PADRAO = "-46.6210"


def _dia_coletado() -> str:
    """O dia que a execução cobre, no horário de São Paulo (AAAA-MM-DD)."""
    contexto = get_current_context()
    return contexto["data_interval_start"].in_timezone(FUSO).to_date_string()


def avisar_falha(context) -> None:
    ti = context["ti"]
    inicio = context["data_interval_start"].in_timezone(FUSO).to_date_string()
    mensagem = (
        f"[ZelaCidade] Falha na coleta: tarefa {ti.task_id}, dia {inicio}, "
        f"tentativa {ti.try_number}. Veja o log no Airflow."
    )
    lib.avisar(mensagem, Variable.get("ZELA_ALERTA_WEBHOOK", default=None))


@dag(
    dag_id="zelacidade_coleta",
    description="Coleta diária: ocorrências do MVP (NDJSON) e clima do bairro piloto (Open-Meteo)",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=FUSO),
    start_date=pendulum.datetime(2026, 11, 1, tz=FUSO),
    catchup=True,
    max_active_runs=2,
    default_args={
        "owner": "leonardo",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": avisar_falha,
    },
    tags=["zelacidade", "big-data"],
)
def zelacidade_coleta():
    inicio = EmptyOperator(task_id="inicio")

    @task
    def coletar_ocorrencias() -> dict:
        dia = _dia_coletado()
        conteudo = lib.baixar_ocorrencias(
            Variable.get("ZELA_API_URL", default=""), Variable.get("ZELA_API_KEY", default=""), dia
        )
        destino = lib.gravar_atomico(lib.caminho_ocorrencias(CHEGADA, dia), conteudo)
        return {"arquivo": destino.name, "ocorrencias": lib.validar_ndjson(conteudo)}

    @task
    def coletar_clima() -> dict:
        dia = _dia_coletado()
        dados = lib.baixar_clima(
            float(Variable.get("ZELA_LATITUDE", default=LATITUDE_PADRAO)),
            float(Variable.get("ZELA_LONGITUDE", default=LONGITUDE_PADRAO)),
            dia,
        )
        conteudo = json.dumps(dados, ensure_ascii=False).encode("utf-8")
        destino = lib.gravar_atomico(lib.caminho_clima(CHEGADA, dia), conteudo)
        return {"arquivo": destino.name, "chuva_mm": dados["daily"]["precipitation_sum"][0]}

    @task
    def conferir_chegada() -> dict:
        return lib.conferir_chegada(CHEGADA, _dia_coletado())

    inicio >> [coletar_ocorrencias(), coletar_clima()] >> conferir_chegada()


zelacidade_coleta()
