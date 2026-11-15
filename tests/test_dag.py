"""Estrutura do DAG (roda só onde o Airflow está instalado; no PC, dentro do container)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    return bag.get_dag("zelacidade_coleta")


def test_paralelismo_e_conferencia(dag):
    assert set(dag.task_ids) == {"inicio", "coletar_ocorrencias", "coletar_clima", "conferir_chegada"}
    assert dag.get_task("inicio").downstream_task_ids == {"coletar_ocorrencias", "coletar_clima"}
    assert dag.get_task("conferir_chegada").upstream_task_ids == {"coletar_ocorrencias", "coletar_clima"}


def test_agenda_retries_e_alerta(dag):
    assert dag.catchup is True
    assert str(dag.start_date.date()) == "2026-11-01"
    for task in dag.tasks:
        assert task.retries == 2
        assert task.on_failure_callback
