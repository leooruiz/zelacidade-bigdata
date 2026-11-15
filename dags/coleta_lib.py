"""Funções da coleta diária do ZelaCidade, sem dependência do Airflow (testáveis com pytest).

Duas fontes por dia, gravadas em chegada/ com a data no nome:
- ocorrencias/ocorrencias_AAAA-MM-DD.json: NDJSON do export do MVP (interna, com chave);
- clima/clima_AAAA-MM-DD.json: chuva e temperatura do dia no bairro piloto, do Open-Meteo (externa, sem chave).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import date
from pathlib import Path

import requests

log = logging.getLogger("zelacidade.coleta")

FUSO = "America/Sao_Paulo"
VARIAVEIS_DIARIAS = "precipitation_sum,rain_sum,temperature_2m_max,temperature_2m_min"
# as URLs aceitam troca por variável de ambiente só para testes locais
OPEN_METEO_PREVISAO = os.environ.get("ZELA_OPEN_METEO_PREVISAO", "https://api.open-meteo.com/v1/forecast")
OPEN_METEO_HISTORICO = os.environ.get("ZELA_OPEN_METEO_HISTORICO", "https://archive-api.open-meteo.com/v1/archive")
CAMPOS_OCORRENCIA = ("id", "protocolo", "categoria", "status", "origem", "criado_em", "atualizado_em")
TIMEOUT_S = 120  # o App Service F1 dorme sem acesso; a primeira chamada pode demorar


class ColetaInvalida(ValueError):
    """Arquivo coletado fora do formato esperado (o Airflow marca a tarefa como falha)."""


def caminho_ocorrencias(chegada: Path, dia: str) -> Path:
    return Path(chegada) / "ocorrencias" / f"ocorrencias_{dia}.json"


def caminho_clima(chegada: Path, dia: str) -> Path:
    return Path(chegada) / "clima" / f"clima_{dia}.json"


def gravar_atomico(caminho: Path, conteudo: bytes) -> Path:
    """Grava num temporário e renomeia: o Auto Loader nunca vê um arquivo pela metade."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    fd, temporario = tempfile.mkstemp(dir=caminho.parent, prefix=".tmp_", suffix=caminho.suffix)
    with os.fdopen(fd, "wb") as arquivo:
        arquivo.write(conteudo)
    os.replace(temporario, caminho)
    return caminho


def validar_ndjson(conteudo: bytes) -> int:
    """Cada linha é um JSON com os campos mínimos. Devolve o número de ocorrências."""
    total = 0
    for numero, linha in enumerate(conteudo.decode("utf-8").splitlines(), start=1):
        if not linha.strip():
            continue
        try:
            registro = json.loads(linha)
        except ValueError as exc:
            raise ColetaInvalida(f"linha {numero} não é JSON: {exc}") from exc
        faltando = [campo for campo in CAMPOS_OCORRENCIA if campo not in registro]
        if faltando:
            raise ColetaInvalida(f"linha {numero} sem os campos {faltando}")
        total += 1
    return total


def baixar_ocorrencias(api_url: str, api_key: str, dia: str, sessao=requests) -> bytes:
    """GET /api/v1/export/ocorrencias?data=AAAA-MM-DD com X-API-Key (EXPORT_API_KEY do MVP)."""
    if not api_url or not api_key:
        raise ColetaInvalida("ZELA_API_URL e ZELA_API_KEY precisam estar nas Variables do Airflow")
    url = f"{api_url.rstrip('/')}/api/v1/export/ocorrencias"
    resposta = sessao.get(url, params={"data": dia}, headers={"X-API-Key": api_key}, timeout=TIMEOUT_S)
    if resposta.status_code in (401, 403):
        raise ColetaInvalida(f"chave recusada pelo MVP (HTTP {resposta.status_code})")
    resposta.raise_for_status()
    validar_ndjson(resposta.content)
    return resposta.content


def _recortar_dia(dados: dict, dia: str) -> dict:
    """Mantém só o dia pedido nas listas de 'daily' (a API de previsão devolve vários dias com past_days)."""
    diario = dados.get("daily") or {}
    datas = diario.get("time") or []
    if dia not in datas:
        raise ColetaInvalida(f"Open-Meteo não trouxe o dia {dia}")
    i = datas.index(dia)
    dados["daily"] = {chave: [valores[i]] for chave, valores in diario.items()}
    return dados


def baixar_clima(latitude: float, longitude: float, dia: str, hoje: date | None = None, sessao=requests) -> dict:
    """Chuva e temperatura do dia. O histórico ERA5 chega com cerca de 5 dias de atraso, então os dias
    recentes vêm da API de previsão (past_days cobre até 92 dias) e os antigos, do histórico."""
    hoje = hoje or date.today()
    alvo = date.fromisoformat(dia)
    if alvo > hoje:
        raise ColetaInvalida(f"o dia {dia} ainda não aconteceu")
    base = {"latitude": latitude, "longitude": longitude, "daily": VARIAVEIS_DIARIAS, "timezone": FUSO}
    if (hoje - alvo).days <= 90:
        params = {**base, "past_days": (hoje - alvo).days + 1, "forecast_days": 1}
        url = OPEN_METEO_PREVISAO
    else:
        params = {**base, "start_date": dia, "end_date": dia}
        url = OPEN_METEO_HISTORICO
    resposta = sessao.get(url, params=params, timeout=60)
    resposta.raise_for_status()
    dados = _recortar_dia(resposta.json(), dia)
    dados["fonte"] = "open-meteo-previsao" if url == OPEN_METEO_PREVISAO else "open-meteo-historico"
    dados["data_coleta"] = dia
    return dados


def conferir_chegada(chegada: Path, dia: str) -> dict:
    """Os dois arquivos do dia existem e estão no formato certo. Devolve um resumo para o log."""
    arq_ocorrencias = caminho_ocorrencias(chegada, dia)
    arq_clima = caminho_clima(chegada, dia)
    for arquivo in (arq_ocorrencias, arq_clima):
        if not arquivo.is_file():
            raise ColetaInvalida(f"arquivo ausente: {arquivo.name}")
    total = validar_ndjson(arq_ocorrencias.read_bytes())
    clima = json.loads(arq_clima.read_text(encoding="utf-8"))
    diario = clima.get("daily") or {}
    if diario.get("time") != [dia]:
        raise ColetaInvalida(f"{arq_clima.name} não é do dia {dia}")
    chuva = (diario.get("precipitation_sum") or [None])[0]
    if chuva is not None and chuva < 0:
        raise ColetaInvalida(f"chuva negativa em {arq_clima.name}")
    return {"dia": dia, "ocorrencias": total, "chuva_mm": chuva}


def avisar(mensagem: str, webhook: str | None, sessao=requests) -> None:
    """Alerta de falha: sempre no log e, se houver ZELA_ALERTA_WEBHOOK, também no webhook (Discord, Slack ou Teams)."""
    log.error(mensagem)
    if not webhook:
        return
    try:
        sessao.post(webhook, json={"content": mensagem, "text": mensagem}, timeout=10)
    except requests.RequestException as exc:  # o alerta nunca derruba a tarefa
        log.warning("webhook de alerta indisponível: %s", type(exc).__name__)

