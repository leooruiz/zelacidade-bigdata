"""Testes das funções da coleta, sem Airflow e sem rede (sessão HTTP falsa)."""

import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

import coleta_lib as lib  # noqa: E402

LINHA = {
    "id": 1, "protocolo": "ZC-000001", "categoria": "buraco", "status": "recebida", "origem": "cidadao",
    "criado_em": "2026-11-10T13:00:00Z", "atualizado_em": "2026-11-10T13:00:00Z",
}


class Resposta:
    def __init__(self, status=200, conteudo=b"", dados=None):
        self.status_code = status
        self.content = conteudo
        self._dados = dados

    def json(self):
        return json.loads(json.dumps(self._dados))

    def raise_for_status(self):
        if self.status_code >= 400:
            raise lib.requests.HTTPError(f"HTTP {self.status_code}")


class Sessao:
    def __init__(self, resposta):
        self.resposta = resposta
        self.chamadas = []

    def get(self, url, **kwargs):
        self.chamadas.append((url, kwargs))
        return self.resposta

    def post(self, url, **kwargs):
        self.chamadas.append((url, kwargs))
        return self.resposta


def clima_api(datas, chuva):
    return {"latitude": -23.5, "longitude": -46.62, "daily": {
        "time": datas, "precipitation_sum": chuva, "rain_sum": chuva,
        "temperature_2m_max": [28.0] * len(datas), "temperature_2m_min": [18.0] * len(datas)}}


def test_ndjson_valido_conta_linhas():
    conteudo = (json.dumps(LINHA) + "\n" + json.dumps({**LINHA, "id": 2}) + "\n").encode()
    assert lib.validar_ndjson(conteudo) == 2
    assert lib.validar_ndjson(b"") == 0  # dia sem ocorrências é válido


def test_ndjson_sem_campo_obrigatorio_falha():
    with pytest.raises(lib.ColetaInvalida, match="categoria"):
        lib.validar_ndjson(json.dumps({k: v for k, v in LINHA.items() if k != "categoria"}).encode())


def test_ndjson_quebrado_falha():
    with pytest.raises(lib.ColetaInvalida, match="linha 1"):
        lib.validar_ndjson(b"{nao e json}\n")


def test_baixar_ocorrencias_usa_chave_e_data():
    sessao = Sessao(Resposta(conteudo=(json.dumps(LINHA) + "\n").encode()))
    conteudo = lib.baixar_ocorrencias("https://mvp.exemplo/", "chave", "2026-11-10", sessao=sessao)
    url, kwargs = sessao.chamadas[0]
    assert url == "https://mvp.exemplo/api/v1/export/ocorrencias"
    assert kwargs["params"] == {"data": "2026-11-10"}
    assert kwargs["headers"] == {"X-API-Key": "chave"}
    assert lib.validar_ndjson(conteudo) == 1


def test_baixar_ocorrencias_sem_variables_falha():
    with pytest.raises(lib.ColetaInvalida, match="Variables"):
        lib.baixar_ocorrencias("", "", "2026-11-10", sessao=Sessao(Resposta()))


def test_chave_recusada_vira_erro_claro():
    with pytest.raises(lib.ColetaInvalida, match="chave recusada"):
        lib.baixar_ocorrencias("https://mvp.exemplo", "errada", "2026-11-10", sessao=Sessao(Resposta(401)))


def test_clima_recente_usa_previsao_e_recorta_o_dia():
    datas = ["2026-11-08", "2026-11-09", "2026-11-10", "2026-11-11"]
    sessao = Sessao(Resposta(dados=clima_api(datas, [0.0, 12.5, 3.0, 0.0])))
    dados = lib.baixar_clima(-23.5, -46.62, "2026-11-09", hoje=date(2026, 11, 11), sessao=sessao)
    url, kwargs = sessao.chamadas[0]
    assert url == lib.OPEN_METEO_PREVISAO
    assert kwargs["params"]["past_days"] == 3 and kwargs["params"]["timezone"] == "America/Sao_Paulo"
    assert dados["daily"]["time"] == ["2026-11-09"]
    assert dados["daily"]["precipitation_sum"] == [12.5]
    assert dados["fonte"] == "open-meteo-previsao"


def test_clima_antigo_usa_historico():
    sessao = Sessao(Resposta(dados=clima_api(["2026-07-01"], [1.0])))
    dados = lib.baixar_clima(-23.5, -46.62, "2026-07-01", hoje=date(2026, 11, 11), sessao=sessao)
    assert sessao.chamadas[0][0] == lib.OPEN_METEO_HISTORICO
    assert sessao.chamadas[0][1]["params"]["start_date"] == "2026-07-01"
    assert dados["fonte"] == "open-meteo-historico"


def test_clima_de_dia_futuro_falha():
    with pytest.raises(lib.ColetaInvalida, match="ainda não aconteceu"):
        lib.baixar_clima(-23.5, -46.62, "2026-11-20", hoje=date(2026, 11, 11), sessao=Sessao(Resposta()))


def test_conferir_chegada_ok_e_reexecucao_regrava(tmp_path):
    dia = "2026-11-10"
    lib.gravar_atomico(lib.caminho_ocorrencias(tmp_path, dia), (json.dumps(LINHA) + "\n").encode())
    clima = clima_api([dia], [4.2])
    lib.gravar_atomico(lib.caminho_clima(tmp_path, dia), json.dumps(clima).encode())
    assert lib.conferir_chegada(tmp_path, dia) == {"dia": dia, "ocorrencias": 1, "chuva_mm": 4.2}
    # a mesma execução de novo regrava o mesmo arquivo, sem criar outro
    lib.gravar_atomico(lib.caminho_ocorrencias(tmp_path, dia), b"")
    assert sorted(p.name for p in (tmp_path / "ocorrencias").iterdir()) == [f"ocorrencias_{dia}.json"]


def test_conferir_chegada_sem_arquivo_falha(tmp_path):
    with pytest.raises(lib.ColetaInvalida, match="ausente"):
        lib.conferir_chegada(tmp_path, "2026-11-10")


def test_conferir_chegada_com_chuva_negativa_falha(tmp_path):
    dia = "2026-11-10"
    lib.gravar_atomico(lib.caminho_ocorrencias(tmp_path, dia), b"")
    lib.gravar_atomico(lib.caminho_clima(tmp_path, dia), json.dumps(clima_api([dia], [-1.0])).encode())
    with pytest.raises(lib.ColetaInvalida, match="negativa"):
        lib.conferir_chegada(tmp_path, dia)


def test_alerta_vai_ao_webhook_e_nunca_derruba():
    sessao = Sessao(Resposta())
    lib.avisar("falhou", "https://webhook.exemplo", sessao=sessao)
    assert sessao.chamadas[0][1]["json"]["content"] == "falhou"

    class Quebrada:
        def post(self, *a, **k):
            raise lib.requests.ConnectionError("fora")

    lib.avisar("falhou", "https://webhook.exemplo", sessao=Quebrada())  # não levanta
