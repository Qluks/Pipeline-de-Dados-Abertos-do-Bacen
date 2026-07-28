"""
Testes da ingestão (src/ingestion/bacen_api.py).

A API do Bacen nunca é chamada de verdade aqui — tudo é mockado, pra
os testes rodarem rápido e de forma determinística, sem depender de
rede (inclusive em CI).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from src.ingestion.bacen_api import (
    BacenAPIError,
    _gerar_janelas_anuais,
    fetch_serie,
    save_raw,
)


class TestGerarJanelasAnuais:
    def test_intervalo_menor_que_um_ano_gera_uma_janela(self):
        janelas = _gerar_janelas_anuais(date(2024, 1, 1), date(2024, 6, 1))
        assert janelas == [(date(2024, 1, 1), date(2024, 6, 1))]

    def test_intervalo_de_tres_anos_gera_tres_janelas(self):
        janelas = _gerar_janelas_anuais(date(2015, 1, 1), date(2018, 6, 15))
        assert len(janelas) == 4  # 2015, 2016, 2017, e o pedaço final de 2018

    def test_janelas_sao_contiguas_sem_sobreposicao_nem_buraco(self):
        janelas = _gerar_janelas_anuais(date(2015, 1, 1), date(2020, 12, 31))
        for (_, fim_atual), (proximo_inicio, _) in zip(janelas, janelas[1:]):
            assert (proximo_inicio - fim_atual).days == 1

    def test_primeira_e_ultima_janela_batem_com_intervalo_pedido(self):
        inicio, fim = date(2015, 3, 10), date(2019, 8, 20)
        janelas = _gerar_janelas_anuais(inicio, fim)
        assert janelas[0][0] == inicio
        assert janelas[-1][1] == fim


class TestFetchSerie:
    def _fake_response(self, status_code=200, payload=None):
        class FakeResp:
            def __init__(self):
                self.status_code = status_code
                self.url = "https://api.bcb.gov.br/fake"

            def json(self):
                return payload

        return FakeResp()

    @pytest.fixture(autouse=True)
    def _retry_sem_espera(self):
        """
        Remove o backoff exponencial durante os testes — sem isso, um
        teste que força uma falha ficaria esperando dezenas de
        segundos de verdade entre tentativas.
        """
        from tenacity import stop_after_attempt, wait_none

        from src.ingestion.bacen_api import _request_janela

        wait_original = _request_janela.retry.wait
        stop_original = _request_janela.retry.stop
        _request_janela.retry.wait = wait_none()
        _request_janela.retry.stop = stop_after_attempt(2)
        yield
        _request_janela.retry.wait = wait_original
        _request_janela.retry.stop = stop_original

    def test_agrega_registros_de_multiplas_janelas(self):
        respostas = [
            self._fake_response(payload=[{"data": "01/01/2015", "valor": "11,75"}]),
            self._fake_response(payload=[{"data": "01/01/2016", "valor": "14,25"}]),
        ]
        with patch("src.ingestion.bacen_api.requests.get", side_effect=respostas):
            registros = fetch_serie(11, "01/01/2015", "01/06/2016")
        assert len(registros) == 2

    def test_data_inicial_maior_que_data_final_levanta_erro(self):
        with pytest.raises(ValueError):
            fetch_serie(11, "01/01/2026", "01/01/2020")

    def test_http_status_diferente_de_200_levanta_bacen_api_error(self):
        resposta_erro = self._fake_response(status_code=500)
        with patch(
            "src.ingestion.bacen_api.requests.get",
            return_value=resposta_erro,
        ):
            with pytest.raises(BacenAPIError):
                fetch_serie(11, "01/01/2024", "01/02/2024")

    def test_resposta_com_formato_inesperado_levanta_erro(self):
        resposta_invalida = self._fake_response(payload={"nao_e_uma_lista": True})
        with patch(
            "src.ingestion.bacen_api.requests.get",
            return_value=resposta_invalida,
        ):
            with pytest.raises(BacenAPIError):
                fetch_serie(11, "01/01/2024", "01/02/2024")


class TestSaveRaw:
    def test_grava_no_path_particionado_esperado(self, tmp_path):
        registros = [{"data": "01/01/2024", "valor": "11,75"}]
        caminho = save_raw(
            registros,
            codigo=11,
            nome="selic_diaria",
            data_ingestao=date(2026, 7, 23),
            base_path=tmp_path,
        )
        assert caminho == tmp_path / "bronze" / "serie=11" / "data_ingestao=2026-07-23" / "raw.json"
        assert caminho.exists()

    def test_conteudo_gravado_inclui_metadata(self, tmp_path):
        import json

        registros = [{"data": "01/01/2024", "valor": "11,75"}]
        caminho = save_raw(
            registros,
            codigo=11,
            nome="selic_diaria",
            data_ingestao=date(2026, 7, 23),
            base_path=tmp_path,
        )
        conteudo = json.loads(caminho.read_text(encoding="utf-8"))
        assert conteudo["codigo"] == 11
        assert conteudo["nome"] == "selic_diaria"
        assert conteudo["quantidade_registros"] == 1
        assert conteudo["registros"] == registros
