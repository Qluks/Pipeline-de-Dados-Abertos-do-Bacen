"""
Testes do módulo de qualidade (src/quality/expectations.py).

Usa a fixture 'spark' do conftest.py só pra construir os DataFrames de
entrada — a validação em si roda em pandas por baixo dos panos.
"""

from __future__ import annotations

from datetime import date

import pytest

from src.quality.expectations import DataQualityError, validar_ou_falhar, validar_silver


def _silver_df(spark, dados):
    return spark.createDataFrame(dados, schema=["serie_codigo", "serie_nome", "data", "valor"])


class TestValidarSilver:
    def test_dado_limpo_passa_em_todas_as_checagens(self, spark):
        df = _silver_df(
            spark,
            [
                (11, "selic_diaria", date(2024, 1, 1), 11.75),
                (11, "selic_diaria", date(2024, 1, 2), 11.75),
                (433, "ipca_mensal", date(2024, 1, 1), 0.42),
            ],
        )
        relatorio = validar_silver(df)
        assert relatorio["success"] is True
        assert relatorio["falhas"] == []

    def test_valor_fora_do_range_da_serie_falha(self, spark):
        # SELIC de 350% é claramente um erro de dado (bug de unidade, por exemplo)
        df = _silver_df(spark, [(11, "selic_diaria", date(2024, 1, 1), 350.0)])
        relatorio = validar_silver(df)
        assert relatorio["success"] is False
        expectativas_falhas = [f["expectativa"] for f in relatorio["falhas"]]
        assert "expect_column_values_to_be_between" in expectativas_falhas

    def test_range_e_especifico_por_serie(self, spark):
        # 0.42 é um valor normal de IPCA mensal, mas bem fora do range
        # esperado pra SELIC diária -- o teste confirma que o range
        # aplicado é o da série certa, não um range genérico único.
        df = _silver_df(
            spark,
            [
                (433, "ipca_mensal", date(2024, 1, 1), 0.42),  # dentro do range do IPCA
                (11, "selic_diaria", date(2024, 1, 1), 11.75),  # dentro do range da SELIC
            ],
        )
        relatorio = validar_silver(df)
        assert relatorio["success"] is True

    def test_duplicata_de_serie_e_data_falha(self, spark):
        df = _silver_df(
            spark,
            [
                (11, "selic_diaria", date(2024, 1, 1), 11.75),
                (11, "selic_diaria", date(2024, 1, 1), 11.90),  # mesma série+data duas vezes
            ],
        )
        relatorio = validar_silver(df)
        assert relatorio["success"] is False
        expectativas_falhas = [f["expectativa"] for f in relatorio["falhas"]]
        assert "expect_compound_columns_to_be_unique" in expectativas_falhas

    def test_serie_desconhecida_usa_range_default_amplo(self, spark):
        # série que não está em RANGES_POR_SERIE não deveria falhar por
        # causa de um range apertado demais que não faz sentido pra ela
        df = _silver_df(spark, [(9999, "serie_nao_mapeada", date(2024, 1, 1), 42_000.0)])
        relatorio = validar_silver(df)
        assert relatorio["success"] is True


class TestValidarOuFalhar:
    def test_nao_levanta_excecao_quando_dado_e_valido(self, spark):
        df = _silver_df(spark, [(11, "selic_diaria", date(2024, 1, 1), 11.75)])
        validar_ou_falhar(df)  # não deve levantar

    def test_levanta_data_quality_error_quando_dado_e_invalido(self, spark):
        df = _silver_df(spark, [(11, "selic_diaria", date(2024, 1, 1), 350.0)])
        with pytest.raises(DataQualityError):
            validar_ou_falhar(df)
