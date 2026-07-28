"""
Testes das transformações (src/transform/).

Usa uma SparkSession local (fixture 'spark' do conftest.py) e
DataFrames construídos na mão — não depende dos arquivos reais da
camada bronze, nem do formato Delta (testa a lógica, não a gravação
em disco).
"""

from __future__ import annotations

from src.transform.bronze_to_silver import bronze_para_silver
from src.transform.schemas import BRONZE_SCHEMA
from src.transform.silver_to_gold import (
    construir_comparativo_mensal,
    construir_mensal_por_serie,
)


def _bronze_df(spark, registros_por_arquivo):
    """Helper: constrói um DataFrame bronze a partir de uma lista de
    dicts no mesmo formato dos arquivos raw.json."""
    return spark.createDataFrame(registros_por_arquivo, schema=BRONZE_SCHEMA)


class TestBronzeParaSilver:
    def test_converte_virgula_decimal_para_double(self, spark):
        bronze = _bronze_df(
            spark,
            [
                {
                    "codigo": 11,
                    "nome": "selic_diaria",
                    "data_ingestao": "2026-07-23",
                    "quantidade_registros": 1,
                    "registros": [{"data": "01/01/2024", "valor": "11,75"}],
                }
            ],
        )
        silver = bronze_para_silver(bronze)
        valor = silver.collect()[0]["valor"]
        assert valor == 11.75

    def test_dedup_mantem_ingestao_mais_recente(self, spark):
        bronze = _bronze_df(
            spark,
            [
                {
                    "codigo": 11,
                    "nome": "selic_diaria",
                    "data_ingestao": "2026-07-20",
                    "quantidade_registros": 1,
                    "registros": [{"data": "01/01/2024", "valor": "11,75"}],
                },
                {
                    "codigo": 11,
                    "nome": "selic_diaria",
                    "data_ingestao": "2026-07-23",
                    "quantidade_registros": 1,
                    "registros": [{"data": "01/01/2024", "valor": "11,90"}],
                },
            ],
        )
        silver = bronze_para_silver(bronze)
        linhas = silver.collect()
        assert len(linhas) == 1
        assert linhas[0]["valor"] == 11.90

    def test_descarta_registros_com_valor_nulo(self, spark):
        bronze = _bronze_df(
            spark,
            [
                {
                    "codigo": 11,
                    "nome": "selic_diaria",
                    "data_ingestao": "2026-07-23",
                    "quantidade_registros": 2,
                    "registros": [
                        {"data": "01/01/2024", "valor": "11,75"},
                        {"data": "02/01/2024", "valor": None},
                    ],
                }
            ],
        )
        silver = bronze_para_silver(bronze)
        assert silver.count() == 1

    def test_series_diferentes_nao_se_misturam_no_dedup(self, spark):
        bronze = _bronze_df(
            spark,
            [
                {
                    "codigo": 11,
                    "nome": "selic_diaria",
                    "data_ingestao": "2026-07-23",
                    "quantidade_registros": 1,
                    "registros": [{"data": "01/01/2024", "valor": "11,75"}],
                },
                {
                    "codigo": 433,
                    "nome": "ipca_mensal",
                    "data_ingestao": "2026-07-23",
                    "quantidade_registros": 1,
                    "registros": [{"data": "01/01/2024", "valor": "0,42"}],
                },
            ],
        )
        silver = bronze_para_silver(bronze)
        # mesma data, séries diferentes -> ambas devem sobreviver ao dedup
        assert silver.count() == 2


class TestSilverParaGold:
    def _silver_df(self, spark):
        from datetime import date

        dados = [
            (11, "selic_diaria", date(2024, 1, 1), 11.75),
            (11, "selic_diaria", date(2024, 1, 15), 11.75),
            (11, "selic_diaria", date(2024, 2, 1), 11.25),
            (433, "ipca_mensal", date(2024, 1, 1), 0.42),
            (433, "ipca_mensal", date(2024, 2, 1), 0.83),
        ]
        return spark.createDataFrame(
            dados, schema=["serie_codigo", "serie_nome", "data", "valor"]
        )

    def test_agregacao_mensal_calcula_media_correta(self, spark):
        silver = self._silver_df(spark)
        mensal = construir_mensal_por_serie(silver)
        selic_jan = (
            mensal.filter("serie_codigo = 11 AND ano = 2024 AND mes = 1")
            .collect()[0]
        )
        assert selic_jan["valor_medio"] == 11.75
        assert selic_jan["qtd_observacoes"] == 2

    def test_variacao_percentual_calculada_entre_meses_consecutivos(self, spark):
        silver = self._silver_df(spark)
        mensal = construir_mensal_por_serie(silver)
        selic_fev = (
            mensal.filter("serie_codigo = 11 AND ano = 2024 AND mes = 2")
            .collect()[0]
        )
        # (11.25 - 11.75) / 11.75 * 100 ≈ -4.26
        assert selic_fev["variacao_percentual_mensal"] == -4.26

    def test_primeiro_mes_de_uma_serie_nao_tem_variacao(self, spark):
        silver = self._silver_df(spark)
        mensal = construir_mensal_por_serie(silver)
        selic_jan = (
            mensal.filter("serie_codigo = 11 AND ano = 2024 AND mes = 1")
            .collect()[0]
        )
        assert selic_jan["variacao_percentual_mensal"] is None

    def test_comparativo_mensal_pivota_series_em_colunas(self, spark):
        silver = self._silver_df(spark)
        mensal = construir_mensal_por_serie(silver)
        comparativo = construir_comparativo_mensal(mensal)
        assert "selic_diaria" in comparativo.columns
        assert "ipca_mensal" in comparativo.columns

        jan = comparativo.filter("ano = 2024 AND mes = 1").collect()[0]
        assert jan["selic_diaria"] == 11.75
        assert jan["ipca_mensal"] == 0.42
