"""
Silver -> Gold.

Aqui entra a "modelagem de negócio": duas tabelas gold, pensadas pra
serem consumidas direto pelo dashboard, sem transformação adicional.

  1. mensal_por_serie: agregação mensal por série, com variação
     percentual mês a mês.
  2. comparativo_mensal: as séries lado a lado (uma coluna por série),
     por mês — pra comparar por exemplo SELIC vs IPCA no mesmo período.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

GOLD_MENSAL_PATH_DEFAULT = "data/gold/mensal_por_serie"
GOLD_COMPARATIVO_PATH_DEFAULT = "data/gold/comparativo_mensal"


def construir_mensal_por_serie(df_silver: DataFrame) -> DataFrame:
    """Agrega a silver em granularidade mensal e calcula variação % mês a mês."""
    mensal = df_silver.groupBy(
        "serie_codigo",
        "serie_nome",
        F.year("data").alias("ano"),
        F.month("data").alias("mes"),
    ).agg(
        F.avg("valor").alias("valor_medio"),
        F.min("valor").alias("valor_min"),
        F.max("valor").alias("valor_max"),
        F.count("valor").alias("qtd_observacoes"),
    )

    janela = Window.partitionBy("serie_codigo").orderBy("ano", "mes")
    mensal = mensal.withColumn(
        "valor_mes_anterior", F.lag("valor_medio").over(janela)
    ).withColumn(
        "variacao_percentual_mensal",
        F.round(
            (F.col("valor_medio") - F.col("valor_mes_anterior"))
            / F.col("valor_mes_anterior")
            * 100,
            2,
        ),
    ).drop("valor_mes_anterior")

    return mensal.orderBy("serie_codigo", "ano", "mes")


def construir_comparativo_mensal(df_mensal: DataFrame) -> DataFrame:
    """
    Pivota as séries em colunas, uma linha por (ano, mes) — permite
    comparar diretamente séries diferentes (ex: SELIC vs IPCA) no
    mesmo período, sem precisar de join manual.
    """
    comparativo = (
        df_mensal.groupBy("ano", "mes")
        .pivot("serie_nome")
        .agg(F.first("valor_medio"))
        .orderBy("ano", "mes")
    )
    return comparativo


def executar(
    spark: SparkSession,
    silver_path: str,
    gold_mensal_path: str = GOLD_MENSAL_PATH_DEFAULT,
    gold_comparativo_path: str = GOLD_COMPARATIVO_PATH_DEFAULT,
) -> tuple[DataFrame, DataFrame]:
    df_silver = spark.read.format("delta").load(silver_path)

    df_mensal = construir_mensal_por_serie(df_silver)
    df_mensal.write.format("delta").mode("overwrite").save(gold_mensal_path)
    logger.info("Gold mensal_por_serie gravada em %s: %d linhas", gold_mensal_path, df_mensal.count())

    df_comparativo = construir_comparativo_mensal(df_mensal)
    df_comparativo.write.format("delta").mode("overwrite").save(gold_comparativo_path)
    logger.info(
        "Gold comparativo_mensal gravada em %s: %d linhas",
        gold_comparativo_path,
        df_comparativo.count(),
    )

    return df_mensal, df_comparativo