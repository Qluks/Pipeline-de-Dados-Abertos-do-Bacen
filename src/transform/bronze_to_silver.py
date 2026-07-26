"""
Bronze -> Silver.

Lê os JSONs brutos da camada bronze, aplica schema explícito, corrige
o formato numérico brasileiro (vírgula decimal), converte tipos,
remove duplicatas (mantendo sempre o registro da ingestão mais
recente) e grava como tabela Delta.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.transform.schemas import BRONZE_SCHEMA

logger = logging.getLogger(__name__)

SILVER_PATH_DEFAULT = "data/silver/series"


def ler_bronze(spark: SparkSession, bronze_glob: str) -> DataFrame:
    """
    Lê todos os arquivos raw.json da camada bronze de uma vez, usando o
    schema explícito. bronze_glob normalmente é algo como
    'data/bronze/serie=*/data_ingestao=*/raw.json'.
    """
    df = spark.read.schema(BRONZE_SCHEMA).option("multiLine", "true").json(bronze_glob)
    return df


def bronze_para_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Aplica toda a transformação bronze -> silver:
      1. Explode o array de registros (1 linha por dia/série)
      2. Corrige vírgula decimal (padrão BR) antes de converter pra double
      3. Converte data de string (dd/MM/yyyy) pra tipo date
      4. Remove nulos essenciais (data ou valor ausente)
      5. Dedup por (codigo, data), mantendo o registro da ingestão mais recente
    """
    df = (
        df_bronze.select(
            F.col("codigo").alias("serie_codigo"),
            F.col("nome").alias("serie_nome"),
            F.col("data_ingestao"),
            F.explode("registros").alias("registro"),
        )
        .select(
            "serie_codigo",
            "serie_nome",
            "data_ingestao",
            F.to_date(F.col("registro.data"), "dd/MM/yyyy").alias("data"),
            F.regexp_replace(F.col("registro.valor"), ",", ".")
            .cast("double")
            .alias("valor"),
        )
    )

    # Nulos essenciais: sem data ou sem valor, o registro não serve pra nada
    antes = df.count()
    df = df.filter(F.col("data").isNotNull() & F.col("valor").isNotNull())
    depois = df.count()
    if antes != depois:
        logger.warning(
            "Descartados %d registro(s) com data ou valor nulo (de %d totais)",
            antes - depois,
            antes,
        )

    # Dedup: mesma série + mesma data pode aparecer em múltiplas ingestões
    # (janelas de datas se sobrepõem entre execuções diárias). Mantém
    # sempre a versão mais recente.
    janela = Window.partitionBy("serie_codigo", "data").orderBy(
        F.col("data_ingestao").desc()
    )
    df = (
        df.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "data_ingestao")
    )

    return df


def executar(
    spark: SparkSession,
    bronze_glob: str = "data/bronze/serie=*/data_ingestao=*/raw.json",
    silver_path: str = SILVER_PATH_DEFAULT,
) -> DataFrame:
    df_bronze = ler_bronze(spark, bronze_glob)
    df_silver = bronze_para_silver(df_bronze)

    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .partitionBy("serie_codigo")
        .save(silver_path)
    )

    total = df_silver.count()
    logger.info("Silver gravada em %s: %d registros", silver_path, total)
    return df_silver