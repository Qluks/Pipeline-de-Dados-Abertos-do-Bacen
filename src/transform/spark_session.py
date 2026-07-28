"""
SparkSession local com suporte a Delta Lake — sem cluster, sem cloud.
"""

from __future__ import annotations

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# No Windows, o executável se chama "python.exe" (não "python3"), que é
# o nome que o Spark tenta usar por padrão pra subir os processos worker.
# Forçar explicitamente pro mesmo interpretador que já está rodando
# evita o erro "Cannot run program 'python3'" — funciona em qualquer OS.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark_session(app_name: str = "bacen-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        # Reduz verbosidade de log no console — mais fácil de debugar
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
