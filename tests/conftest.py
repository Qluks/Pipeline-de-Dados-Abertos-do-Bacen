"""
Fixtures compartilhadas entre os testes.
"""

from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Mesmo motivo do src/transform/spark_session.py: no Windows não existe
# um executável "python3", que é o nome que o Spark tenta usar por padrão.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    SparkSession local de teste. scope='session' porque criar uma
    SparkSession é caro (alguns segundos) — reutilizar entre todos os
    testes do arquivo deixa a suíte inteira muito mais rápida.

    Sem Delta aqui de propósito: os testes de transformação testam
    a LÓGICA (DataFrames em memória), não a gravação em disco no
    formato Delta — isso evita depender de download de JARs do Maven
    toda vez que a suíte roda (ex: em CI).
    """
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("bacen-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()