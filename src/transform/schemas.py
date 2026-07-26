"""
Schema explícito do payload bronze. Definir isso explicitamente (em vez
de deixar o Spark inferir com inferSchema) evita que uma mudança sutil
no formato da API quebre o pipeline silenciosamente mais adiante —
qualquer incompatibilidade aparece aqui, na leitura, de forma clara.
"""

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("codigo", IntegerType(), nullable=False),
        StructField("nome", StringType(), nullable=False),
        StructField("data_ingestao", StringType(), nullable=False),
        StructField("quantidade_registros", IntegerType(), nullable=True),
        StructField(
            "registros",
            ArrayType(
                StructType(
                    [
                        StructField("data", StringType(), nullable=True),
                        StructField("valor", StringType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)