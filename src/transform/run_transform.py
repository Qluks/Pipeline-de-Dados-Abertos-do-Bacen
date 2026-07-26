"""
Ponto de entrada da transformação: bronze -> silver -> gold.

Uso:
    python -m src.transform.run_transform
"""

from __future__ import annotations

import argparse
import logging

from src.transform.bronze_to_silver import SILVER_PATH_DEFAULT, executar as bronze_to_silver
from src.transform.silver_to_gold import (
    GOLD_COMPARATIVO_PATH_DEFAULT,
    GOLD_MENSAL_PATH_DEFAULT,
    executar as silver_to_gold,
)
from src.transform.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Transformação bronze -> silver -> gold")
    parser.add_argument("--bronze-glob", default="data/bronze/serie=*/data_ingestao=*/raw.json")
    parser.add_argument("--silver-path", default=SILVER_PATH_DEFAULT)
    parser.add_argument("--gold-mensal-path", default=GOLD_MENSAL_PATH_DEFAULT)
    parser.add_argument("--gold-comparativo-path", default=GOLD_COMPARATIVO_PATH_DEFAULT)
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        logger.info("Etapa bronze -> silver")
        bronze_to_silver(spark, bronze_glob=args.bronze_glob, silver_path=args.silver_path)

        logger.info("Etapa silver -> gold")
        silver_to_gold(
            spark,
            silver_path=args.silver_path,
            gold_mensal_path=args.gold_mensal_path,
            gold_comparativo_path=args.gold_comparativo_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()