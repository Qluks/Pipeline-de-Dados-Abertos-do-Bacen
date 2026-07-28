"""
Ponto de entrada da ingestão. Lê config/series.yaml e, para cada série
configurada, busca os dados na API do Bacen e salva na camada bronze.

Uso:
    python -m src.ingestion.run_ingestion
    python -m src.ingestion.run_ingestion --config config/series.yaml --data-dir data
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

from src.ingestion.bacen_api import BacenAPIError, fetch_serie, save_raw

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def carregar_config(config_path: str | Path) -> list[dict]:
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["series"]


def executar_ingestao(config_path: str | Path, data_dir: str | Path) -> int:
    """Executa a ingestão de todas as séries configuradas. Retorna a
    quantidade de séries que falharam (0 = sucesso total)."""
    series = carregar_config(config_path)
    falhas = 0

    for serie in series:
        codigo = serie["codigo"]
        nome = serie["nome"]
        data_inicial = serie["data_inicial"]
        data_final = serie.get("data_final")  # None = até hoje

        try:
            registros = fetch_serie(codigo, data_inicial, data_final)
            save_raw(registros, codigo=codigo, nome=nome, base_path=data_dir)
        except (BacenAPIError, Exception) as exc:  # noqa: BLE001
            # Uma série falhar não deve impedir as outras de rodar.
            logger.error("Falha ao ingerir série %s (%s): %s", codigo, nome, exc)
            falhas += 1

    logger.info(
        "Ingestão concluída: %d série(s) ok, %d falha(s)",
        len(series) - falhas,
        falhas,
    )
    return falhas


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestão de séries do Bacen (camada bronze)")
    parser.add_argument("--config", default="config/series.yaml", help="Caminho do YAML de séries")
    parser.add_argument("--data-dir", default="data", help="Diretório base de dados (bronze/silver/gold)")
    args = parser.parse_args()

    falhas = executar_ingestao(args.config, args.data_dir)
    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()
