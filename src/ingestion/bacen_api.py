"""
Ingestão de séries temporais do SGS/Bacen.

Camada bronze: salva o payload bruto retornado pela API, sem nenhuma
transformação, particionado por série e data de ingestão. Isso garante
um dado imutável e auditável para reprocessamento futuro.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"

# A API do SGS não documenta um limite oficial de linhas por request,
# mas na prática requests com janelas muito longas (décadas) podem
# falhar ou demorar demais. Quebrar por ano é uma forma simples e
# segura de evitar esse problema, independente da série.
CHUNK_BY_YEARS = 1


@dataclass(frozen=True)
class SerieConfig:
    codigo: int
    nome: str
    data_inicial: str  # formato DD/MM/AAAA
    data_final: str | None = None  # None = até hoje


class BacenAPIError(Exception):
    """Erro ao consultar a API do SGS/Bacen."""


def _parse_br_date(data_str: str) -> date:
    return datetime.strptime(data_str, "%d/%m/%Y").date()


def _format_br_date(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def _gerar_janelas_anuais(data_inicial: date, data_final: date) -> list[tuple[date, date]]:
    """Quebra o intervalo total em janelas de até CHUNK_BY_YEARS ano(s)."""
    janelas = []
    inicio_janela = data_inicial
    while inicio_janela <= data_final:
        # Fim da janela: início + N anos - 1 dia, limitado pela data_final real
        try:
            fim_janela = inicio_janela.replace(year=inicio_janela.year + CHUNK_BY_YEARS)
        except ValueError:
            # 29/02 em ano não-bissexto
            fim_janela = inicio_janela.replace(
                year=inicio_janela.year + CHUNK_BY_YEARS, day=28
            )
        fim_janela = min(fim_janela, data_final)
        janelas.append((inicio_janela, fim_janela))
        inicio_janela = date.fromordinal(fim_janela.toordinal() + 1)
    return janelas


@retry(
    retry=retry_if_exception_type((requests.RequestException, BacenAPIError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _request_janela(codigo: int, data_inicial: date, data_final: date) -> list[dict]:
    """Faz uma única requisição HTTP para uma janela de datas, com retry/backoff."""
    url = BASE_URL.format(codigo=codigo)
    params = {
        "formato": "json",
        "dataInicial": _format_br_date(data_inicial),
        "dataFinal": _format_br_date(data_final),
    }

    logger.info("Requisitando série %s: %s a %s", codigo, params["dataInicial"], params["dataFinal"])
    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code != 200:
        raise BacenAPIError(
            f"Série {codigo}: HTTP {resp.status_code} ao consultar {resp.url}"
        )

    try:
        payload = resp.json()
    except ValueError as exc:
        raise BacenAPIError(f"Série {codigo}: resposta não é JSON válido") from exc

    if not isinstance(payload, list):
        raise BacenAPIError(f"Série {codigo}: formato de resposta inesperado: {type(payload)}")

    return payload


def fetch_serie(codigo: int, data_inicial: str, data_final: str | None = None) -> list[dict]:
    """
    Busca todos os registros de uma série do SGS/Bacen entre duas datas,
    quebrando o intervalo em janelas anuais para evitar respostas
    excessivamente grandes.

    Retorna a lista completa de registros brutos (cada um como
    {"data": "DD/MM/AAAA", "valor": "..."}), na ordem retornada pela API.
    """
    dt_inicial = _parse_br_date(data_inicial)
    dt_final = _parse_br_date(data_final) if data_final else date.today()

    if dt_inicial > dt_final:
        raise ValueError(f"data_inicial ({data_inicial}) é posterior a data_final")

    registros: list[dict] = []
    for inicio_janela, fim_janela in _gerar_janelas_anuais(dt_inicial, dt_final):
        registros.extend(_request_janela(codigo, inicio_janela, fim_janela))

    logger.info("Série %s: %d registros coletados no total", codigo, len(registros))
    return registros


def save_raw(
    registros: list[dict],
    codigo: int,
    nome: str,
    data_ingestao: date | None = None,
    base_path: str | Path = "data",
) -> Path:
    """
    Salva o payload bruto em disco, particionado por série e data de
    ingestão — camada bronze do medallion architecture.

    Path resultante: {base_path}/bronze/serie={codigo}/data_ingestao={AAAA-MM-DD}/raw.json
    """
    data_ingestao = data_ingestao or date.today()
    partition_dir = (
        Path(base_path)
        / "bronze"
        / f"serie={codigo}"
        / f"data_ingestao={data_ingestao.isoformat()}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    output_path = partition_dir / "raw.json"
    payload_com_metadata = {
        "codigo": codigo,
        "nome": nome,
        "data_ingestao": data_ingestao.isoformat(),
        "quantidade_registros": len(registros),
        "registros": registros,
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload_com_metadata, f, ensure_ascii=False, indent=2)

    logger.info("Salvo: %s (%d registros)", output_path, len(registros))
    return output_path
