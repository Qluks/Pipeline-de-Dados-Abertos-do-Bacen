"""
Leitura das tabelas gold pro dashboard.

Decisão importante: aqui usamos `deltalake` (delta-rs, biblioteca Rust
com bindings Python), NÃO PySpark. O pipeline de transformação usa
Spark porque precisa processar/agregar os dados; mas o dashboard só
PRECISA LER duas tabelas pequenas e já prontas. Subir uma JVM inteira
via Spark só pra isso adicionaria uns bons segundos de latência toda
vez que alguém abrisse o dashboard, sem nenhum benefício — delta-rs lê
o mesmo formato Delta Lake (é o mesmo protocolo aberto), sem precisar
de Java.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

GOLD_MENSAL_PATH = "data/gold/mensal_por_serie"
GOLD_COMPARATIVO_PATH = "data/gold/comparativo_mensal"


class GoldDataNotFoundError(Exception):
    """Levantado quando as tabelas gold ainda não foram geradas."""


def _ler_delta_como_pandas(path: str) -> pd.DataFrame:
    if not Path(path).exists():
        raise GoldDataNotFoundError(
            f"Tabela não encontrada em '{path}'. Rode o pipeline primeiro: "
            f"python -m src.transform.run_transform"
        )
    return DeltaTable(path).to_pandas()


@st.cache_data(ttl=600)
def carregar_mensal_por_serie(path: str = GOLD_MENSAL_PATH) -> pd.DataFrame:
    df = _ler_delta_como_pandas(path)
    df["periodo"] = pd.to_datetime(
        df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
    )
    return df.sort_values(["serie_nome", "periodo"])


@st.cache_data(ttl=600)
def carregar_comparativo_mensal(path: str = GOLD_COMPARATIVO_PATH) -> pd.DataFrame:
    df = _ler_delta_como_pandas(path)
    df["periodo"] = pd.to_datetime(
        df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
    )
    return df.sort_values("periodo")
