"""
Dashboard do pipeline Bacen — consome direto a camada gold.

Rodar com:
    streamlit run src/dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# O Streamlit roda este arquivo diretamente (não como `python -m`), então
# a raiz do projeto não entra sozinha no import path do Python. Sem isso,
# `from src.dashboard...` falha com ModuleNotFoundError.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.data_loader import (
    GoldDataNotFoundError,
    carregar_comparativo_mensal,
    carregar_mensal_por_serie,
)

st.set_page_config(page_title="Indicadores do Bacen", layout="wide")
st.title("📊 Indicadores Econômicos do Banco Central")
st.caption("Dados públicos do SGS/Bacen — SELIC, câmbio, IPCA e outros indicadores")

try:
    df_mensal = carregar_mensal_por_serie()
    df_comparativo = carregar_comparativo_mensal()
except GoldDataNotFoundError as exc:
    st.error(str(exc))
    st.stop()

# --- Filtros (sidebar) ---
series_disponiveis = sorted(df_mensal["serie_nome"].unique())
series_selecionadas = st.sidebar.multiselect(
    "Séries", options=series_disponiveis, default=series_disponiveis
)

periodo_min, periodo_max = df_mensal["periodo"].min(), df_mensal["periodo"].max()
intervalo = st.sidebar.slider(
    "Período",
    min_value=periodo_min.to_pydatetime(),
    max_value=periodo_max.to_pydatetime(),
    value=(periodo_min.to_pydatetime(), periodo_max.to_pydatetime()),
    format="MM/YYYY",
)

if not series_selecionadas:
    st.warning("Selecione ao menos uma série na barra lateral.")
    st.stop()

df_filtrado = df_mensal[
    df_mensal["serie_nome"].isin(series_selecionadas)
    & df_mensal["periodo"].between(intervalo[0], intervalo[1])
]

# --- KPIs: último valor de cada série selecionada ---
colunas_kpi = st.columns(len(series_selecionadas))
for col, serie in zip(colunas_kpi, series_selecionadas):
    linha_recente = df_filtrado[df_filtrado["serie_nome"] == serie].sort_values("periodo").iloc[-1]
    variacao = linha_recente["variacao_percentual_mensal"]
    col.metric(
        label=serie.replace("_", " ").title(),
        value=f"{linha_recente['valor_medio']:.2f}",
        delta=f"{variacao:.2f}%" if pd.notna(variacao) else None,
    )

# --- Gráfico de série temporal ---
st.subheader("Evolução mensal")
fig_serie = px.line(
    df_filtrado,
    x="periodo",
    y="valor_medio",
    color="serie_nome",
    markers=True,
    labels={"periodo": "Período", "valor_medio": "Valor médio", "serie_nome": "Série"},
)
st.plotly_chart(fig_serie, width='stretch')

# --- Comparativo lado a lado ---
st.subheader("Comparativo entre indicadores")
colunas_disponiveis = [c for c in series_selecionadas if c in df_comparativo.columns]
if colunas_disponiveis:
    df_comp_filtrado = df_comparativo[
        df_comparativo["periodo"].between(intervalo[0], intervalo[1])
    ]
    fig_comparativo = px.line(
        df_comp_filtrado,
        x="periodo",
        y=colunas_disponiveis,
        markers=True,
        labels={"periodo": "Período", "value": "Valor", "variable": "Série"},
    )
    st.plotly_chart(fig_comparativo, width='stretch')

# --- Tabela detalhada ---
with st.expander("Ver dados em tabela"):
    st.dataframe(
        df_filtrado[
            ["serie_nome", "periodo", "valor_medio", "valor_min", "valor_max", "variacao_percentual_mensal"]
        ].sort_values(["serie_nome", "periodo"]),
        width='stretch',
    )