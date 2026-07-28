"""
Validação de qualidade de dados da camada silver, usando Great
Expectations. Roda como um "checkpoint" antes da promoção silver ->
gold: se falhar, o pipeline para e não deixa dado inválido virar gold.

Os ranges por série são propositalmente específicos (não um range
genérico gigante) — isso é o que torna a checagem útil de verdade:
uma SELIC de 350% é um bug óbvio, mas só pega se o range for realista
pra essa série específica.
"""

from __future__ import annotations

import logging
import uuid

import great_expectations as gx
from pyspark.sql import DataFrame as SparkDataFrame

logger = logging.getLogger(__name__)

# (valor_minimo, valor_maximo) esperado por série. Séries não listadas
# caem no DEFAULT_RANGE — mais largo, só pra pegar erros grosseiros
# (ex: um valor 1000x maior por engano de unidade).
RANGES_POR_SERIE = {
    "selic_diaria": (0, 100),
    "taxa_media_juros_credito": (0, 200),
    "cambio_usd_venda": (0, 20),
    "ipca_mensal": (-5, 5),
}
DEFAULT_RANGE = (-1_000_000, 1_000_000)


class DataQualityError(Exception):
    """Levantado quando a camada silver falha na validação de qualidade."""


def _novo_contexto_e_batch(df_pandas, nome_base: str):
    """
    Cria um contexto GX efêmero (em memória, sem persistir nada em
    disco) e um batch a partir de um DataFrame pandas. O sufixo
    aleatório evita colisão de nomes entre chamadas na mesma sessão
    (ex: rodando os testes várias vezes).
    """
    sufixo = uuid.uuid4().hex[:8]
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(f"{nome_base}_{sufixo}")
    data_asset = data_source.add_dataframe_asset(name=f"asset_{sufixo}")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"batch_{sufixo}")
    return batch_definition.get_batch(batch_parameters={"dataframe": df_pandas})


def validar_silver(df_silver: SparkDataFrame) -> dict:
    """
    Valida a camada silver e retorna um relatório:
        {"success": bool, "falhas": [ {expectativa, escopo, detalhe}, ... ]}

    Levanta DataQualityError se success=False — o chamador (run_transform)
    decide o que fazer, mas por padrão o pipeline para aqui.
    """
    df_pandas = df_silver.toPandas()
    falhas: list[dict] = []

    # --- Checagens globais (aplicam a todas as séries) ---
    batch_global = _novo_contexto_e_batch(df_pandas, "silver_global")
    suite_global = gx.ExpectationSuite(name="silver_global")
    suite_global.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="data"))
    suite_global.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="valor"))
    suite_global.add_expectation(
        gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["serie_codigo", "data"])
    )
    resultado_global = batch_global.validate(suite_global)
    for r in resultado_global.results:
        if not r.success:
            falhas.append(
                {
                    "expectativa": r.expectation_config.type,
                    "escopo": "global",
                    "unexpected_count": r.result.get("unexpected_count"),
                }
            )

    # --- Checagem de range, específica por série ---
    for serie_nome, grupo in df_pandas.groupby("serie_nome"):
        minimo, maximo = RANGES_POR_SERIE.get(serie_nome, DEFAULT_RANGE)
        batch_serie = _novo_contexto_e_batch(grupo, f"silver_{serie_nome}")
        suite_serie = gx.ExpectationSuite(name=f"silver_{serie_nome}")
        suite_serie.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="valor", min_value=minimo, max_value=maximo
            )
        )
        resultado_serie = batch_serie.validate(suite_serie)
        for r in resultado_serie.results:
            if not r.success:
                falhas.append(
                    {
                        "expectativa": r.expectation_config.type,
                        "escopo": serie_nome,
                        "unexpected_count": r.result.get("unexpected_count"),
                    }
                )

    sucesso = len(falhas) == 0

    if sucesso:
        logger.info("Validação de qualidade OK: %d linha(s) verificadas", len(df_pandas))
    else:
        for f in falhas:
            logger.error(
                "Falha de qualidade [%s] em '%s': %d registro(s) fora do esperado",
                f["expectativa"],
                f["escopo"],
                f["unexpected_count"],
            )

    return {"success": sucesso, "falhas": falhas}


def validar_ou_falhar(df_silver: SparkDataFrame) -> None:
    """Levanta DataQualityError se a validação falhar — usado pra barrar
    a promoção silver -> gold quando o dado não passa nas checagens."""
    relatorio = validar_silver(df_silver)
    if not relatorio["success"]:
        raise DataQualityError(
            f"Validação de qualidade falhou com {len(relatorio['falhas'])} problema(s): "
            f"{relatorio['falhas']}"
        )
