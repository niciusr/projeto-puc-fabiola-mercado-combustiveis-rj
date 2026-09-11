# Databricks notebook source
"""Cria o perfil por atributo e executa regras de qualidade do projeto."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, Row, functions as F


# COMMAND ----------

dbutils.widgets.text("database", "workspace.anp_combustiveis_br_2022_2024", "Database")
dbutils.widgets.text(
    "config_root",
    "/Volumes/workspace/anp_rj_2022_2024/anp/config",
    "Pasta config no Volume",
)
dbutils.widgets.dropdown("perfil_completo", "true", ["true", "false"], "Perfil completo")

DATABASE = dbutils.widgets.get("database").strip()
CONFIG_ROOT = dbutils.widgets.get("config_root").rstrip("/")
PERFIL_COMPLETO = dbutils.widgets.get("perfil_completo") == "true"
EXECUTED_AT = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def table(name: str) -> str:
    return f"{DATABASE}.{name}"


def write_delta(name: str, frame: DataFrame) -> None:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table(name))


PROFILE_TABLES = [
    "silver_preco_coletado",
    "silver_venda_municipio",
    "silver_venda_empresa_uf_mes",
    "silver_cadastro_revenda",
    "gold_dim_tempo",
    "gold_dim_uf",
    "gold_dim_municipio",
    "gold_dim_produto",
    "gold_dim_empresa_vendedora",
    "gold_dim_revenda",
    "gold_dim_bandeira",
    "gold_dim_lote_carga",
    "gold_ref_conciliacao_municipio",
    "gold_ref_mapeamento_produto",
    "gold_ref_empresa_grupo",
    "gold_fato_preco_coletado",
    "gold_fato_preco_municipio_semana",
    "gold_fato_preco_municipio_anual",
    "gold_fato_preco_uf_mes",
    "gold_fato_venda_municipio_anual",
    "gold_fato_venda_empresa_uf_mes",
    "gold_fato_participacao_vendedor_uf_mes",
    "gold_fato_concentracao_uf_mes",
    "gold_mart_mercado_uf_mes",
    "gold_fato_mercado_municipio_anual",
    "gold_fato_cadastro_revenda_snapshot",
    "gold_fato_rede_bandeira_uf_snapshot",
    "gold_cobertura_pesquisa",
    "gold_preco_outlier",
    "gold_reconciliacao_volume_uf_ano",
    "gold_reconciliacao_etapas",
]

DISTINCT_BATCH_SIZE = 8


def value_expression(column_name: str):
    raw = F.col(column_name)
    return F.when(raw.isNull() | (F.trim(raw.cast("string")) == ""), F.lit(None)).otherwise(raw)


def top_categories(frame: DataFrame, column_name: str) -> str | None:
    values = value_expression(column_name).cast("string")
    rows = (
        frame.select(values.alias("valor"))
        .where(F.col("valor").isNotNull())
        .groupBy("valor")
        .count()
        .orderBy(F.desc("count"), F.asc("valor"))
        .limit(5)
        .collect()
    )
    if not rows:
        return None
    return " | ".join(f"{row['valor']} ({row['count']})" for row in rows)


def profile_metrics(frame: DataFrame, column_names: list[str]) -> tuple[Row, dict[str, int]]:
    expressions = [F.count(F.lit(1)).alias("total_registros")]
    for index, column_name in enumerate(column_names):
        value = value_expression(column_name)
        expressions.extend(
            [
                F.sum(F.when(value.isNull(), F.lit(1)).otherwise(F.lit(0))).alias(f"nulos_{index}"),
                F.min(value).alias(f"minimo_{index}"),
                F.max(value).alias(f"maximo_{index}"),
            ]
        )

    metrics = frame.agg(*expressions).first()
    distinct_counts: dict[str, int] = {}
    for start in range(0, len(column_names), DISTINCT_BATCH_SIZE):
        batch = column_names[start : start + DISTINCT_BATCH_SIZE]
        distinct = frame.agg(
            *[
                F.countDistinct(value_expression(column_name)).alias(f"distintos_{index}")
                for index, column_name in enumerate(batch, start)
            ]
        ).first()
        distinct_counts.update(
            {
                column_name: int(distinct[f"distintos_{index}"] or 0)
                for index, column_name in enumerate(batch, start)
            }
        )
    return metrics, distinct_counts


def profile_table(table_name: str) -> list[dict[str, object]]:
    frame = spark.table(table(table_name))
    if not PERFIL_COMPLETO:
        frame = frame.limit(100_000)

    profiles: list[dict[str, object]] = []
    schema = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
    column_names = list(schema)
    metrics, distinct_counts = profile_metrics(frame, column_names)
    total = int(metrics["total_registros"] or 0)

    for index, (column_name, spark_type) in enumerate(schema.items()):
        nulls = int(metrics[f"nulos_{index}"] or 0)
        profiles.append(
            {
                "tabela": table_name,
                "coluna": column_name,
                "tipo_spark": spark_type,
                "total_registros": total,
                "qtd_nulos": nulls,
                "pct_nulos": round((nulls / total * 100) if total else 0, 4),
                "qtd_distintos": distinct_counts[column_name],
                "min_observado": (
                    None if metrics[f"minimo_{index}"] is None else str(metrics[f"minimo_{index}"])
                ),
                "max_observado": (
                    None if metrics[f"maximo_{index}"] is None else str(metrics[f"maximo_{index}"])
                ),
                "categorias_frequentes": top_categories(frame, column_name)
                if spark_type.startswith(("string", "boolean"))
                else None,
                "modo_perfil": "completo" if PERFIL_COMPLETO else "amostra_100000",
                "data_execucao": EXECUTED_AT,
            }
        )
    return profiles


def result(
    rule_id: str,
    table_name: str,
    description: str,
    affected: int,
    total: int,
    status_if_affected: str = "ATENCAO",
) -> Row:
    return Row(
        regra_id=rule_id,
        tabela=table_name,
        descricao=description,
        total_avaliado=total,
        qtd_afetada=affected,
        pct_afetada=round((affected / total * 100) if total else 0, 4),
        status="APROVADA" if affected == 0 else status_if_affected,
        data_execucao=EXECUTED_AT,
    )


def count_rule(
    table_name: str,
    rule_id: str,
    description: str,
    condition,
    status_if_affected: str = "ATENCAO",
) -> Row:
    frame = spark.table(table(table_name))
    counts = frame.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).alias("afetados"),
    ).first()
    return result(
        rule_id,
        table_name,
        description,
        int(counts["afetados"] or 0),
        int(counts["total"] or 0),
        status_if_affected,
    )


# COMMAND ----------

profile_rows: list[dict[str, object]] = []
for name in PROFILE_TABLES:
    profile_rows.extend(profile_table(name))

catalogo_ref = (
    spark.read.option("header", True)
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .csv(f"{CONFIG_ROOT}/catalogo_atributos.csv")
)

qualidade = [
    count_rule(
        "silver_preco_coletado",
        "PRECO_001",
        "Preço de venda ausente, zero ou negativo.",
        ~F.col("preco_valido"),
    ),
    count_rule(
        "silver_preco_coletado",
        "PRECO_002",
        "CNPJ ausente ou fora do formato de 14 dígitos.",
        ~F.col("cnpj_formato_valido"),
    ),
    count_rule(
        "silver_preco_coletado",
        "PRECO_003",
        "Município de preço sem conciliação com o código IBGE.",
        ~F.col("municipio_conciliado"),
    ),
    count_rule(
        "silver_preco_coletado",
        "PRECO_004",
        "Produto de preço sem regra de mapeamento.",
        F.col("produto_analitico") == "NAO_MAPEADO",
    ),
    count_rule(
        "silver_preco_coletado",
        "PRECO_005",
        "Repetição da chave CNPJ, produto e data de coleta.",
        F.col("duplicidade_chave"),
        "INFORMATIVA",
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_MUNICIPIO_001",
        "Volume municipal ausente ou negativo.",
        ~F.col("volume_valido"),
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_MUNICIPIO_002",
        "Código IBGE fora do padrão de sete dígitos.",
        ~F.col("codigo_ibge").rlike(r"^\d{7}$"),
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_MUNICIPIO_003",
        "Produto municipal sem regra de mapeamento.",
        F.col("produto_analitico") == "NAO_MAPEADO",
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_MUNICIPIO_004",
        "Repetição da chave código IBGE, produto e ano.",
        F.col("duplicidade_chave"),
    ),
    count_rule(
        "silver_venda_empresa_uf_mes",
        "LOGISTICA_001",
        "Período mensal ausente ou inválido.",
        F.col("data_referencia").isNull(),
    ),
    count_rule(
        "silver_venda_empresa_uf_mes",
        "LOGISTICA_002",
        "UF de destino fora do domínio de siglas estaduais.",
        ~F.col("uf_destino_valida"),
        "INFORMATIVA",
    ),
    count_rule(
        "silver_venda_empresa_uf_mes",
        "LOGISTICA_003",
        "Vendedor ausente após normalização.",
        ~F.col("vendedor_preenchido"),
    ),
    count_rule(
        "silver_venda_empresa_uf_mes",
        "LOGISTICA_004",
        "Quantidade líquida ausente ou inválida.",
        ~F.col("volume_valido"),
    ),
    count_rule(
        "silver_venda_empresa_uf_mes",
        "LOGISTICA_005",
        "Produto principal sem regra de mapeamento.",
        F.col("produto_origem_normalizado").isin("GASOLINA C COMUM", "ETANOL HIDRATADO COMUM")
        & (F.col("produto_analitico") == "NAO_MAPEADO"),
    ),
    count_rule(
        "silver_venda_empresa_uf_mes",
        "LOGISTICA_006",
        "Ajuste líquido negativo preservado para auditoria.",
        F.col("ajuste_negativo"),
        "INFORMATIVA",
    ),
    count_rule(
        "silver_venda_empresa_uf_mes",
        "LOGISTICA_007",
        "Repetição da chave mês, UF, produto e vendedor.",
        F.col("duplicidade_chave"),
    ),
    count_rule(
        "gold_fato_participacao_vendedor_uf_mes",
        "PARTICIPACAO_001",
        "Participação fora do intervalo de 0% a 100%.",
        (F.col("participacao_pct") < 0) | (F.col("participacao_pct") > 100),
    ),
    count_rule(
        "gold_fato_participacao_vendedor_uf_mes",
        "PARTICIPACAO_002",
        "Participação calculada sem total positivo de referência.",
        F.col("volume_total_positivo_uf_litros").isNull() | (F.col("volume_total_positivo_uf_litros") <= 0),
    ),
    count_rule(
        "gold_fato_mercado_municipio_anual",
        "MERCADO_MUNICIPIO_001",
        "Venda municipal disponível sem preço no mesmo município, ano e produto.",
        F.col("possui_venda") & ~F.col("possui_preco"),
        "INFORMATIVA",
    ),
    count_rule(
        "gold_fato_mercado_municipio_anual",
        "MERCADO_MUNICIPIO_002",
        "Preço disponível sem venda municipal no mesmo município, ano e produto.",
        F.col("possui_preco") & ~F.col("possui_venda"),
    ),
    count_rule(
        "gold_fato_mercado_municipio_anual",
        "MERCADO_MUNICIPIO_003",
        "Cobertura inferior ao mínimo de postos ou semanas definido no notebook.",
        F.col("possui_preco") & ~F.col("cobertura_suficiente"),
        "INFORMATIVA",
    ),
    count_rule(
        "gold_preco_outlier",
        "PRECO_006",
        "Preço fora dos limites de 1,5 IQR no município, produto e ano.",
        F.col("outlier_iqr"),
        "INFORMATIVA",
    ),
    count_rule(
        "gold_reconciliacao_volume_uf_ano",
        "RECONCILIACAO_001",
        "Comparação anual sem os doze meses observados na fonte logística.",
        F.col("status_conciliacao") == "PERIODO_INCOMPLETO",
        "INFORMATIVA",
    ),
    count_rule(
        "gold_reconciliacao_volume_uf_ano",
        "RECONCILIACAO_002",
        "Diferença de escopo entre o volume logístico e o municipal.",
        F.col("status_conciliacao") == "DIVERGENCIA_DE_ESCOPO",
        "INFORMATIVA",
    ),
]

participacao_soma = (
    spark.table(table("gold_fato_participacao_vendedor_uf_mes"))
    .groupBy("data_referencia", "uf", "produto_analitico")
    .agg(F.sum("participacao_pct").alias("soma_participacao_pct"))
)
contagem_participacao = participacao_soma.agg(
    F.count(F.lit(1)).alias("total"),
    F.sum(F.when(F.abs(F.col("soma_participacao_pct") - 100) > 0.01, F.lit(1)).otherwise(F.lit(0))).alias(
        "afetados"
    ),
).first()
qualidade.append(
    result(
        "PARTICIPACAO_003",
        "gold_fato_participacao_vendedor_uf_mes",
        "Soma das participações diferente de 100% dentro da tolerância de arredondamento.",
        int(contagem_participacao["afetados"] or 0),
        int(contagem_participacao["total"] or 0),
    )
)

fato_preco = spark.table(table("gold_fato_preco_coletado"))
dim_municipio = spark.table(table("gold_dim_municipio")).select("codigo_ibge").distinct()
orfaos = fato_preco.filter(F.col("codigo_ibge").isNotNull()).join(
    dim_municipio, "codigo_ibge", "left_anti"
).count()
qualidade.append(
    result(
        "INTEGRIDADE_001",
        "gold_fato_preco_coletado",
        "Código IBGE de preço sem correspondente na dimensão de município.",
        orfaos,
        fato_preco.count(),
    )
)

resultado_qualidade = spark.createDataFrame(qualidade)
write_delta("gold_resultado_regra_qualidade", resultado_qualidade)

resumo = resultado_qualidade.groupBy("status").agg(
    F.count(F.lit(1)).alias("qtd_regras"),
    F.sum("qtd_afetada").alias("qtd_ocorrencias"),
    F.max("data_execucao").alias("data_execucao"),
)
write_delta("gold_resumo_qualidade", resumo)

profile_rows.extend(profile_table("gold_resultado_regra_qualidade"))
profile_rows.extend(profile_table("gold_resumo_qualidade"))
perfil = spark.createDataFrame(profile_rows)
catalogo = perfil.join(catalogo_ref, ["tabela", "coluna"], "left")
catalogo = (
    catalogo.withColumn(
        "descricao",
        F.coalesce(
            F.col("descricao"),
            F.when(F.col("tipo_spark").like("boolean%"), F.lit("Indicador lógico produzido pelo processo."))
            .when(F.col("coluna").like("%_id"), F.lit("Identificador técnico do processo."))
            .when(F.col("coluna").like("%_pct"), F.lit("Medida percentual produzida pelo processo."))
            .when(F.col("coluna").like("%_litros"), F.lit("Medida de volume produzida pelo processo."))
            .otherwise(F.concat(F.lit("Campo produzido na tabela "), F.col("tabela"))),
        ),
    )
    .withColumn(
        "dominio_esperado",
        F.coalesce(
            F.col("dominio_esperado"),
            F.when(F.col("tipo_spark").like("boolean%"), F.lit("true ou false"))
            .when(F.col("tipo_spark").like("date%"), F.lit("data válida"))
            .when(F.col("tipo_spark").like("timestamp%"), F.lit("data e hora válida"))
            .when(F.col("tipo_spark").rlike("^(int|bigint|decimal|double|float)"), F.lit("valor numérico"))
            .otherwise(F.lit("categoria observada na fonte ou no processo")),
        ),
    )
    .withColumn("unidade", F.coalesce(F.col("unidade"), F.lit("não aplicável")))
    .withColumn(
        "origem_linhagem",
        F.coalesce(F.col("origem_linhagem"), F.concat(F.lit("gerado por "), F.col("tabela"))),
    )
)
write_delta("gold_catalogo_atributos", catalogo)

print(f"Catálogo e regras de qualidade concluídos em {EXECUTED_AT}.")
display(resultado_qualidade.orderBy("status", "regra_id"))
