# Databricks notebook source
"""Gera perfil por atributo e executa regras de qualidade do projeto."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, Row, functions as F


# COMMAND ----------

dbutils.widgets.text("database", "workspace.anp_rj_2022_2024", "Database")
dbutils.widgets.text(
    "config_root",
    "/Volumes/workspace/anp_rj_2022_2024/anp/config",
    "Pasta config no Volume",
)
dbutils.widgets.dropdown("perfil_completo", "true", ["true", "false"], "Contar todas as linhas")

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
    "gold_dim_tempo",
    "gold_dim_municipio",
    "gold_dim_produto",
    "gold_dim_revenda",
    "gold_dim_bandeira",
    "gold_dim_lote_carga",
    "gold_ref_conciliacao_municipio",
    "gold_ref_mapeamento_produto",
    "gold_fato_preco_coletado",
    "gold_fato_preco_municipio_semana",
    "gold_fato_preco_municipio_anual",
    "gold_fato_venda_municipio_anual",
    "gold_fato_mercado_municipio_anual",
    "gold_fato_cadastro_revenda_snapshot",
    "gold_cobertura_pesquisa",
    "gold_preco_outlier",
    "gold_reconciliacao_etapas",
]


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


def profile_table(table_name: str) -> list[dict[str, object]]:
    frame = spark.table(table(table_name))
    if not PERFIL_COMPLETO:
        frame = frame.limit(100_000)

    profiles: list[dict[str, object]] = []
    schema = {field.name: field.dataType.simpleString() for field in frame.schema.fields}
    for column_name, spark_type in schema.items():
        value = value_expression(column_name)
        metrics = frame.agg(
            F.count(F.lit(1)).alias("total_registros"),
            F.sum(F.when(value.isNull(), F.lit(1)).otherwise(F.lit(0))).alias("qtd_nulos"),
            F.countDistinct(value).alias("qtd_distintos"),
            F.min(value).alias("minimo"),
            F.max(value).alias("maximo"),
        ).first()
        categories = top_categories(frame, column_name) if spark_type.startswith(("string", "boolean")) else None
        total = int(metrics["total_registros"] or 0)
        nulls = int(metrics["qtd_nulos"] or 0)
        profiles.append(
            {
                "tabela": table_name,
                "coluna": column_name,
                "tipo_spark": spark_type,
                "total_registros": total,
                "qtd_nulos": nulls,
                "pct_nulos": round((nulls / total * 100) if total else 0, 4),
                "qtd_distintos": int(metrics["qtd_distintos"] or 0),
                "min_observado": None if metrics["minimo"] is None else str(metrics["minimo"]),
                "max_observado": None if metrics["maximo"] is None else str(metrics["maximo"]),
                "categorias_frequentes": categories,
                "modo_perfil": "completo" if PERFIL_COMPLETO else "amostra_100000",
                "data_execucao": EXECUTED_AT,
            }
        )
    return profiles


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

# COMMAND ----------

def result(rule_id: str, table_name: str, description: str, affected: int, total: int) -> Row:
    percentage = round((affected / total * 100) if total else 0, 4)
    return Row(
        regra_id=rule_id,
        tabela=table_name,
        descricao=description,
        total_avaliado=total,
        qtd_afetada=affected,
        pct_afetada=percentage,
        status="APROVADA" if affected == 0 else "ATENCAO",
        data_execucao=EXECUTED_AT,
    )


def count_rule(table_name: str, rule_id: str, description: str, condition) -> Row:
    frame = spark.table(table(table_name))
    return result(rule_id, table_name, description, frame.filter(condition).count(), frame.count())


quality_results = [
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
        "Município de preço não conciliado ao código IBGE.",
        ~F.col("municipio_conciliado"),
    ),
    count_rule(
        "silver_preco_coletado",
        "PRECO_004",
        "Produto publicado sem correspondência na tabela de mapeamento.",
        F.col("produto_analitico") == "NAO_MAPEADO",
    ),
    count_rule(
        "silver_preco_coletado",
        "PRECO_005",
        "Repetição da chave CNPJ + produto + data de coleta.",
        F.col("duplicidade_chave"),
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_001",
        "Volume ausente ou negativo.",
        ~F.col("volume_valido"),
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_002",
        "Código IBGE fora do padrão de sete dígitos.",
        ~F.col("codigo_ibge").rlike(r"^\d{7}$"),
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_003",
        "Produto de venda sem correspondência no mapeamento.",
        F.col("produto_analitico") == "NAO_MAPEADO",
    ),
    count_rule(
        "silver_venda_municipio",
        "VENDA_004",
        "Repetição da chave código IBGE + produto + ano.",
        F.col("duplicidade_chave"),
    ),
    count_rule(
        "gold_fato_mercado_municipio_anual",
        "MERCADO_001",
        "Venda disponível sem observação de preço no município-produto-ano.",
        F.col("possui_venda") & ~F.col("possui_preco"),
    ),
    count_rule(
        "gold_fato_mercado_municipio_anual",
        "MERCADO_002",
        "Preço disponível sem venda no município-produto-ano.",
        F.col("possui_preco") & ~F.col("possui_venda"),
    ),
    count_rule(
        "gold_fato_mercado_municipio_anual",
        "MERCADO_003",
        "Cobertura abaixo do critério de publicação.",
        F.col("possui_preco") & ~F.col("cobertura_suficiente"),
    ),
    count_rule(
        "gold_preco_outlier",
        "PRECO_006",
        "Preço fora dos limites de 1,5 IQR do município-produto-ano.",
        F.col("outlier_iqr"),
    ),
]

fato_preco = spark.table(table("gold_fato_preco_coletado"))
dim_municipio = spark.table(table("gold_dim_municipio")).select("codigo_ibge").distinct()
orfaos = fato_preco.filter(F.col("codigo_ibge").isNotNull()).join(dim_municipio, "codigo_ibge", "left_anti").count()
quality_results.append(
    result(
        "INTEGRIDADE_001",
        "gold_fato_preco_coletado",
        "Código IBGE de preço sem correspondente na dimensão de município.",
        orfaos,
        fato_preco.count(),
    )
)

qualidade = spark.createDataFrame(quality_results)
write_delta("gold_resultado_regra_qualidade", qualidade)

resumo = qualidade.groupBy("status").agg(
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
        F.coalesce(F.col("descricao"), F.concat(F.lit("Campo técnico produzido na tabela "), F.col("tabela"))),
    )
    .withColumn("dominio_esperado", F.coalesce(F.col("dominio_esperado"), F.lit("Consultar valores observados")))
    .withColumn("unidade", F.coalesce(F.col("unidade"), F.lit("não aplicável")))
    .withColumn(
        "origem_linhagem",
        F.coalesce(F.col("origem_linhagem"), F.concat(F.lit("gerado por "), F.col("tabela"))),
    )
)
write_delta("gold_catalogo_atributos", catalogo)

print(f"Catálogo e regras de qualidade concluídos em {EXECUTED_AT}.")
display(qualidade.orderBy("status", "regra_id"))
