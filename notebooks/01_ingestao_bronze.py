# Databricks notebook source
"""Carrega arquivos preparados da ANP na camada Bronze."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, functions as F


# COMMAND ----------

dbutils.widgets.text("database", "workspace.anp_combustiveis_br_2022_2024", "Database")
dbutils.widgets.text(
    "raw_root",
    "/Volumes/workspace/anp_rj_2022_2024/anp/raw",
    "Pasta raw no Volume",
)
dbutils.widgets.text(
    "config_root",
    "/Volumes/workspace/anp_rj_2022_2024/anp/config",
    "Pasta config no Volume",
)

DATABASE = dbutils.widgets.get("database").strip()
RAW_ROOT = dbutils.widgets.get("raw_root").rstrip("/")
CONFIG_ROOT = dbutils.widgets.get("config_root").rstrip("/")
LOTE_ID = f"anp_br_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
INGESTED_AT = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

if not DATABASE:
    raise ValueError("Informe o schema no widget database.")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}")


def table(name: str) -> str:
    return f"{DATABASE}.{name}"


def has_files(path: str) -> bool:
    try:
        return bool(dbutils.fs.ls(path))
    except Exception:
        return False


def safe_column_name(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")
    if not normalized:
        return "coluna"
    if normalized[0].isdigit():
        return f"coluna_{normalized}"
    return normalized


def normalize_column_names(frame: DataFrame) -> DataFrame:
    used: set[str] = set()
    names: list[str] = []
    for original in frame.columns:
        candidate = safe_column_name(original)
        suffix = 2
        while candidate in used:
            candidate = f"{safe_column_name(original)}_{suffix}"
            suffix += 1
        names.append(candidate)
        used.add(candidate)
    return frame.toDF(*names)


def with_load_metadata(frame: DataFrame, source_name: str) -> DataFrame:
    enriched = (
        frame.withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("fonte_carga", F.lit(source_name))
        .withColumn("lote_id", F.lit(LOTE_ID))
        .withColumn("data_ingestao", F.to_timestamp(F.lit(INGESTED_AT)))
    )
    return normalize_column_names(enriched)


def read_csv(path: str, source_name: str, encoding: str = "UTF-8", recursive: bool = False) -> DataFrame:
    reader = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", encoding)
        .option("enforceSchema", "false")
    )
    if recursive:
        reader = reader.option("recursiveFileLookup", "true")
    return with_load_metadata(reader.csv(path), source_name)


def write_bronze(name: str, frame: DataFrame) -> int:
    row_count = frame.count()
    (
        frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table(name))
    )
    return row_count


# COMMAND ----------

paths = {
    "preco": f"{RAW_ROOT}/precos/extraidos",
    "venda_gasolina_c": f"{RAW_ROOT}/vendas/vendas_gasolina_c_municipio.csv",
    "venda_etanol_hidratado": f"{RAW_ROOT}/vendas/vendas_etanol_hidratado_municipio.csv",
    "venda_logistica_mercado": f"{RAW_ROOT}/logistica/extraidos/vendas_mercado_brasileiro.csv",
    "cadastro_revenda": f"{RAW_ROOT}/cadastro/cadastro_revendedores_atual.csv",
    "produto_mapeamento": f"{CONFIG_ROOT}/produto_mapeamento.csv",
    "empresa_grupo_mapeamento": f"{CONFIG_ROOT}/empresa_grupo_mapeamento.csv",
}
missing = [name for name, path in paths.items() if not has_files(path)]
if missing:
    raise FileNotFoundError(
        "Não encontrei: "
        + ", ".join(missing)
        + ". Confirme o upload das pastas raw e config e ajuste os widgets."
    )

preco = read_csv(paths["preco"], "anp_preco_semanal", recursive=True)
venda_gasolina = read_csv(paths["venda_gasolina_c"], "anp_venda_gasolina_c_municipio")
venda_etanol = read_csv(paths["venda_etanol_hidratado"], "anp_venda_etanol_hidratado_municipio")
venda_logistica = read_csv(
    paths["venda_logistica_mercado"],
    "anp_logistica_02_vendas_mercado",
    encoding="ISO-8859-1",
)
cadastro = read_csv(paths["cadastro_revenda"], "anp_cadastro_revendedores")
produto_mapeamento = read_csv(paths["produto_mapeamento"], "referencia_produto_projeto")
empresa_grupo = read_csv(paths["empresa_grupo_mapeamento"], "referencia_empresa_grupo")

rows_by_table = [
    ("bronze_preco", "anp_preco_semanal", write_bronze("bronze_preco", preco)),
    (
        "bronze_venda_gasolina_c",
        "anp_venda_gasolina_c_municipio",
        write_bronze("bronze_venda_gasolina_c", venda_gasolina),
    ),
    (
        "bronze_venda_etanol_hidratado",
        "anp_venda_etanol_hidratado_municipio",
        write_bronze("bronze_venda_etanol_hidratado", venda_etanol),
    ),
    (
        "bronze_venda_logistica_mercado",
        "anp_logistica_02_vendas_mercado",
        write_bronze("bronze_venda_logistica_mercado", venda_logistica),
    ),
    ("bronze_cadastro_revenda", "anp_cadastro_revendedores", write_bronze("bronze_cadastro_revenda", cadastro)),
    (
        "bronze_ref_produto_mapeamento",
        "referencia_produto_projeto",
        write_bronze("bronze_ref_produto_mapeamento", produto_mapeamento),
    ),
    (
        "bronze_ref_empresa_grupo_mapeamento",
        "referencia_empresa_grupo",
        write_bronze("bronze_ref_empresa_grupo_mapeamento", empresa_grupo),
    ),
]

lote = spark.createDataFrame(rows_by_table, ["tabela", "fonte", "quantidade_linhas"])
lote = (
    lote.withColumn("lote_id", F.lit(LOTE_ID))
    .withColumn("data_ingestao", F.to_timestamp(F.lit(INGESTED_AT)))
    .withColumn("raw_root", F.lit(RAW_ROOT))
)
lote.write.format("delta").mode("append").saveAsTable(table("bronze_lote_carga"))

print(f"Lote concluído: {LOTE_ID}")
display(lote.orderBy("tabela"))
