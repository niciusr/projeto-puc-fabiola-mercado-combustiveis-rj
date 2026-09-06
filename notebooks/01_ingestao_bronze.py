# Databricks notebook source
"""Carga Bronze dos arquivos ANP já enviados para um Volume do Databricks."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, functions as F


# COMMAND ----------

dbutils.widgets.text("database", "workspace.anp_rj_2022_2024", "Database")
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
LOTE_ID = f"anp_rj_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
INGESTED_AT = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

if not DATABASE:
    raise ValueError("Informe o schema no widget database. Exemplo: workspace.anp_rj_2022_2024")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}")


def table(name: str) -> str:
    return f"{DATABASE}.{name}"


def has_files(path: str) -> bool:
    try:
        return bool(dbutils.fs.ls(path))
    except Exception:
        return False


def with_load_metadata(frame: DataFrame, source_name: str) -> DataFrame:
    return (
        frame.withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("fonte_carga", F.lit(source_name))
        .withColumn("lote_id", F.lit(LOTE_ID))
        .withColumn("data_ingestao", F.to_timestamp(F.lit(INGESTED_AT)))
    )


def read_csv_tree(path: str, source_name: str) -> DataFrame:
    return with_load_metadata(
        spark.read.option("header", True)
        .option("sep", ";")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("recursiveFileLookup", "true")
        .option("enforceSchema", "false")
        .csv(path),
        source_name,
    )


def write_bronze(name: str, frame: DataFrame) -> int:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(table(name))
    )
    return frame.count()


# COMMAND ----------

paths = {
    "preco": f"{RAW_ROOT}/precos/extraidos",
    "venda_gasolina_c": f"{RAW_ROOT}/vendas/vendas_gasolina_c_municipio.csv",
    "venda_etanol_hidratado": f"{RAW_ROOT}/vendas/vendas_etanol_hidratado_municipio.csv",
    "cadastro_revenda": f"{RAW_ROOT}/cadastro/cadastro_revendedores_atual.csv",
    "produto_mapeamento": f"{CONFIG_ROOT}/produto_mapeamento.csv",
}

missing = [name for name, path in paths.items() if not has_files(path)]
if missing:
    raise FileNotFoundError(
        "Não encontrei: "
        + ", ".join(missing)
        + ". Confirme o upload das pastas raw e config e ajuste os widgets."
    )

preco = read_csv_tree(paths["preco"], "anp_preco_semanal")
venda_gasolina = read_csv_tree(paths["venda_gasolina_c"], "anp_venda_gasolina_c_municipio")
venda_etanol = read_csv_tree(paths["venda_etanol_hidratado"], "anp_venda_etanol_hidratado_municipio")
cadastro = read_csv_tree(paths["cadastro_revenda"], "anp_cadastro_revendedores")
produto_mapeamento = read_csv_tree(paths["produto_mapeamento"], "referencia_produto_projeto")

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
    ("bronze_cadastro_revenda", "anp_cadastro_revendedores", write_bronze("bronze_cadastro_revenda", cadastro)),
    (
        "bronze_ref_produto_mapeamento",
        "referencia_produto_projeto",
        write_bronze("bronze_ref_produto_mapeamento", produto_mapeamento),
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
