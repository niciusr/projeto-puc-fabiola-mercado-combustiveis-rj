# Databricks notebook source
"""Transforma as fontes Bronze em tabelas Silver e Gold do projeto."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DecimalType


# COMMAND ----------

dbutils.widgets.text("database", "workspace.anp_combustiveis_br_2022_2024", "Database")
dbutils.widgets.text("ano_inicial", "2022", "Ano inicial")
dbutils.widgets.text("ano_final", "2024", "Ano final")
dbutils.widgets.text("min_postos", "3", "Mínimo de postos")
dbutils.widgets.text("min_semanas", "4", "Mínimo de semanas")
dbutils.widgets.text("lote_id", "", "Lote Bronze (vazio = último lote)")

DATABASE = dbutils.widgets.get("database").strip()
ANO_INICIAL = int(dbutils.widgets.get("ano_inicial"))
ANO_FINAL = int(dbutils.widgets.get("ano_final"))
MIN_POSTOS = int(dbutils.widgets.get("min_postos"))
MIN_SEMANAS = int(dbutils.widgets.get("min_semanas"))
LOTE_ID = dbutils.widgets.get("lote_id").strip()
EXECUTED_AT = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

if not DATABASE:
    raise ValueError("Informe o schema no widget database.")
if ANO_INICIAL > ANO_FINAL:
    raise ValueError("O ano inicial não pode ser maior que o ano final.")


def table(name: str) -> str:
    return f"{DATABASE}.{name}"


def write_delta(name: str, frame: DataFrame) -> None:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table(name))


if not LOTE_ID:
    latest = (
        spark.table(table("bronze_lote_carga"))
        .orderBy(F.col("data_ingestao").desc(), F.col("lote_id").desc())
        .select("lote_id")
        .first()
    )
    if latest is None:
        raise ValueError("Não há lote Bronze. Execute o notebook 01 antes deste.")
    LOTE_ID = latest["lote_id"]


def bronze_lote(name: str) -> DataFrame:
    frame = spark.table(table(name)).filter(F.col("lote_id") == LOTE_ID)
    if frame.limit(1).count() == 0:
        raise ValueError(f"A tabela {name} não contém o lote {LOTE_ID}.")
    return frame


def normalized_name(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", ascii_name.lower())).strip("_")


def normalize_headers(frame: DataFrame) -> DataFrame:
    result = frame
    for original in frame.columns:
        target = normalized_name(original)
        if original != target and target not in result.columns:
            result = result.withColumnRenamed(original, target)
    return result


ACCENTED = "ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇÑáàãâäéèêëíìîïóòõôöúùûüçñ"
PLAIN = "AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn"


def normalized_text(column):
    return F.upper(F.trim(F.translate(F.coalesce(column.cast("string"), F.lit("")), ACCENTED, PLAIN)))


def normalized_key(column):
    return F.trim(F.regexp_replace(normalized_text(column), r"[^A-Z0-9]+", " "))


def decimal_from_text(column):
    raw = F.trim(column.cast("string"))
    brazilian_format = raw.rlike(r"^-?\d{1,3}(\.\d{3})+(,\d+)?$") | raw.rlike(r"^-?\d+,\d+$")
    standardized = F.regexp_replace(F.regexp_replace(raw, r"\.", ""), ",", ".")
    return (
        F.when(raw == "", F.lit(None))
        .when(brazilian_format, standardized.cast(DecimalType(20, 3)))
        .otherwise(raw.cast(DecimalType(20, 3)))
    )


def pick(frame: DataFrame, aliases: list[str], label: str, required: bool = True):
    for alias in aliases:
        if alias in frame.columns:
            return F.col(alias)
    if required:
        raise ValueError(f"A coluna {label} não foi encontrada. Colunas disponíveis: {frame.columns}")
    return F.lit(None).cast("string")


def date_from_anp(column):
    text = F.trim(column.cast("string"))
    return (
        F.when(text.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(text, "dd/MM/yyyy"))
        .when(text.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(text, "yyyy-MM-dd"))
        .otherwise(F.lit(None).cast("date"))
    )


def normalized_cnpj(column):
    return F.regexp_replace(F.coalesce(column.cast("string"), F.lit("")), r"\D", "")


def bool_from_text(column):
    return F.lower(F.trim(column.cast("string"))).isin("true", "1", "sim", "s")


def add_duplicate_flag(frame: DataFrame, key_column: str) -> DataFrame:
    return frame.withColumn("duplicidade_chave", F.count(F.lit(1)).over(Window.partitionBy(key_column)) > 1)


# COMMAND ----------

produto_ref_raw = normalize_headers(bronze_lote("bronze_ref_produto_mapeamento"))
produto_ref = (
    produto_ref_raw.select(
        normalized_text(pick(produto_ref_raw, ["fonte"], "fonte")).alias("fonte"),
        normalized_text(
            pick(produto_ref_raw, ["produto_origem_normalizado"], "produto_origem_normalizado")
        ).alias("produto_origem_normalizado"),
        normalized_text(pick(produto_ref_raw, ["produto_analitico"], "produto_analitico")).alias(
            "produto_analitico"
        ),
        normalized_text(pick(produto_ref_raw, ["familia"], "familia")).alias("familia"),
        normalized_text(
            pick(produto_ref_raw, ["grupo_reconciliacao_municipal"], "grupo_reconciliacao_municipal")
        ).alias("grupo_reconciliacao_municipal"),
        F.lower(
            F.trim(pick(produto_ref_raw, ["compatibilidade_analitica"], "compatibilidade_analitica"))
        ).alias("compatibilidade_analitica"),
        bool_from_text(
            pick(produto_ref_raw, ["usar_analise_principal"], "usar_analise_principal")
        ).alias("usar_analise_principal"),
        pick(produto_ref_raw, ["observacao"], "observacao", required=False).alias("observacao"),
    )
    .dropDuplicates(["fonte", "produto_origem_normalizado"])
)

empresa_ref_raw = normalize_headers(bronze_lote("bronze_ref_empresa_grupo_mapeamento"))
empresa_ref = (
    empresa_ref_raw.select(
        normalized_key(pick(empresa_ref_raw, ["vendedor_chave"], "vendedor_chave")).alias("vendedor_chave"),
        F.trim(pick(empresa_ref_raw, ["empresa_canonica"], "empresa_canonica")).alias("empresa_canonica"),
        F.trim(pick(empresa_ref_raw, ["grupo_economico"], "grupo_economico")).alias("grupo_economico"),
        F.trim(pick(empresa_ref_raw, ["fonte_classificacao"], "fonte_classificacao")).alias(
            "fonte_classificacao"
        ),
        date_from_anp(pick(empresa_ref_raw, ["vigencia_inicio"], "vigencia_inicio", required=False)).alias(
            "vigencia_inicio"
        ),
        date_from_anp(pick(empresa_ref_raw, ["vigencia_fim"], "vigencia_fim", required=False)).alias(
            "vigencia_fim"
        ),
        pick(empresa_ref_raw, ["observacao"], "observacao", required=False).alias("observacao"),
    )
    .filter(F.col("vendedor_chave") != "")
    .dropDuplicates(["vendedor_chave"])
)


def apply_product_map(frame: DataFrame, source_name: str) -> DataFrame:
    mapping = produto_ref.filter(F.col("fonte") == source_name).select(
        "produto_origem_normalizado",
        "produto_analitico",
        "familia",
        "grupo_reconciliacao_municipal",
        "compatibilidade_analitica",
        "usar_analise_principal",
    )
    return (
        frame.join(mapping, "produto_origem_normalizado", "left")
        .withColumn("produto_analitico", F.coalesce(F.col("produto_analitico"), F.lit("NAO_MAPEADO")))
        .withColumn("familia", F.coalesce(F.col("familia"), F.lit("NAO_MAPEADA")))
        .withColumn(
            "grupo_reconciliacao_municipal",
            F.coalesce(F.col("grupo_reconciliacao_municipal"), F.lit("NAO_MAPEADO")),
        )
        .withColumn(
            "compatibilidade_analitica",
            F.coalesce(F.col("compatibilidade_analitica"), F.lit("nao_mapeado")),
        )
        .withColumn("usar_analise_principal", F.coalesce(F.col("usar_analise_principal"), F.lit(False)))
    )


# COMMAND ----------

def prepare_sales(frame: DataFrame, default_product: str) -> DataFrame:
    source = normalize_headers(frame)
    product = F.coalesce(pick(source, ["produto"], "produto", required=False), F.lit(default_product))
    result = source.select(
        "lote_id",
        "arquivo_origem",
        "data_ingestao",
        "fonte_carga",
        F.trim(pick(source, ["ano"], "ano").cast("string")).cast("int").alias("ano"),
        normalized_text(pick(source, ["grande_regiao"], "grande_regiao", required=False)).alias("grande_regiao"),
        normalized_text(pick(source, ["uf"], "uf")).alias("uf"),
        F.regexp_replace(pick(source, ["codigo_ibge"], "codigo_ibge").cast("string"), r"\.0$", "").alias(
            "codigo_ibge"
        ),
        F.trim(pick(source, ["municipio"], "municipio")).alias("municipio"),
        F.trim(product).alias("produto_origem"),
        decimal_from_text(pick(source, ["vendas"], "vendas")).alias("volume_litros"),
    )
    result = result.filter(F.col("ano").between(ANO_INICIAL, ANO_FINAL))
    return result.withColumn("municipio_norm", normalized_text(F.col("municipio"))).withColumn(
        "produto_origem_normalizado", normalized_text(F.col("produto_origem"))
    )


venda_gasolina = prepare_sales(bronze_lote("bronze_venda_gasolina_c"), "GASOLINA C")
venda_etanol = prepare_sales(bronze_lote("bronze_venda_etanol_hidratado"), "ETANOL HIDRATADO")
venda_silver = apply_product_map(
    venda_gasolina.unionByName(venda_etanol, allowMissingColumns=True), "VENDAS_MUNICIPAIS"
)
venda_silver = (
    venda_silver.withColumn("volume_valido", F.col("volume_litros").isNotNull() & (F.col("volume_litros") >= 0))
    .withColumn(
        "chave_venda",
        F.sha2(F.concat_ws("||", "codigo_ibge", "produto_analitico", F.col("ano").cast("string")), 256),
    )
)
venda_silver = add_duplicate_flag(venda_silver, "chave_venda")
write_delta("silver_venda_municipio", venda_silver)

dim_municipio = (
    venda_silver.filter(F.col("codigo_ibge").rlike(r"^\d{7}$"))
    .groupBy("codigo_ibge", "uf")
    .agg(
        F.first("municipio", ignorenulls=True).alias("municipio"),
        F.first("municipio_norm", ignorenulls=True).alias("municipio_norm"),
        F.first("grande_regiao", ignorenulls=True).alias("grande_regiao"),
    )
)
municipio_match = dim_municipio.select(
    F.col("municipio_norm").alias("municipio_norm_ref"),
    F.col("uf").alias("uf_ref"),
    "codigo_ibge",
)


# COMMAND ----------

preco_raw = normalize_headers(bronze_lote("bronze_preco"))
preco_base = preco_raw.select(
    "lote_id",
    "arquivo_origem",
    "data_ingestao",
    "fonte_carga",
    normalized_text(pick(preco_raw, ["estado_sigla"], "estado_sigla")).alias("uf"),
    F.trim(pick(preco_raw, ["municipio"], "municipio")).alias("municipio_origem"),
    F.trim(pick(preco_raw, ["revenda"], "revenda")).alias("revenda"),
    normalized_cnpj(pick(preco_raw, ["cnpj_da_revenda"], "cnpj_da_revenda")).alias("cnpj"),
    F.trim(pick(preco_raw, ["produto"], "produto")).alias("produto_origem"),
    date_from_anp(pick(preco_raw, ["data_da_coleta"], "data_da_coleta")).alias("data_coleta"),
    decimal_from_text(pick(preco_raw, ["valor_de_venda"], "valor_de_venda")).alias("preco_venda"),
    F.trim(pick(preco_raw, ["unidade_de_medida"], "unidade_de_medida")).alias("unidade_medida"),
    F.trim(pick(preco_raw, ["bandeira"], "bandeira", required=False)).alias("bandeira"),
    F.concat_ws(
        " ",
        F.trim(pick(preco_raw, ["nome_da_rua"], "nome_da_rua", required=False)),
        F.trim(pick(preco_raw, ["numero_rua"], "numero_rua", required=False)),
    ).alias("endereco"),
    F.trim(pick(preco_raw, ["bairro"], "bairro", required=False)).alias("bairro"),
    F.trim(pick(preco_raw, ["cep"], "cep", required=False)).alias("cep"),
)
preco_base = (
    preco_base.withColumn("municipio_norm", normalized_text(F.col("municipio_origem")))
    .withColumn("produto_origem_normalizado", normalized_text(F.col("produto_origem")))
    .withColumn("ano", F.year("data_coleta"))
    .withColumn("mes", F.month("data_coleta"))
    .withColumn("data_referencia", F.to_date(F.date_trunc("month", F.col("data_coleta"))))
    .withColumn("semana_inicio", F.to_date(F.date_trunc("week", F.col("data_coleta"))))
    .withColumn("semana", F.weekofyear("semana_inicio"))
    .filter(F.col("ano").between(ANO_INICIAL, ANO_FINAL))
)
preco_silver = preco_base.join(
    municipio_match,
    (F.col("municipio_norm") == F.col("municipio_norm_ref")) & (F.col("uf") == F.col("uf_ref")),
    "left",
).drop("municipio_norm_ref", "uf_ref")
preco_silver = apply_product_map(preco_silver, "PRECOS")
preco_silver = (
    preco_silver.withColumn("cnpj_formato_valido", F.length("cnpj") == 14)
    .withColumn("municipio_conciliado", F.col("codigo_ibge").isNotNull())
    .withColumn("preco_valido", F.col("preco_venda").isNotNull() & (F.col("preco_venda") > 0))
    .withColumn(
        "chave_preco",
        F.sha2(F.concat_ws("||", "cnpj", "produto_origem_normalizado", F.col("data_coleta").cast("string")), 256),
    )
)
preco_silver = add_duplicate_flag(preco_silver, "chave_preco")
dedupe_window = Window.partitionBy("chave_preco").orderBy(
    F.col("arquivo_origem").asc(), F.col("preco_venda").asc_nulls_last()
)
preco_silver = preco_silver.withColumn("_ordem_chave_preco", F.row_number().over(dedupe_window)).withColumn(
    "selecionado_para_agregacao", F.col("_ordem_chave_preco") == 1
).drop("_ordem_chave_preco")
write_delta("silver_preco_coletado", preco_silver)

conciliacao_municipio = (
    preco_silver.groupBy("municipio_origem", "municipio_norm", "uf", "codigo_ibge", "municipio_conciliado")
    .agg(
        F.count(F.lit(1)).alias("qtd_observacoes_preco"),
        F.min("data_coleta").alias("primeira_coleta"),
        F.max("data_coleta").alias("ultima_coleta"),
    )
    .withColumn("regra_conciliacao", F.lit("UF e município normalizado contra a referência de vendas"))
)
write_delta("gold_ref_conciliacao_municipio", conciliacao_municipio)


# COMMAND ----------

logistica_raw = normalize_headers(bronze_lote("bronze_venda_logistica_mercado"))
logistica_base = logistica_raw.select(
    "lote_id",
    "arquivo_origem",
    "data_ingestao",
    "fonte_carga",
    F.trim(pick(logistica_raw, ["periodo"], "periodo")).alias("periodo_origem"),
    normalized_text(pick(logistica_raw, ["uf_destino"], "uf_destino")).alias("uf"),
    F.trim(pick(logistica_raw, ["produto"], "produto")).alias("produto_origem"),
    F.trim(pick(logistica_raw, ["vendedor"], "vendedor")).alias("vendedor"),
    decimal_from_text(pick(logistica_raw, ["qtd_produto_liquido"], "qtd_produto_liquido")).alias(
        "volume_liquido_litros"
    ),
)
logistica_base = (
    logistica_base.withColumn(
        "data_referencia", F.to_date(F.concat(F.col("periodo_origem"), F.lit("/01")), "yyyy/MM/dd")
    )
    .withColumn("ano", F.year("data_referencia"))
    .withColumn("mes", F.month("data_referencia"))
    .withColumn("produto_origem_normalizado", normalized_text(F.col("produto_origem")))
    .withColumn("vendedor_chave", normalized_key(F.col("vendedor")))
    .filter(F.col("ano").between(ANO_INICIAL, ANO_FINAL))
)
logistica_silver = apply_product_map(logistica_base, "LOGISTICA_MERCADO")
logistica_silver = (
    logistica_silver.withColumn("uf_destino_valida", F.col("uf").rlike(r"^[A-Z]{2}$"))
    .withColumn("vendedor_preenchido", F.col("vendedor_chave") != "")
    .withColumn("volume_valido", F.col("volume_liquido_litros").isNotNull())
    .withColumn("ajuste_negativo", F.col("volume_liquido_litros") < 0)
    .withColumn(
        "chave_venda_logistica",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("data_referencia").cast("string"),
                "uf",
                "produto_origem_normalizado",
                "vendedor_chave",
            ),
            256,
        ),
    )
)
logistica_silver = add_duplicate_flag(logistica_silver, "chave_venda_logistica")
write_delta("silver_venda_empresa_uf_mes", logistica_silver)


# COMMAND ----------

cadastro_raw = normalize_headers(bronze_lote("bronze_cadastro_revenda"))
cadastro_silver = cadastro_raw.select(
    "lote_id",
    "arquivo_origem",
    "data_ingestao",
    "fonte_carga",
    F.trim(pick(cadastro_raw, ["codigoisimp"], "codigoisimp", required=False)).alias("codigo_isimp"),
    F.trim(pick(cadastro_raw, ["autorizacao"], "autorizacao", required=False)).alias("autorizacao"),
    date_from_anp(pick(cadastro_raw, ["datapublicacao"], "datapublicacao", required=False)).alias(
        "data_publicacao"
    ),
    F.trim(pick(cadastro_raw, ["razaosocial"], "razaosocial", required=False)).alias("razao_social"),
    normalized_cnpj(pick(cadastro_raw, ["cnpj"], "cnpj", required=False)).alias("cnpj"),
    F.trim(pick(cadastro_raw, ["endereco"], "endereco", required=False)).alias("endereco"),
    F.trim(pick(cadastro_raw, ["bairro"], "bairro", required=False)).alias("bairro"),
    F.trim(pick(cadastro_raw, ["cep"], "cep", required=False)).alias("cep"),
    normalized_text(pick(cadastro_raw, ["uf"], "uf", required=False)).alias("uf"),
    F.trim(pick(cadastro_raw, ["municipio"], "municipio", required=False)).alias("municipio"),
    F.coalesce(F.trim(pick(cadastro_raw, ["bandeira"], "bandeira", required=False)), F.lit("NAO INFORMADA")).alias(
        "bandeira"
    ),
    date_from_anp(pick(cadastro_raw, ["datavinculacao"], "datavinculacao", required=False)).alias(
        "data_vinculacao"
    ),
).withColumn("data_extracao", F.to_date("data_ingestao"))
cadastro_silver = cadastro_silver.withColumn("cnpj_formato_valido", F.length("cnpj") == 14).withColumn(
    "chave_cadastro", F.sha2(F.concat_ws("||", "cnpj", "uf", "bandeira"), 256)
)
cadastro_silver = add_duplicate_flag(cadastro_silver, "chave_cadastro")
write_delta("silver_cadastro_revenda", cadastro_silver)


# COMMAND ----------

ref_mapeamento_produto = produto_ref.dropDuplicates()
dim_produto = (
    ref_mapeamento_produto.groupBy("produto_analitico")
    .agg(
        F.first("familia", ignorenulls=True).alias("familia"),
        F.first("grupo_reconciliacao_municipal", ignorenulls=True).alias("grupo_reconciliacao_municipal"),
        F.concat_ws(" | ", F.sort_array(F.collect_set("compatibilidade_analitica"))).alias(
            "compatibilidades_origem"
        ),
        F.max(F.col("usar_analise_principal").cast("int")).cast("boolean").alias("usar_analise_principal"),
    )
)
dim_empresa_vendedora = (
    logistica_silver.groupBy("vendedor_chave")
    .agg(
        F.first("vendedor", ignorenulls=True).alias("vendedor"),
        F.min("data_referencia").alias("primeira_referencia"),
        F.max("data_referencia").alias("ultima_referencia"),
    )
    .join(empresa_ref, "vendedor_chave", "left")
    .withColumn("empresa_canonica", F.coalesce(F.col("empresa_canonica"), F.col("vendedor")))
    .withColumn("grupo_economico", F.coalesce(F.col("grupo_economico"), F.lit("NAO CLASSIFICADO")))
    .withColumn("grupo_mapeado", F.col("fonte_classificacao").isNotNull())
)
ufs_venda = dim_municipio.select("uf", "grande_regiao")
ufs_preco = preco_silver.select("uf").withColumn("grande_regiao", F.lit(None).cast("string"))
ufs_logistica = logistica_silver.select("uf").withColumn("grande_regiao", F.lit(None).cast("string"))
dim_uf = (
    ufs_venda.unionByName(ufs_preco).unionByName(ufs_logistica).filter(F.col("uf").rlike(r"^[A-Z]{2}$"))
    .groupBy("uf")
    .agg(F.first("grande_regiao", ignorenulls=True).alias("grande_regiao"))
)
dim_revenda = (
    cadastro_silver.filter(F.length("cnpj") == 14)
    .groupBy("cnpj")
    .agg(
        F.first("razao_social", ignorenulls=True).alias("razao_social"),
        F.first("uf", ignorenulls=True).alias("uf"),
        F.first("municipio", ignorenulls=True).alias("municipio"),
        F.first("bandeira", ignorenulls=True).alias("bandeira"),
        F.max("data_extracao").alias("data_extracao"),
    )
)
dim_bandeira = (
    cadastro_silver.select(F.coalesce(F.col("bandeira"), F.lit("NAO INFORMADA")).alias("bandeira"))
    .unionByName(preco_silver.select(F.coalesce(F.col("bandeira"), F.lit("NAO INFORMADA")).alias("bandeira")))
    .distinct()
    .withColumn("bandeira_branca", normalized_text(F.col("bandeira")) == "BANDEIRA BRANCA")
)
dim_tempo = spark.sql(
    "SELECT explode(sequence("
    f"DATE '{ANO_INICIAL}-01-01', DATE '{ANO_FINAL}-12-31', INTERVAL 1 DAY)) AS data"
)
dim_tempo = (
    dim_tempo.withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("semana", F.weekofyear("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("semestre", F.when(F.col("mes") <= 6, F.lit(1)).otherwise(F.lit(2)))
)

write_delta("gold_dim_tempo", dim_tempo)
write_delta("gold_dim_uf", dim_uf)
write_delta("gold_dim_municipio", dim_municipio)
write_delta("gold_dim_produto", dim_produto)
write_delta("gold_dim_empresa_vendedora", dim_empresa_vendedora)
write_delta("gold_dim_revenda", dim_revenda)
write_delta("gold_dim_bandeira", dim_bandeira)
write_delta("gold_dim_lote_carga", bronze_lote("bronze_lote_carga"))
write_delta("gold_ref_mapeamento_produto", ref_mapeamento_produto)
write_delta("gold_ref_empresa_grupo", empresa_ref)
write_delta("gold_fato_preco_coletado", preco_silver)
write_delta("gold_fato_venda_municipio_anual", venda_silver)
write_delta("gold_fato_cadastro_revenda_snapshot", cadastro_silver)


# COMMAND ----------

preco_agregavel = preco_silver.filter(
    F.col("preco_valido")
    & F.col("municipio_conciliado")
    & (F.col("produto_analitico") != "NAO_MAPEADO")
    & F.col("selecionado_para_agregacao")
)
preco_semana = (
    preco_agregavel.groupBy("codigo_ibge", "uf", "produto_analitico", "ano", "semana_inicio", "semana")
    .agg(
        F.count(F.lit(1)).alias("qtd_coletas"),
        F.countDistinct("cnpj").alias("qtd_postos"),
        F.expr("percentile_approx(preco_venda, 0.50, 10000)").alias("mediana_preco"),
        F.avg("preco_venda").alias("media_preco"),
        F.expr("percentile_approx(preco_venda, 0.10, 10000)").alias("p10_preco"),
        F.expr("percentile_approx(preco_venda, 0.90, 10000)").alias("p90_preco"),
        F.min("preco_venda").alias("min_preco"),
        F.max("preco_venda").alias("max_preco"),
        F.stddev_pop("preco_venda").alias("desvio_padrao_preco"),
        F.max("lote_id").alias("lote_id"),
    )
    .withColumn("spread_p90_p10", F.col("p90_preco") - F.col("p10_preco"))
    .withColumn("amplitude_preco", F.col("max_preco") - F.col("min_preco"))
)
write_delta("gold_fato_preco_municipio_semana", preco_semana)

cobertura_anual = preco_agregavel.groupBy("codigo_ibge", "produto_analitico", "ano").agg(
    F.countDistinct("cnpj").alias("postos_distintos")
)
preco_anual_base = (
    preco_semana.groupBy("codigo_ibge", "uf", "produto_analitico", "ano")
    .agg(
        F.expr("percentile_approx(mediana_preco, 0.50, 10000)").alias("mediana_preco_anual"),
        F.avg("mediana_preco").alias("media_semanal_preco"),
        F.stddev_pop("mediana_preco").alias("desvio_entre_semanas"),
        F.avg("spread_p90_p10").alias("spread_medio_p90_p10"),
        F.countDistinct("semana_inicio").alias("semanas_pesquisadas"),
        F.sum("qtd_coletas").alias("qtd_coletas"),
        F.max("lote_id").alias("lote_id"),
    )
    .join(cobertura_anual, ["codigo_ibge", "produto_analitico", "ano"], "left")
)
referencia_uf = preco_anual_base.groupBy("uf", "produto_analitico", "ano").agg(
    F.expr("percentile_approx(mediana_preco_anual, 0.50, 10000)").alias("mediana_preco_uf")
)
preco_anual = (
    preco_anual_base.join(referencia_uf, ["uf", "produto_analitico", "ano"], "left")
    .withColumn(
        "cobertura_suficiente",
        (F.col("postos_distintos") >= MIN_POSTOS) & (F.col("semanas_pesquisadas") >= MIN_SEMANAS),
    )
    .withColumn(
        "preco_relativo_uf",
        F.when(F.col("mediana_preco_uf") > 0, F.col("mediana_preco_anual") / F.col("mediana_preco_uf")),
    )
)
write_delta("gold_fato_preco_municipio_anual", preco_anual)

preco_uf_mes = (
    preco_agregavel.filter(F.col("usar_analise_principal"))
    .groupBy("data_referencia", "ano", "mes", "uf", "produto_analitico")
    .agg(
        F.count(F.lit(1)).alias("qtd_coletas"),
        F.countDistinct("cnpj").alias("qtd_postos"),
        F.countDistinct("codigo_ibge").alias("qtd_municipios"),
        F.expr("percentile_approx(preco_venda, 0.50, 10000)").alias("mediana_preco"),
        F.avg("preco_venda").alias("media_preco"),
        F.expr("percentile_approx(preco_venda, 0.10, 10000)").alias("p10_preco"),
        F.expr("percentile_approx(preco_venda, 0.90, 10000)").alias("p90_preco"),
        F.max("lote_id").alias("lote_id"),
    )
    .withColumn("spread_p90_p10", F.col("p90_preco") - F.col("p10_preco"))
)
write_delta("gold_fato_preco_uf_mes", preco_uf_mes)


# COMMAND ----------

venda_empresa = (
    logistica_silver.filter(
        F.col("uf_destino_valida")
        & F.col("vendedor_preenchido")
        & F.col("volume_valido")
        & (F.col("produto_analitico") != "NAO_MAPEADO")
        & F.col("usar_analise_principal")
    )
    .groupBy("data_referencia", "ano", "mes", "uf", "produto_analitico", "vendedor_chave")
    .agg(
        F.first("vendedor", ignorenulls=True).alias("vendedor"),
        F.sum("volume_liquido_litros").alias("volume_liquido_litros"),
        F.sum(F.when(F.col("ajuste_negativo"), F.lit(1)).otherwise(F.lit(0))).alias("qtd_ajustes_negativos"),
        F.sum(F.when(F.col("duplicidade_chave"), F.lit(1)).otherwise(F.lit(0))).alias("qtd_linhas_duplicadas"),
        F.max("lote_id").alias("lote_id"),
    )
    .join(
        dim_empresa_vendedora.select("vendedor_chave", "empresa_canonica", "grupo_economico", "grupo_mapeado"),
        "vendedor_chave",
        "left",
    )
    .withColumn("volume_liquido_m3", F.col("volume_liquido_litros") / F.lit(1000))
)
write_delta("gold_fato_venda_empresa_uf_mes", venda_empresa)

chaves_mercado = ["data_referencia", "ano", "mes", "uf", "produto_analitico"]
resumo_venda_uf = venda_empresa.groupBy(*chaves_mercado).agg(
    F.sum("volume_liquido_litros").alias("volume_total_liquido_uf_litros"),
    F.sum(F.when(F.col("volume_liquido_litros") > 0, F.col("volume_liquido_litros")).otherwise(F.lit(0))).alias(
        "volume_total_positivo_uf_litros"
    ),
    F.countDistinct(F.when(F.col("volume_liquido_litros") > 0, F.col("vendedor_chave"))).alias("vendedores_positivos"),
    F.sum("qtd_ajustes_negativos").alias("qtd_ajustes_negativos"),
    F.max("lote_id").alias("lote_id"),
)
participacao = (
    venda_empresa.join(
        resumo_venda_uf.drop("lote_id", "qtd_ajustes_negativos"),
        chaves_mercado,
        "inner",
    )
    .filter((F.col("volume_liquido_litros") > 0) & (F.col("volume_total_positivo_uf_litros") > 0))
    .withColumn(
        "participacao_pct",
        F.col("volume_liquido_litros") / F.col("volume_total_positivo_uf_litros") * F.lit(100),
    )
)
ranking_window = Window.partitionBy(*chaves_mercado).orderBy(
    F.col("volume_liquido_litros").desc(), F.col("vendedor_chave").asc()
)
participacao = participacao.withColumn("posicao", F.row_number().over(ranking_window))
write_delta("gold_fato_participacao_vendedor_uf_mes", participacao)

concentracao = (
    participacao.groupBy(*chaves_mercado)
    .agg(
        F.countDistinct("vendedor_chave").alias("vendedores_positivos"),
        F.sum(F.pow(F.col("participacao_pct") / F.lit(100), F.lit(2)) * F.lit(10000)).alias("hhi"),
        F.sum(F.when(F.col("posicao") <= 3, F.col("participacao_pct")).otherwise(F.lit(0))).alias(
            "participacao_top3_pct"
        ),
        F.max(F.when(F.col("posicao") == 1, F.col("empresa_canonica"))).alias("empresa_lider"),
        F.max(F.when(F.col("posicao") == 1, F.col("participacao_pct"))).alias("participacao_lider_pct"),
        F.max("lote_id").alias("lote_id"),
    )
    .withColumn(
        "faixa_hhi",
        F.when(F.col("hhi") < 1500, F.lit("BAIXA_CONCENTRACAO"))
        .when(F.col("hhi") < 2500, F.lit("CONCENTRACAO_MODERADA"))
        .otherwise(F.lit("ALTA_CONCENTRACAO")),
    )
)
write_delta("gold_fato_concentracao_uf_mes", concentracao)

mercado_uf = resumo_venda_uf.alias("v").join(preco_uf_mes.alias("p"), chaves_mercado, "full_outer")
mercado_uf = mercado_uf.select(
    *[F.coalesce(F.col(f"v.{key}"), F.col(f"p.{key}")).alias(key) for key in chaves_mercado],
    F.col("v.volume_total_liquido_uf_litros").alias("volume_total_liquido_uf_litros"),
    F.col("v.volume_total_positivo_uf_litros").alias("volume_total_positivo_uf_litros"),
    F.col("v.vendedores_positivos").alias("vendedores_positivos"),
    F.col("v.qtd_ajustes_negativos").alias("qtd_ajustes_negativos"),
    F.col("p.mediana_preco").alias("mediana_preco"),
    F.col("p.media_preco").alias("media_preco"),
    F.col("p.p10_preco").alias("p10_preco"),
    F.col("p.p90_preco").alias("p90_preco"),
    F.col("p.spread_p90_p10").alias("spread_p90_p10"),
    F.col("p.qtd_coletas").alias("qtd_coletas"),
    F.col("p.qtd_postos").alias("qtd_postos"),
    F.col("p.qtd_municipios").alias("qtd_municipios"),
    F.coalesce(F.col("v.lote_id"), F.col("p.lote_id")).alias("lote_id"),
).join(concentracao.drop("lote_id", "vendedores_positivos"), chaves_mercado, "left")
mercado_uf = mercado_uf.withColumn("preco_disponivel", F.col("mediana_preco").isNotNull()).withColumn(
    "volume_disponivel", F.col("volume_total_liquido_uf_litros").isNotNull()
)
write_delta("gold_mart_mercado_uf_mes", mercado_uf)


# COMMAND ----------

produtos_principais = dim_produto.filter(F.col("usar_analise_principal")).select("produto_analitico").distinct()
venda_natural = venda_silver.filter(F.col("volume_valido")).withColumn(
    "_ordem", F.row_number().over(Window.partitionBy("chave_venda").orderBy("arquivo_origem"))
)
venda_principal = (
    venda_natural.filter(F.col("_ordem") == 1)
    .join(produtos_principais, "produto_analitico", "inner")
    .select("codigo_ibge", "uf", "produto_analitico", "ano", "volume_litros", "lote_id")
)
preco_principal = preco_anual.join(produtos_principais, "produto_analitico", "inner")
mercado_municipio = venda_principal.alias("v").join(
    preco_principal.alias("p"), ["codigo_ibge", "produto_analitico", "ano"], "full_outer"
).select(
    F.col("codigo_ibge"),
    F.col("produto_analitico"),
    F.col("ano"),
    F.coalesce(F.col("v.uf"), F.col("p.uf")).alias("uf"),
    F.col("v.volume_litros").alias("volume_litros"),
    F.col("p.mediana_preco_anual").alias("mediana_preco_anual"),
    F.col("p.preco_relativo_uf").alias("preco_relativo_uf"),
    F.col("p.desvio_entre_semanas").alias("desvio_entre_semanas"),
    F.col("p.spread_medio_p90_p10").alias("spread_medio_p90_p10"),
    F.col("p.semanas_pesquisadas").alias("semanas_pesquisadas"),
    F.col("p.postos_distintos").alias("postos_distintos"),
    F.col("p.qtd_coletas").alias("qtd_coletas"),
    F.coalesce(F.col("p.cobertura_suficiente"), F.lit(False)).alias("cobertura_suficiente"),
    F.col("p.mediana_preco_anual").isNotNull().alias("possui_preco"),
    F.col("v.volume_litros").isNotNull().alias("possui_venda"),
    F.coalesce(F.col("v.lote_id"), F.col("p.lote_id")).alias("lote_id"),
)
mercado_municipio = mercado_municipio.withColumn(
    "publicar_analise", F.col("possui_preco") & F.col("possui_venda") & F.col("cobertura_suficiente")
)
write_delta("gold_fato_mercado_municipio_anual", mercado_municipio)

cobertura = (
    mercado_municipio.groupBy("uf", "ano", "produto_analitico")
    .agg(
        F.sum(F.when(F.col("possui_venda"), F.lit(1)).otherwise(F.lit(0))).alias("municipios_com_venda"),
        F.sum(F.when(F.col("possui_preco"), F.lit(1)).otherwise(F.lit(0))).alias("municipios_com_preco"),
        F.sum(F.when(F.col("publicar_analise"), F.lit(1)).otherwise(F.lit(0))).alias("municipios_publicaveis"),
    )
    .withColumn("preco_disponivel", F.col("municipios_com_preco") > 0)
    .withColumn("cobertura_suficiente", F.col("municipios_publicaveis") > 0)
    .withColumn(
        "cobertura_preco_pct",
        F.when(F.col("municipios_com_venda") > 0, F.col("municipios_com_preco") / F.col("municipios_com_venda") * 100),
    )
)
write_delta("gold_cobertura_pesquisa", cobertura)

chaves_iqr = ["codigo_ibge", "produto_analitico", "ano"]
limites_iqr = preco_agregavel.groupBy(*chaves_iqr).agg(
    F.expr("percentile_approx(preco_venda, 0.25, 10000)").alias("p25_preco"),
    F.expr("percentile_approx(preco_venda, 0.75, 10000)").alias("p75_preco"),
)
preco_iqr = preco_agregavel.join(limites_iqr, chaves_iqr, "left")
preco_iqr = preco_iqr.withColumn("iqr_preco", F.col("p75_preco") - F.col("p25_preco")).withColumn(
    "limite_inferior", F.col("p25_preco") - F.lit(1.5) * F.col("iqr_preco")
).withColumn("limite_superior", F.col("p75_preco") + F.lit(1.5) * F.col("iqr_preco"))
preco_iqr = preco_iqr.withColumn(
    "outlier_iqr",
    (F.col("preco_venda") < F.col("limite_inferior")) | (F.col("preco_venda") > F.col("limite_superior")),
)
write_delta("gold_preco_outlier", preco_iqr)


# COMMAND ----------

logistica_reconciliacao = (
    logistica_silver.filter(
        F.col("uf_destino_valida")
        & F.col("volume_valido")
        & (F.col("grupo_reconciliacao_municipal") != "NAO_MAPEADO")
    )
    .groupBy("ano", "uf", "grupo_reconciliacao_municipal")
    .agg(
        F.sum("volume_liquido_litros").alias("volume_logistica_litros"),
        F.countDistinct("data_referencia").alias("meses_logistica_observados"),
    )
)
municipal_reconciliacao = (
    venda_silver.filter(
        F.col("volume_valido") & (F.col("grupo_reconciliacao_municipal") != "NAO_MAPEADO")
    )
    .groupBy("ano", "uf", "grupo_reconciliacao_municipal")
    .agg(F.sum("volume_litros").alias("volume_municipal_litros"))
)
reconciliacao_volume = logistica_reconciliacao.alias("l").join(
    municipal_reconciliacao.alias("m"), ["ano", "uf", "grupo_reconciliacao_municipal"], "full_outer"
).select(
    "ano",
    "uf",
    "grupo_reconciliacao_municipal",
    F.col("l.volume_logistica_litros").alias("volume_logistica_litros"),
    F.col("m.volume_municipal_litros").alias("volume_municipal_litros"),
    F.col("l.meses_logistica_observados").alias("meses_logistica_observados"),
).withColumn(
    "diferenca_litros", F.col("volume_logistica_litros") - F.col("volume_municipal_litros")
).withColumn(
    "diferenca_pct",
    F.when(
        F.col("volume_municipal_litros") != 0,
        F.col("diferenca_litros") / F.col("volume_municipal_litros") * 100,
    ),
).withColumn(
    "status_conciliacao",
    F.when(
        F.col("volume_logistica_litros").isNull() | F.col("volume_municipal_litros").isNull(),
        F.lit("SEM_BASE_COMPARAVEL"),
    )
    .when(F.col("meses_logistica_observados") < 12, F.lit("PERIODO_INCOMPLETO"))
    .when(F.abs(F.col("diferenca_pct")) <= 1, F.lit("COMPATIVEL"))
    .otherwise(F.lit("DIVERGENCIA_DE_ESCOPO")),
).withColumn("lote_id", F.lit(LOTE_ID))
write_delta("gold_reconciliacao_volume_uf_ano", reconciliacao_volume)

rede_bandeira = cadastro_silver.filter(F.col("uf").rlike(r"^[A-Z]{2}$")).groupBy(
    "data_extracao", "uf", "bandeira"
).agg(F.countDistinct("cnpj").alias("qtd_revendas"))
rede_total = rede_bandeira.groupBy("data_extracao", "uf").agg(F.sum("qtd_revendas").alias("qtd_revendas_uf"))
rede_bandeira = rede_bandeira.join(rede_total, ["data_extracao", "uf"], "left").withColumn(
    "participacao_rede_pct",
    F.when(F.col("qtd_revendas_uf") > 0, F.col("qtd_revendas") / F.col("qtd_revendas_uf") * 100),
).withColumn("lote_id", F.lit(LOTE_ID))
write_delta("gold_fato_rede_bandeira_uf_snapshot", rede_bandeira)

reconciliacao_etapas = spark.createDataFrame(
    [
        ("bronze_preco", bronze_lote("bronze_preco").count(), "Registros de preço recebidos no lote."),
        (
            "bronze_logistica_mercado",
            bronze_lote("bronze_venda_logistica_mercado").count(),
            "Registros de vendas por vendedor recebidos no lote.",
        ),
        ("silver_preco", preco_silver.count(), "Preços tipados no período selecionado."),
        ("silver_venda_municipio", venda_silver.count(), "Vendas municipais tipadas no período selecionado."),
        ("silver_venda_empresa", logistica_silver.count(), "Vendas por vendedor tipadas no período selecionado."),
        ("gold_mercado_uf_mes", mercado_uf.count(), "Combinações UF-mês-produto disponíveis no mart estadual."),
        (
            "gold_mercado_municipio_anual",
            mercado_municipio.count(),
            "Combinações município-ano-produto disponíveis no mart municipal.",
        ),
    ],
    ["etapa", "quantidade_linhas", "descricao"],
).withColumn("lote_id", F.lit(LOTE_ID)).withColumn(
    "data_execucao", F.to_timestamp(F.lit(EXECUTED_AT))
)
write_delta("gold_reconciliacao_etapas", reconciliacao_etapas)

print(f"Transformação concluída para o lote {LOTE_ID}.")
display(reconciliacao_etapas.orderBy("etapa"))
