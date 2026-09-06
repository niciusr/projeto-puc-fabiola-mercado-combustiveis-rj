# Databricks notebook source
"""Transforma os dados Bronze em tabelas Silver e Gold do projeto."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DecimalType


# COMMAND ----------

dbutils.widgets.text("database", "workspace.anp_rj_2022_2024", "Database")
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

if ANO_INICIAL > ANO_FINAL:
    raise ValueError("O ano inicial não pode ser maior que o ano final.")
if not DATABASE:
    raise ValueError("Informe o schema no widget database.")


def table(name: str) -> str:
    return f"{DATABASE}.{name}"


def write_delta(name: str, frame: DataFrame) -> None:
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table(name))


if not LOTE_ID:
    ultimo_lote = (
        spark.table(table("bronze_lote_carga"))
        .orderBy(F.col("data_ingestao").desc(), F.col("lote_id").desc())
        .select("lote_id")
        .first()
    )
    if ultimo_lote is None:
        raise ValueError("Não há lote Bronze. Execute primeiro o notebook 01.")
    LOTE_ID = ultimo_lote["lote_id"]


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


def normalized_text(column) -> F.Column:
    return F.upper(F.trim(F.translate(F.coalesce(column.cast("string"), F.lit("")), ACCENTED, PLAIN)))


def brazilian_decimal(column) -> F.Column:
    raw = F.trim(column.cast("string"))
    standardized = F.regexp_replace(F.regexp_replace(raw, r"\.", ""), ",", ".")
    return F.when(raw == "", F.lit(None)).otherwise(standardized.cast(DecimalType(18, 3)))


def pick(frame: DataFrame, aliases: list[str], label: str, required: bool = True) -> F.Column:
    for alias in aliases:
        if alias in frame.columns:
            return F.col(alias)
    if required:
        raise ValueError(f"A coluna {label} não foi encontrada. Colunas disponíveis: {frame.columns}")
    return F.lit(None).cast("string")


def date_from_anp(column) -> F.Column:
    text = F.trim(column.cast("string"))
    return F.coalesce(F.to_date(text, "dd/MM/yyyy"), F.to_date(text, "yyyy-MM-dd"))


def normalized_cnpj(column) -> F.Column:
    return F.regexp_replace(F.coalesce(column.cast("string"), F.lit("")), r"\D", "")


def add_duplicate_flag(frame: DataFrame, key_column: str) -> DataFrame:
    window = Window.partitionBy(key_column)
    return frame.withColumn("duplicidade_chave", F.count(F.lit(1)).over(window) > F.lit(1))


# COMMAND ----------

produto_ref = normalize_headers(bronze_lote("bronze_ref_produto_mapeamento"))
produto_ref = (
    produto_ref.select(
        normalized_text(pick(produto_ref, ["fonte"], "fonte")).alias("fonte"),
        normalized_text(
            pick(produto_ref, ["produto_origem_normalizado"], "produto_origem_normalizado")
        ).alias("produto_origem_normalizado"),
        normalized_text(pick(produto_ref, ["produto_analitico"], "produto_analitico")).alias("produto_analitico"),
        normalized_text(pick(produto_ref, ["familia"], "familia")).alias("familia"),
        F.lower(F.trim(pick(produto_ref, ["compatibilidade_analitica"], "compatibilidade_analitica"))).alias(
            "compatibilidade_analitica"
        ),
        F.lower(F.trim(pick(produto_ref, ["usar_cruzamento_principal"], "usar_cruzamento_principal"))).cast(
            "boolean"
        ).alias("usar_cruzamento_principal"),
        pick(produto_ref, ["observacao"], "observacao", required=False).alias("observacao"),
    )
    .dropDuplicates(["fonte", "produto_origem_normalizado"])
)


def prepare_sales(frame: DataFrame, default_product: str) -> DataFrame:
    source = normalize_headers(frame)
    product = F.coalesce(pick(source, ["produto"], "produto", required=False), F.lit(default_product))
    result = source.select(
        F.col("lote_id"),
        F.col("arquivo_origem"),
        F.col("data_ingestao"),
        F.col("fonte_carga"),
        F.trim(pick(source, ["ano"], "ANO").cast("string")).cast("int").alias("ano"),
        normalized_text(pick(source, ["grande_regiao"], "GRANDE REGIÃO", required=False)).alias("grande_regiao"),
        normalized_text(pick(source, ["uf"], "UF")).alias("uf"),
        F.regexp_replace(pick(source, ["codigo_ibge"], "CÓDIGO IBGE").cast("string"), r"\.0$", "").alias(
            "codigo_ibge"
        ),
        F.trim(pick(source, ["municipio"], "MUNICÍPIO")).alias("municipio"),
        F.trim(product).alias("produto_origem"),
        brazilian_decimal(pick(source, ["vendas"], "VENDAS")).alias("volume_litros"),
    )
    return result.filter(
        (F.col("uf") == "RJ") & F.col("ano").between(ANO_INICIAL, ANO_FINAL)
    ).withColumn("municipio_norm", normalized_text(F.col("municipio"))).withColumn(
        "produto_origem_normalizado", normalized_text(F.col("produto_origem"))
    )


venda_gasolina = prepare_sales(bronze_lote("bronze_venda_gasolina_c"), "GASOLINA C")
venda_etanol = prepare_sales(bronze_lote("bronze_venda_etanol_hidratado"), "ETANOL HIDRATADO")
venda_base = venda_gasolina.unionByName(venda_etanol, allowMissingColumns=True)

mapa_venda = produto_ref.filter(F.col("fonte") == "VENDAS_MUNICIPAIS").select(
    "produto_origem_normalizado",
    "produto_analitico",
    "familia",
    "compatibilidade_analitica",
    "usar_cruzamento_principal",
)

venda_silver = venda_base.join(mapa_venda, "produto_origem_normalizado", "left")
venda_silver = (
    venda_silver.withColumn("produto_analitico", F.coalesce(F.col("produto_analitico"), F.lit("NAO_MAPEADO")))
    .withColumn("familia", F.coalesce(F.col("familia"), F.lit("NAO_MAPEADA")))
    .withColumn("compatibilidade_analitica", F.coalesce(F.col("compatibilidade_analitica"), F.lit("nao_mapeado")))
    .withColumn("usar_cruzamento_principal", F.coalesce(F.col("usar_cruzamento_principal"), F.lit(False)))
    .withColumn("volume_valido", F.col("volume_litros").isNotNull() & (F.col("volume_litros") >= 0))
    .withColumn(
        "chave_venda",
        F.sha2(F.concat_ws("||", "codigo_ibge", "produto_analitico", F.col("ano").cast("string")), 256),
    )
)
venda_silver = add_duplicate_flag(venda_silver, "chave_venda")

write_delta("silver_venda_municipio", venda_silver)

# A venda já traz o código IBGE e vira a referência geográfica do modelo.
dim_municipio = (
    venda_silver.filter(F.col("codigo_ibge").rlike(r"^\d{7}$"))
    .groupBy("codigo_ibge", "municipio_norm", "uf")
    .agg(
        F.first("municipio", ignorenulls=True).alias("municipio"),
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
    F.col("lote_id"),
    F.col("arquivo_origem"),
    F.col("data_ingestao"),
    F.col("fonte_carga"),
    normalized_text(pick(preco_raw, ["estado_sigla"], "Estado - Sigla")).alias("uf"),
    F.trim(pick(preco_raw, ["municipio"], "Municipio")).alias("municipio_origem"),
    F.trim(pick(preco_raw, ["revenda"], "Revenda")).alias("revenda"),
    normalized_cnpj(pick(preco_raw, ["cnpj_da_revenda"], "CNPJ da Revenda")).alias("cnpj"),
    F.trim(pick(preco_raw, ["produto"], "Produto")).alias("produto_origem"),
    date_from_anp(pick(preco_raw, ["data_da_coleta"], "Data da Coleta")).alias("data_coleta"),
    brazilian_decimal(pick(preco_raw, ["valor_de_venda"], "Valor de Venda")).alias("preco_venda"),
    F.trim(pick(preco_raw, ["unidade_de_medida"], "Unidade de Medida")).alias("unidade_medida"),
    F.trim(pick(preco_raw, ["bandeira"], "Bandeira", required=False)).alias("bandeira"),
    F.concat_ws(
        " ",
        F.trim(pick(preco_raw, ["nome_da_rua"], "Nome da Rua", required=False)),
        F.trim(pick(preco_raw, ["numero_rua"], "Numero Rua", required=False)),
    ).alias("endereco"),
    F.trim(pick(preco_raw, ["bairro"], "Bairro", required=False)).alias("bairro"),
    F.trim(pick(preco_raw, ["cep"], "Cep", required=False)).alias("cep"),
).filter(F.col("uf") == "RJ")

preco_base = (
    preco_base.withColumn("municipio_norm", normalized_text(F.col("municipio_origem")))
    .withColumn("produto_origem_normalizado", normalized_text(F.col("produto_origem")))
    .withColumn("ano", F.year("data_coleta"))
    .withColumn("semana_inicio", F.to_date(F.date_trunc("week", F.col("data_coleta"))))
    .withColumn("semana", F.weekofyear("semana_inicio"))
    .filter(F.col("ano").between(ANO_INICIAL, ANO_FINAL))
)

mapa_preco = produto_ref.filter(F.col("fonte") == "PRECOS").select(
    "produto_origem_normalizado",
    "produto_analitico",
    "familia",
    "compatibilidade_analitica",
    "usar_cruzamento_principal",
)

preco_silver = preco_base.join(
    municipio_match,
    (F.col("municipio_norm") == F.col("municipio_norm_ref")) & (F.col("uf") == F.col("uf_ref")),
    "left",
).drop("municipio_norm_ref", "uf_ref")
preco_silver = preco_silver.join(mapa_preco, "produto_origem_normalizado", "left")
preco_silver = (
    preco_silver.withColumn("produto_analitico", F.coalesce(F.col("produto_analitico"), F.lit("NAO_MAPEADO")))
    .withColumn("familia", F.coalesce(F.col("familia"), F.lit("NAO_MAPEADA")))
    .withColumn("compatibilidade_analitica", F.coalesce(F.col("compatibilidade_analitica"), F.lit("nao_mapeado")))
    .withColumn("usar_cruzamento_principal", F.coalesce(F.col("usar_cruzamento_principal"), F.lit(False)))
    .withColumn("cnpj_formato_valido", F.length("cnpj") == 14)
    .withColumn("municipio_conciliado", F.col("codigo_ibge").isNotNull())
    .withColumn("preco_valido", F.col("preco_venda").isNotNull() & (F.col("preco_venda") > 0))
    .withColumn(
        "chave_preco",
        F.sha2(F.concat_ws("||", "cnpj", "produto_origem_normalizado", F.col("data_coleta").cast("string")), 256),
    )
)
preco_silver = add_duplicate_flag(preco_silver, "chave_preco")
janela_deduplicacao = Window.partitionBy("chave_preco").orderBy(
    F.col("arquivo_origem").asc(), F.col("preco_venda").asc_nulls_last()
)
preco_silver = preco_silver.withColumn("_ordem_chave_preco", F.row_number().over(janela_deduplicacao)).withColumn(
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
    .withColumn(
        "regra_conciliacao",
        F.lit("UF + municipio normalizado contra a referência de vendas"),
    )
)
write_delta("gold_ref_conciliacao_municipio", conciliacao_municipio)

# COMMAND ----------

ref_mapeamento_produto = produto_ref.select(
    "fonte",
    "produto_origem_normalizado",
    "produto_analitico",
    "familia",
    "compatibilidade_analitica",
    "usar_cruzamento_principal",
    "observacao",
).dropDuplicates()
dim_produto = (
    ref_mapeamento_produto.groupBy("produto_analitico")
    .agg(
        F.first("familia", ignorenulls=True).alias("familia"),
        F.concat_ws(" | ", F.sort_array(F.collect_set("compatibilidade_analitica"))).alias("compatibilidades_origem"),
        F.max(F.col("usar_cruzamento_principal").cast("int")).cast("boolean").alias("usar_cruzamento_principal"),
    )
)

revenda_intervalo = (
    preco_silver.filter(F.length("cnpj") > 0)
    .groupBy("cnpj")
    .agg(F.min("data_coleta").alias("primeira_coleta"), F.max("data_coleta").alias("ultima_coleta"))
)
revenda_mais_recente = (
    preco_silver.filter(F.length("cnpj") > 0)
    .withColumn("_ordem", F.row_number().over(Window.partitionBy("cnpj").orderBy(F.col("data_coleta").desc_nulls_last())))
    .filter(F.col("_ordem") == 1)
    .select(
        "cnpj",
        "cnpj_formato_valido",
        "revenda",
        "bandeira",
        "municipio_origem",
        "municipio_norm",
        "codigo_ibge",
        "uf",
    )
)
dim_revenda = (
    revenda_mais_recente.join(revenda_intervalo, "cnpj", "left")
    .withColumn("bandeira_branca", normalized_text(F.col("bandeira")) == "BANDEIRA BRANCA")
)
dim_bandeira = (
    preco_silver.select(F.coalesce(F.col("bandeira"), F.lit("NAO INFORMADA")).alias("bandeira"))
    .distinct()
    .withColumn("bandeira_branca", normalized_text(F.col("bandeira")) == "BANDEIRA BRANCA")
)

date_limits = preco_silver.agg(F.min("data_coleta").alias("inicio"), F.max("data_coleta").alias("fim")).first()
if not date_limits["inicio"] or not date_limits["fim"]:
    raise ValueError("Não há datas válidas na pesquisa de preços após o filtro do recorte.")

dim_tempo = spark.sql(
    "SELECT explode(sequence("
    f"DATE '{date_limits['inicio']}', DATE '{date_limits['fim']}', INTERVAL 1 DAY)) AS data"
)
dim_tempo = (
    dim_tempo.withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("semana", F.weekofyear("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("semestre", F.when(F.col("mes") <= 6, F.lit(1)).otherwise(F.lit(2)))
)

write_delta("gold_dim_tempo", dim_tempo)
write_delta("gold_dim_municipio", dim_municipio)
write_delta("gold_dim_produto", dim_produto)
write_delta("gold_ref_mapeamento_produto", ref_mapeamento_produto)
write_delta("gold_dim_revenda", dim_revenda)
write_delta("gold_dim_bandeira", dim_bandeira)
write_delta("gold_fato_preco_coletado", preco_silver)
write_delta("gold_fato_venda_municipio_anual", venda_silver)

# COMMAND ----------

preco_agregavel = preco_silver.filter(
    F.col("preco_valido")
    & F.col("municipio_conciliado")
    & (F.col("produto_analitico") != "NAO_MAPEADO")
)
preco_publicavel = preco_agregavel.filter(F.col("selecionado_para_agregacao"))

preco_semana = (
    preco_publicavel.groupBy("codigo_ibge", "uf", "produto_analitico", "ano", "semana_inicio", "semana")
    .agg(
        F.count(F.lit(1)).alias("qtd_coletas"),
        F.countDistinct("cnpj").alias("qtd_postos"),
        F.expr("percentile_approx(preco_venda, 0.50, 10000)").alias("mediana_preco"),
        F.avg("preco_venda").alias("media_preco"),
        F.expr("percentile_approx(preco_venda, 0.25, 10000)").alias("p25_preco"),
        F.expr("percentile_approx(preco_venda, 0.75, 10000)").alias("p75_preco"),
        F.expr("percentile_approx(preco_venda, 0.90, 10000)").alias("p90_preco"),
        F.expr("percentile_approx(preco_venda, 0.10, 10000)").alias("p10_preco"),
        F.min("preco_venda").alias("min_preco"),
        F.max("preco_venda").alias("max_preco"),
        F.stddev_pop("preco_venda").alias("desvio_padrao_preco"),
        F.max("lote_id").alias("lote_id"),
    )
    .withColumn("amplitude_preco", F.col("max_preco") - F.col("min_preco"))
    .withColumn("spread_p90_p10", F.col("p90_preco") - F.col("p10_preco"))
    .withColumn(
        "cv_preco_pct",
        F.when(F.col("media_preco") != 0, F.col("desvio_padrao_preco") / F.col("media_preco") * 100),
    )
)
write_delta("gold_fato_preco_municipio_semana", preco_semana)

cobertura_anual = preco_publicavel.groupBy("codigo_ibge", "produto_analitico", "ano").agg(
    F.countDistinct("cnpj").alias("postos_distintos")
)
preco_anual_base = (
    preco_semana.groupBy("codigo_ibge", "uf", "produto_analitico", "ano")
    .agg(
        F.expr("percentile_approx(mediana_preco, 0.50, 10000)").alias("mediana_preco_anual"),
        F.avg("mediana_preco").alias("media_semanal_preco"),
        F.stddev_pop("mediana_preco").alias("desvio_entre_semanas"),
        F.min("mediana_preco").alias("min_mediana_semanal"),
        F.max("mediana_preco").alias("max_mediana_semanal"),
        F.avg("spread_p90_p10").alias("spread_medio_p90_p10"),
        F.avg("cv_preco_pct").alias("cv_medio_preco_pct"),
        F.countDistinct("semana_inicio").alias("semanas_pesquisadas"),
        F.sum("qtd_coletas").alias("qtd_coletas"),
        F.max("lote_id").alias("lote_id"),
    )
    .join(cobertura_anual, ["codigo_ibge", "produto_analitico", "ano"], "left")
)
referencia_estado = preco_anual_base.groupBy("produto_analitico", "ano").agg(
    F.expr("percentile_approx(mediana_preco_anual, 0.50, 10000)").alias("mediana_preco_estado")
)
preco_anual = (
    preco_anual_base.join(referencia_estado, ["produto_analitico", "ano"], "left")
    .withColumn(
        "cobertura_suficiente",
        (F.col("postos_distintos") >= MIN_POSTOS) & (F.col("semanas_pesquisadas") >= MIN_SEMANAS),
    )
    .withColumn(
        "preco_relativo_estado",
        F.when(F.col("mediana_preco_estado") > 0, F.col("mediana_preco_anual") / F.col("mediana_preco_estado")),
    )
    .drop("mediana_preco_estado")
)
write_delta("gold_fato_preco_municipio_anual", preco_anual)

# COMMAND ----------

produtos_principais = dim_produto.filter(F.col("usar_cruzamento_principal")).select("produto_analitico").distinct()
preco_principal = preco_anual.join(produtos_principais, "produto_analitico", "inner")

venda_natural = venda_silver.filter(F.col("volume_valido")).withColumn(
    "_ordem", F.row_number().over(Window.partitionBy("chave_venda").orderBy("arquivo_origem"))
)
venda_principal = (
    venda_natural.filter(F.col("_ordem") == 1)
    .join(produtos_principais, "produto_analitico", "inner")
    .select("codigo_ibge", "produto_analitico", "ano", "volume_litros", "lote_id")
)

mercado = venda_principal.alias("v").join(
    preco_principal.alias("p"),
    ["codigo_ibge", "produto_analitico", "ano"],
    "full_outer",
).select(
    F.col("codigo_ibge"),
    F.col("produto_analitico"),
    F.col("ano"),
    F.col("v.volume_litros").alias("volume_litros"),
    F.col("p.mediana_preco_anual").alias("mediana_preco_anual"),
    F.col("p.preco_relativo_estado").alias("preco_relativo_estado"),
    F.col("p.desvio_entre_semanas").alias("desvio_entre_semanas"),
    F.col("p.spread_medio_p90_p10").alias("spread_medio_p90_p10"),
    F.col("p.cv_medio_preco_pct").alias("cv_medio_preco_pct"),
    F.col("p.semanas_pesquisadas").alias("semanas_pesquisadas"),
    F.col("p.postos_distintos").alias("postos_distintos"),
    F.col("p.qtd_coletas").alias("qtd_coletas"),
    F.coalesce(F.col("p.cobertura_suficiente"), F.lit(False)).alias("cobertura_suficiente"),
    F.col("p.mediana_preco_anual").isNotNull().alias("possui_preco"),
    F.col("v.volume_litros").isNotNull().alias("possui_venda"),
    F.coalesce(F.col("p.lote_id"), F.col("v.lote_id")).alias("lote_id"),
)
mercado = mercado.join(dim_municipio.select("codigo_ibge", "municipio", "uf"), "codigo_ibge", "left")
mercado = mercado.withColumn(
    "publicar_analise",
    F.col("possui_preco") & F.col("possui_venda") & F.col("cobertura_suficiente"),
)
write_delta("gold_fato_mercado_municipio_anual", mercado)

cobertura = (
    venda_principal.select("codigo_ibge", "produto_analitico", "ano")
    .join(dim_municipio.select("codigo_ibge", "municipio", "uf"), "codigo_ibge", "left")
    .join(
        preco_principal.select(
            "codigo_ibge", "produto_analitico", "ano", "semanas_pesquisadas", "postos_distintos", "cobertura_suficiente"
        ),
        ["codigo_ibge", "produto_analitico", "ano"],
        "left",
    )
    .withColumn("preco_disponivel", F.col("semanas_pesquisadas").isNotNull())
    .withColumn("cobertura_suficiente", F.coalesce(F.col("cobertura_suficiente"), F.lit(False)))
)
write_delta("gold_cobertura_pesquisa", cobertura)

# COMMAND ----------

cadastro_raw = normalize_headers(bronze_lote("bronze_cadastro_revenda"))
cadastro = cadastro_raw.select(
    F.col("lote_id"),
    F.col("arquivo_origem"),
    F.col("data_ingestao"),
    F.trim(pick(cadastro_raw, ["codigoisimp"], "CODIGOISIMP", required=False)).alias("codigoisimp"),
    F.trim(pick(cadastro_raw, ["autorizacao"], "AUTORIZACAO", required=False)).alias("autorizacao"),
    date_from_anp(pick(cadastro_raw, ["datapublicacao"], "DATAPUBLICACAO", required=False)).alias("data_publicacao"),
    F.trim(pick(cadastro_raw, ["razaosocial"], "RAZAOSOCIAL", required=False)).alias("razao_social"),
    normalized_cnpj(pick(cadastro_raw, ["cnpj"], "CNPJ")).alias("cnpj"),
    F.trim(pick(cadastro_raw, ["endereco"], "ENDERECO", required=False)).alias("endereco"),
    F.trim(pick(cadastro_raw, ["complemento"], "COMPLEMENTO", required=False)).alias("complemento"),
    F.trim(pick(cadastro_raw, ["bairro"], "BAIRRO", required=False)).alias("bairro"),
    F.trim(pick(cadastro_raw, ["cep"], "CEP", required=False)).alias("cep"),
    normalized_text(pick(cadastro_raw, ["uf"], "UF")).alias("uf"),
    F.trim(pick(cadastro_raw, ["municipio"], "MUNICIPIO")).alias("municipio"),
    F.trim(pick(cadastro_raw, ["bandeira"], "BANDEIRA", required=False)).alias("bandeira"),
    date_from_anp(pick(cadastro_raw, ["datavinculacao"], "DATAVINCULACAO", required=False)).alias("data_vinculacao"),
).filter(F.col("uf") == "RJ")
cadastro = cadastro.withColumn("municipio_norm", normalized_text(F.col("municipio"))).withColumn(
    "cnpj_formato_valido", F.length("cnpj") == 14
)
cadastro = cadastro.join(dim_revenda.select("cnpj").distinct().withColumn("cnpj_observado_historico", F.lit(True)), "cnpj", "left")
cadastro = cadastro.withColumn("cnpj_observado_historico", F.coalesce(F.col("cnpj_observado_historico"), F.lit(False))).withColumn(
    "data_extracao", F.to_date("data_ingestao")
)
write_delta("gold_fato_cadastro_revenda_snapshot", cadastro)

bounds = preco_publicavel.groupBy("codigo_ibge", "produto_analitico", "ano").agg(
    F.count(F.lit(1)).alias("qtd_observacoes_grupo"),
    F.expr("percentile_approx(preco_venda, 0.25, 10000)").alias("p25_preco"),
    F.expr("percentile_approx(preco_venda, 0.75, 10000)").alias("p75_preco"),
)
preco_outlier = preco_publicavel.join(bounds, ["codigo_ibge", "produto_analitico", "ano"], "left")
preco_outlier = (
    preco_outlier.withColumn("limite_inferior", F.col("p25_preco") - 1.5 * (F.col("p75_preco") - F.col("p25_preco")))
    .withColumn("limite_superior", F.col("p75_preco") + 1.5 * (F.col("p75_preco") - F.col("p25_preco")))
    .select(
        "chave_preco",
        "codigo_ibge",
        "produto_analitico",
        "ano",
        "preco_venda",
        "p25_preco",
        "p75_preco",
        "qtd_observacoes_grupo",
        "limite_inferior",
        "limite_superior",
        (F.col("qtd_observacoes_grupo") >= 4).alias("outlier_iqr_avaliavel"),
        F.when(
            F.col("qtd_observacoes_grupo") >= 4,
            (F.col("preco_venda") < F.col("limite_inferior")) | (F.col("preco_venda") > F.col("limite_superior")),
        ).otherwise(F.lit(False)).alias("outlier_iqr"),
    )
)
write_delta("gold_preco_outlier", preco_outlier)

# COMMAND ----------

dim_lote_bronze = spark.table(table("bronze_lote_carga")).filter(F.col("lote_id") == LOTE_ID)
write_delta("gold_dim_lote_carga", dim_lote_bronze)

reconciliacao_rows = [
    ("preco_bronze", bronze_lote("bronze_preco").count(), "todas as linhas carregadas da pesquisa"),
    ("preco_rj_recorte", preco_base.count(), "linhas de preço após filtro de UF e ano"),
    ("preco_municipio_conciliado", preco_silver.filter("municipio_conciliado").count(), "linhas com código IBGE"),
    ("preco_valido", preco_silver.filter("preco_valido").count(), "linhas com preço positivo"),
    ("preco_deduplicado_gold", preco_publicavel.count(), "linhas usadas nas agregações após desempate da chave natural"),
    ("venda_rj_recorte", venda_silver.count(), "linhas de venda após filtro de UF e ano"),
    ("mercado_integrado", mercado.filter("possui_preco AND possui_venda").count(), "município-produto-ano com as duas fontes"),
]
reconciliacao = spark.createDataFrame(reconciliacao_rows, ["etapa", "quantidade_linhas", "descricao"])
reconciliacao = reconciliacao.withColumn("lote_id", F.lit(LOTE_ID)).withColumn(
    "data_execucao", F.lit(datetime.now(timezone.utc).replace(microsecond=0).isoformat()).cast("timestamp")
)
write_delta("gold_reconciliacao_etapas", reconciliacao)

print(f"Transformação concluída para o lote {LOTE_ID}.")
display(reconciliacao)
