-- Databricks SQL
-- Ajuste o catálogo e o schema se os widgets dos notebooks usarem outro destino.
USE CATALOG workspace;
USE SCHEMA anp_rj_2022_2024;

-- 1. Fechamento da carga e das regras de qualidade.
SELECT *
FROM gold_reconciliacao_etapas
ORDER BY data_execucao DESC, etapa;

SELECT regra_id, tabela, descricao, qtd_afetada, pct_afetada, status
FROM gold_resultado_regra_qualidade
ORDER BY status, regra_id;

-- 2. Cobertura da pesquisa diante dos municípios com potencial de comparação.
SELECT
  ano,
  produto_analitico,
  COUNT(*) AS municipios_no_universo,
  SUM(CASE WHEN preco_disponivel THEN 1 ELSE 0 END) AS municipios_com_preco,
  SUM(CASE WHEN cobertura_suficiente THEN 1 ELSE 0 END) AS municipios_publicaveis,
  ROUND(100.0 * SUM(CASE WHEN preco_disponivel THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_cobertura
FROM gold_cobertura_pesquisa
GROUP BY ano, produto_analitico
ORDER BY ano, produto_analitico;

-- 3. Ranking de preço mediano com a cobertura mínima exigida.
WITH base AS (
  SELECT
    m.ano,
    m.produto_analitico,
    m.municipio,
    m.mediana_preco_anual,
    m.preco_relativo_estado,
    m.semanas_pesquisadas,
    m.postos_distintos,
    ROW_NUMBER() OVER (
      PARTITION BY m.ano, m.produto_analitico
      ORDER BY m.mediana_preco_anual DESC
    ) AS posicao_mais_caro,
    ROW_NUMBER() OVER (
      PARTITION BY m.ano, m.produto_analitico
      ORDER BY m.mediana_preco_anual ASC
    ) AS posicao_mais_barato
  FROM gold_fato_mercado_municipio_anual m
  WHERE m.publicar_analise
)
SELECT *
FROM base
WHERE posicao_mais_caro <= 10 OR posicao_mais_barato <= 10
ORDER BY ano, produto_analitico, mediana_preco_anual DESC;

-- 4. Relação descritiva entre volume, preço e dispersão.
SELECT
  produto_analitico,
  ano,
  COUNT(*) AS municipios_publicaveis,
  ROUND(CORR(LOG(1 + volume_litros), mediana_preco_anual), 4) AS corr_log_volume_preco,
  ROUND(CORR(LOG(1 + volume_litros), preco_relativo_estado), 4) AS corr_log_volume_preco_relativo,
  ROUND(CORR(LOG(1 + volume_litros), spread_medio_p90_p10), 4) AS corr_log_volume_spread,
  ROUND(CORR(LOG(1 + volume_litros), cv_medio_preco_pct), 4) AS corr_log_volume_cv
FROM gold_fato_mercado_municipio_anual
WHERE publicar_analise
GROUP BY produto_analitico, ano
ORDER BY produto_analitico, ano;

-- 5. Casos atípicos: preço relativo ao estado versus volume relativo ao produto-ano.
WITH base AS (
  SELECT *
  FROM gold_fato_mercado_municipio_anual
  WHERE publicar_analise
), referencia AS (
  SELECT
    produto_analitico,
    ano,
    percentile_approx(volume_litros, 0.5, 10000) AS mediana_volume
  FROM base
  GROUP BY produto_analitico, ano
)
SELECT
  b.ano,
  b.produto_analitico,
  b.municipio,
  b.volume_litros,
  b.mediana_preco_anual,
  b.preco_relativo_estado,
  CASE
    WHEN b.volume_litros >= r.mediana_volume AND b.preco_relativo_estado >= 1 THEN 'volume alto / preço alto'
    WHEN b.volume_litros >= r.mediana_volume AND b.preco_relativo_estado < 1 THEN 'volume alto / preço baixo'
    WHEN b.volume_litros < r.mediana_volume AND b.preco_relativo_estado >= 1 THEN 'volume baixo / preço alto'
    ELSE 'volume baixo / preço baixo'
  END AS quadrante
FROM base b
JOIN referencia r USING (produto_analitico, ano)
ORDER BY ano, produto_analitico, quadrante, preco_relativo_estado DESC;

-- 6. Bandeira comparada com a mediana do mesmo município, semana e produto.
WITH referencia_local AS (
  SELECT
    codigo_ibge,
    produto_analitico,
    data_coleta,
    percentile_approx(preco_venda, 0.5, 10000) AS mediana_local
  FROM gold_fato_preco_coletado
  WHERE preco_valido
    AND selecionado_para_agregacao
    AND municipio_conciliado
    AND produto_analitico IN ('GASOLINA_C', 'ETANOL_HIDRATADO')
  GROUP BY codigo_ibge, produto_analitico, data_coleta
), desvios AS (
  SELECT
    p.produto_analitico,
    COALESCE(p.bandeira, 'NAO INFORMADA') AS bandeira,
    p.preco_venda - r.mediana_local AS desvio_preco_local
  FROM gold_fato_preco_coletado p
  JOIN referencia_local r
    ON p.codigo_ibge = r.codigo_ibge
   AND p.produto_analitico = r.produto_analitico
   AND p.data_coleta = r.data_coleta
  WHERE p.preco_valido AND p.selecionado_para_agregacao AND p.municipio_conciliado
)
SELECT
  produto_analitico,
  bandeira,
  COUNT(*) AS observacoes,
  ROUND(AVG(desvio_preco_local), 4) AS desvio_medio_da_mediana_local,
  ROUND(percentile_approx(desvio_preco_local, 0.5, 10000), 4) AS desvio_mediano_da_mediana_local
FROM desvios
GROUP BY produto_analitico, bandeira
HAVING COUNT(*) >= 30
ORDER BY produto_analitico, desvio_medio_da_mediana_local DESC;

-- 7. Outliers são sinalizados para revisão, nunca apagados do dado bruto.
SELECT
  ano,
  produto_analitico,
  COUNT(*) AS observacoes_validas,
  SUM(CASE WHEN outlier_iqr_avaliavel THEN 1 ELSE 0 END) AS observacoes_iqr_avaliaveis,
  SUM(CASE WHEN outlier_iqr THEN 1 ELSE 0 END) AS outliers_iqr,
  ROUND(
    100.0 * SUM(CASE WHEN outlier_iqr THEN 1 ELSE 0 END)
      / NULLIF(SUM(CASE WHEN outlier_iqr_avaliavel THEN 1 ELSE 0 END), 0),
    3
  ) AS pct_outliers
FROM gold_preco_outlier
GROUP BY ano, produto_analitico
ORDER BY ano, produto_analitico;

-- 8. O cadastro atual é uma fotografia; esta consulta não infere status histórico.
SELECT
  cnpj_observado_historico,
  COUNT(*) AS revendedores_no_snapshot
FROM gold_fato_cadastro_revenda_snapshot
GROUP BY cnpj_observado_historico
ORDER BY cnpj_observado_historico DESC;

-- 9. Catálogo observado: use para documentar nulos, domínios e categorias reais.
SELECT
  tabela,
  coluna,
  tipo_spark,
  total_registros,
  qtd_nulos,
  pct_nulos,
  qtd_distintos,
  min_observado,
  max_observado,
  categorias_frequentes,
  dominio_esperado,
  origem_linhagem
FROM gold_catalogo_atributos
ORDER BY tabela, coluna;

-- 10. Síntese objetiva para a abertura da apresentação do projeto.
SELECT
  (SELECT SUM(quantidade_linhas) FROM gold_dim_lote_carga) AS linhas_em_bronze,
  (SELECT COUNT(*) FROM gold_fato_preco_coletado) AS observacoes_preco,
  (SELECT COUNT(*) FROM gold_fato_mercado_municipio_anual) AS municipios_produto_ano,
  (SELECT COUNT(*) FROM gold_resultado_regra_qualidade WHERE status = 'APROVADA') AS regras_aprovadas,
  (SELECT COUNT(*) FROM gold_resultado_regra_qualidade WHERE status = 'ATENCAO') AS regras_em_atencao;
