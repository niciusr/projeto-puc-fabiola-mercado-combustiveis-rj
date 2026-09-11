-- Databricks SQL
USE CATALOG workspace;
USE SCHEMA anp_combustiveis_br_2022_2024;

-- 1. Fechamento da carga e das regras de qualidade.
SELECT *
FROM gold_reconciliacao_etapas
ORDER BY data_execucao DESC, etapa;

SELECT regra_id, tabela, descricao, qtd_afetada, pct_afetada, status
FROM gold_resultado_regra_qualidade
ORDER BY status, regra_id;

-- 2. Ranking mensal geral de vendedores por UF.
WITH vendas_mes AS (
  SELECT
    data_referencia,
    uf,
    produto_analitico,
    empresa_canonica,
    grupo_economico,
    SUM(volume_liquido_litros) AS volume_mensal_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY data_referencia, uf, produto_analitico, empresa_canonica, grupo_economico
), ranking AS (
  SELECT
    *,
    100.0 * volume_mensal_litros / SUM(volume_mensal_litros) OVER (
      PARTITION BY data_referencia, uf, produto_analitico
    ) AS participacao_pct,
    ROW_NUMBER() OVER (
      PARTITION BY data_referencia, uf, produto_analitico
      ORDER BY volume_mensal_litros DESC, empresa_canonica
    ) AS posicao
  FROM vendas_mes
)
SELECT
  data_referencia,
  uf,
  produto_analitico,
  posicao,
  empresa_canonica,
  grupo_economico,
  ROUND(volume_mensal_litros, 0) AS volume_mensal_litros,
  ROUND(participacao_pct, 3) AS participacao_pct
FROM ranking
WHERE posicao <= 10
ORDER BY data_referencia, produto_analitico, uf, posicao;

-- 2A. Ranking anual de apoio para leitura consolidada.
WITH vendas_ano AS (
  SELECT
    ano,
    uf,
    produto_analitico,
    empresa_canonica,
    grupo_economico,
    SUM(volume_liquido_litros) AS volume_anual_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY ano, uf, produto_analitico, empresa_canonica, grupo_economico
), totais AS (
  SELECT
    ano,
    uf,
    produto_analitico,
    SUM(volume_anual_litros) AS volume_total_litros
  FROM vendas_ano
  GROUP BY ano, uf, produto_analitico
), ranking AS (
  SELECT
    v.*,
    100.0 * v.volume_anual_litros / NULLIF(t.volume_total_litros, 0) AS participacao_pct,
    ROW_NUMBER() OVER (
      PARTITION BY v.ano, v.uf, v.produto_analitico
      ORDER BY v.volume_anual_litros DESC, v.empresa_canonica
    ) AS posicao
  FROM vendas_ano v
  JOIN totais t USING (ano, uf, produto_analitico)
)
SELECT
  ano,
  uf,
  produto_analitico,
  posicao,
  empresa_canonica,
  grupo_economico,
  ROUND(volume_anual_litros, 0) AS volume_anual_litros,
  ROUND(participacao_pct, 3) AS participacao_pct
FROM ranking
WHERE posicao <= 10
ORDER BY ano, produto_analitico, uf, posicao;

-- 3. Participação anual de Vibra, Ipiranga, Raízen e Ale por UF.
WITH vendas_ano AS (
  SELECT
    ano,
    uf,
    produto_analitico,
    empresa_canonica,
    grupo_economico,
    SUM(volume_liquido_litros) AS volume_anual_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
    AND grupo_economico IN ('Vibra', 'Ipiranga', 'Raízen', 'Ale')
  GROUP BY ano, uf, produto_analitico, empresa_canonica, grupo_economico
), totais AS (
  SELECT
    ano,
    uf,
    produto_analitico,
    SUM(volume_liquido_litros) AS volume_total_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY ano, uf, produto_analitico
)
SELECT
  v.ano,
  v.uf,
  v.produto_analitico,
  v.empresa_canonica,
  v.grupo_economico,
  ROUND(v.volume_anual_litros, 0) AS volume_anual_litros,
  ROUND(100.0 * v.volume_anual_litros / NULLIF(t.volume_total_litros, 0), 3) AS participacao_pct
FROM vendas_ano v
JOIN totais t USING (ano, uf, produto_analitico)
ORDER BY v.ano, v.produto_analitico, v.uf, participacao_pct DESC;

-- 4. Líderes e concentração anual por UF.
WITH vendas_empresa_ano AS (
  SELECT
    ano,
    uf,
    produto_analitico,
    empresa_canonica,
    SUM(volume_liquido_litros) AS volume_anual_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY ano, uf, produto_analitico, empresa_canonica
), participacao_anual AS (
  SELECT
    *,
    100.0 * volume_anual_litros / SUM(volume_anual_litros) OVER (
      PARTITION BY ano, uf, produto_analitico
    ) AS participacao_pct,
    ROW_NUMBER() OVER (
      PARTITION BY ano, uf, produto_analitico
      ORDER BY volume_anual_litros DESC, empresa_canonica
    ) AS posicao
  FROM vendas_empresa_ano
), concentracao_anual AS (
  SELECT
    ano,
    uf,
    produto_analitico,
    SUM(volume_anual_litros) AS volume_anual_litros,
    COUNT(*) AS vendedores_positivos,
    SUM(POWER(participacao_pct / 100.0, 2) * 10000) AS hhi,
    SUM(CASE WHEN posicao <= 3 THEN participacao_pct ELSE 0 END) AS participacao_top3_pct,
    MAX(CASE WHEN posicao = 1 THEN empresa_canonica END) AS empresa_lider_anual,
    MAX(CASE WHEN posicao = 1 THEN participacao_pct END) AS participacao_lider_anual_pct
  FROM participacao_anual
  GROUP BY ano, uf, produto_analitico
)
SELECT
  ano,
  uf,
  produto_analitico,
  ROUND(volume_anual_litros, 0) AS volume_anual_litros,
  vendedores_positivos,
  ROUND(hhi, 0) AS hhi,
  ROUND(participacao_top3_pct, 2) AS participacao_top3_pct,
  empresa_lider_anual,
  ROUND(participacao_lider_anual_pct, 2) AS participacao_lider_anual_pct,
  CASE
    WHEN hhi < 1500 THEN 'BAIXA_CONCENTRACAO'
    WHEN hhi < 2500 THEN 'CONCENTRACAO_MODERADA'
    ELSE 'ALTA_CONCENTRACAO'
  END AS faixa_hhi
FROM concentracao_anual
ORDER BY ano, produto_analitico, hhi DESC, uf;

-- 5. Resumo anual de preço por UF, com dispersão e cobertura.
SELECT
  ano,
  uf,
  produto_analitico,
  ROUND(PERCENTILE_APPROX(mediana_preco, 0.50, 10000), 3) AS mediana_das_medianas_mensais,
  ROUND(AVG(spread_p90_p10), 3) AS spread_medio_p90_p10,
  ROUND(STDDEV_POP(mediana_preco), 3) AS desvio_das_medianas_mensais,
  COUNT(DISTINCT data_referencia) AS meses_com_preco,
  ROUND(100.0 * COUNT(DISTINCT data_referencia) / 12, 1) AS cobertura_meses_pct,
  SUM(qtd_coletas) AS qtd_coletas,
  ROUND(AVG(qtd_postos), 1) AS postos_medio_mes,
  ROUND(AVG(qtd_municipios), 1) AS municipios_medio_mes
FROM gold_mart_mercado_uf_mes
WHERE preco_disponivel
GROUP BY ano, uf, produto_analitico
ORDER BY ano, produto_analitico, mediana_das_medianas_mensais DESC, uf;

-- 5A. Extremos anuais de preço, dispersão e coletas por UF.
WITH resumo AS (
  SELECT
    uf,
    ROUND(PERCENTILE_APPROX(mediana_preco, 0.50, 10000), 3) AS mediana_preco,
    ROUND(AVG(spread_p90_p10), 3) AS spread_medio_p90_p10,
    SUM(qtd_coletas) AS qtd_coletas
  FROM gold_mart_mercado_uf_mes
  WHERE preco_disponivel
    AND ano = 2024
    AND produto_analitico = 'ETANOL_HIDRATADO'
  GROUP BY uf
), faixa AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY mediana_preco DESC, uf) AS maior_preco,
    ROW_NUMBER() OVER (ORDER BY mediana_preco, uf) AS menor_preco,
    ROW_NUMBER() OVER (ORDER BY spread_medio_p90_p10 DESC, uf) AS maior_spread,
    ROW_NUMBER() OVER (ORDER BY spread_medio_p90_p10, uf) AS menor_spread,
    ROW_NUMBER() OVER (ORDER BY qtd_coletas DESC, uf) AS maior_cobertura,
    ROW_NUMBER() OVER (ORDER BY qtd_coletas, uf) AS menor_cobertura
  FROM resumo
), extremos AS (
  SELECT 'PRECO_MEDIANO' AS indicador, 'MAIOR' AS direcao, uf, mediana_preco AS valor
  FROM faixa WHERE maior_preco = 1
  UNION ALL
  SELECT 'PRECO_MEDIANO', 'MENOR', uf, mediana_preco FROM faixa WHERE menor_preco = 1
  UNION ALL
  SELECT 'SPREAD_P90_P10', 'MAIOR', uf, spread_medio_p90_p10 FROM faixa WHERE maior_spread = 1
  UNION ALL
  SELECT 'SPREAD_P90_P10', 'MENOR', uf, spread_medio_p90_p10 FROM faixa WHERE menor_spread = 1
  UNION ALL
  SELECT 'COLETAS_ANUAIS', 'MAIOR', uf, CAST(qtd_coletas AS DOUBLE) FROM faixa WHERE maior_cobertura = 1
  UNION ALL
  SELECT 'COLETAS_ANUAIS', 'MENOR', uf, CAST(qtd_coletas AS DOUBLE) FROM faixa WHERE menor_cobertura = 1
)
SELECT indicador, direcao, uf, valor
FROM extremos
ORDER BY indicador, direcao DESC;

-- 6. Evolução mensal de volume, preço e concentração.
SELECT
  data_referencia,
  uf,
  produto_analitico,
  ROUND(volume_total_liquido_uf_litros, 0) AS volume_liquido_litros,
  vendedores_positivos,
  ROUND(hhi, 0) AS hhi,
  ROUND(participacao_top3_pct, 2) AS participacao_top3_pct,
  empresa_lider,
  ROUND(mediana_preco, 3) AS mediana_preco,
  ROUND(spread_p90_p10, 3) AS spread_p90_p10,
  qtd_postos,
  qtd_municipios
FROM gold_mart_mercado_uf_mes
WHERE volume_disponivel OR preco_disponivel
ORDER BY produto_analitico, uf, data_referencia;

-- 7. Relação descritiva entre preço e volume por UF-mês.
SELECT
  ano,
  produto_analitico,
  COUNT(*) AS observacoes_uf_mes,
  ROUND(CORR(LOG(1 + volume_total_liquido_uf_litros), mediana_preco), 4) AS corr_log_volume_preco,
  ROUND(CORR(hhi, mediana_preco), 4) AS corr_hhi_preco,
  ROUND(CORR(participacao_top3_pct, mediana_preco), 4) AS corr_top3_preco
FROM gold_mart_mercado_uf_mes
WHERE volume_disponivel AND preco_disponivel
GROUP BY ano, produto_analitico
ORDER BY ano, produto_analitico;

-- 8. Reconciliação entre as vendas por vendedor e o volume municipal.
SELECT
  ano,
  uf,
  grupo_reconciliacao_municipal,
  meses_logistica_observados,
  ROUND(volume_logistica_litros, 0) AS volume_logistica_litros,
  ROUND(volume_municipal_litros, 0) AS volume_municipal_litros,
  ROUND(diferenca_litros, 0) AS diferenca_litros,
  ROUND(diferenca_pct, 3) AS diferenca_pct,
  status_conciliacao
FROM gold_reconciliacao_volume_uf_ano
ORDER BY ano, grupo_reconciliacao_municipal, ABS(diferenca_pct) DESC, uf;

-- 9. Cobertura da pesquisa de preços nos municípios com venda publicada.
SELECT
  ano,
  produto_analitico,
  SUM(municipios_com_venda) AS municipios_com_venda,
  SUM(municipios_com_preco) AS municipios_com_preco,
  SUM(municipios_publicaveis) AS municipios_publicaveis,
  ROUND(100.0 * SUM(municipios_com_preco) / NULLIF(SUM(municipios_com_venda), 0), 2) AS cobertura_preco_pct
FROM gold_cobertura_pesquisa
GROUP BY ano, produto_analitico
ORDER BY ano, produto_analitico;

-- 10. Municípios com preço e volume comparáveis.
SELECT
  m.ano,
  m.uf,
  d.municipio,
  m.produto_analitico,
  ROUND(m.volume_litros, 0) AS volume_litros,
  ROUND(m.mediana_preco_anual, 3) AS mediana_preco_anual,
  ROUND(m.preco_relativo_uf, 3) AS preco_relativo_uf,
  m.semanas_pesquisadas,
  m.postos_distintos
FROM gold_fato_mercado_municipio_anual m
JOIN gold_dim_municipio d USING (codigo_ibge, uf)
WHERE m.publicar_analise
ORDER BY m.ano, m.produto_analitico, m.uf, m.mediana_preco_anual DESC;

-- 11. Fotografia atual da rede por bandeira.
SELECT
  data_extracao,
  uf,
  bandeira,
  qtd_revendas,
  ROUND(participacao_rede_pct, 2) AS participacao_rede_pct
FROM gold_fato_rede_bandeira_uf_snapshot
QUALIFY ROW_NUMBER() OVER (PARTITION BY data_extracao, uf ORDER BY qtd_revendas DESC, bandeira) <= 10
ORDER BY data_extracao DESC, uf, qtd_revendas DESC;

-- 12. Catálogo observado para a documentação da entrega.
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

-- 13. Inventário do schema entregue.
SELECT table_name, table_type
FROM workspace.information_schema.tables
WHERE table_schema = 'anp_combustiveis_br_2022_2024'
ORDER BY table_name;
