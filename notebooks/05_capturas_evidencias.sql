-- Databricks SQL
USE CATALOG workspace;
USE SCHEMA anp_combustiveis_br_2022_2024;

-- 02_bronze_lote.png
SELECT lote_id, tabela, fonte, quantidade_linhas, data_ingestao
FROM bronze_lote_carga
WHERE lote_id = 'anp_br_20260909T081743Z_9b2d128e'
ORDER BY tabela;

-- 04_reconciliacao_etapas.png
SELECT lote_id, etapa, quantidade_linhas, data_execucao
FROM gold_reconciliacao_etapas
ORDER BY data_execucao DESC, etapa;

-- 05_catalogo_atributos.png
SELECT coluna, tipo_spark, qtd_nulos, qtd_distintos, min_observado, max_observado
FROM gold_catalogo_atributos
WHERE tabela = 'silver_venda_empresa_uf_mes'
  AND coluna IN (
    'data_referencia', 'uf', 'produto_analitico', 'volume_liquido_litros',
    'ajuste_negativo'
  )
ORDER BY tabela, coluna;

-- 06_qualidade.png
SELECT regra_id, tabela, qtd_afetada, pct_afetada, status
FROM gold_resultado_regra_qualidade
ORDER BY status, regra_id;

-- 07_ranking_vendedores.png
WITH vendas AS (
  SELECT ano, uf, produto_analitico, empresa_canonica,
         SUM(volume_liquido_litros) AS volume_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY ano, uf, produto_analitico, empresa_canonica
), ranking AS (
  SELECT *,
         100.0 * volume_litros / SUM(volume_litros) OVER (
           PARTITION BY ano, uf, produto_analitico
         ) AS participacao_pct,
         ROW_NUMBER() OVER (
           PARTITION BY ano, uf, produto_analitico
           ORDER BY volume_litros DESC, empresa_canonica
         ) AS posicao
  FROM vendas
)
SELECT ano, uf, posicao, empresa_canonica,
       ROUND(volume_litros / 1000000.0, 3) AS volume_milhoes_litros,
       ROUND(participacao_pct, 3) AS participacao_pct
FROM ranking
WHERE ano = 2023
  AND uf = 'DF'
  AND produto_analitico = 'ETANOL_HIDRATADO'
  AND posicao <= 3
ORDER BY posicao;

-- 08_concentracao_uf.png
WITH vendas_empresa_ano AS (
  SELECT ano, uf, produto_analitico, empresa_canonica,
         SUM(volume_liquido_litros) AS volume_anual_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY ano, uf, produto_analitico, empresa_canonica
), participacao_anual AS (
  SELECT *,
         100.0 * volume_anual_litros / SUM(volume_anual_litros) OVER (
           PARTITION BY ano, uf, produto_analitico
         ) AS participacao_pct,
         ROW_NUMBER() OVER (
           PARTITION BY ano, uf, produto_analitico
           ORDER BY volume_anual_litros DESC, empresa_canonica
         ) AS posicao
  FROM vendas_empresa_ano
), concentracao AS (
  SELECT ano, uf, produto_analitico,
         COUNT(*) AS vendedores_positivos,
         SUM(POWER(participacao_pct / 100.0, 2) * 10000) AS hhi,
         SUM(CASE WHEN posicao <= 3 THEN participacao_pct ELSE 0 END) AS participacao_top3_pct,
         MAX(CASE WHEN posicao = 1 THEN participacao_pct END) AS participacao_lider_pct
  FROM participacao_anual
  GROUP BY ano, uf, produto_analitico
)
SELECT uf, vendedores_positivos, ROUND(hhi, 0) AS hhi,
       ROUND(participacao_top3_pct, 2) AS participacao_top3_pct,
       ROUND(participacao_lider_pct, 2) AS participacao_lider_pct,
       CASE WHEN hhi < 1500 THEN 'BAIXA_CONCENTRACAO'
            WHEN hhi < 2500 THEN 'CONCENTRACAO_MODERADA'
            ELSE 'ALTA_CONCENTRACAO' END AS faixa_hhi
FROM concentracao
WHERE ano = 2022
  AND produto_analitico = 'ETANOL_HIDRATADO'
  AND uf IN ('AP', 'MG')
ORDER BY uf;

-- 09_preco_volume.png
SELECT ano, produto_analitico, COUNT(*) AS observacoes_uf_mes,
       ROUND(CORR(LOG(1 + volume_total_liquido_uf_litros), mediana_preco), 4) AS corr_log_volume_preco,
       ROUND(CORR(hhi, mediana_preco), 4) AS corr_hhi_preco,
       ROUND(CORR(participacao_top3_pct, mediana_preco), 4) AS corr_top3_preco
FROM gold_mart_mercado_uf_mes
WHERE volume_disponivel AND preco_disponivel
GROUP BY ano, produto_analitico
ORDER BY ano, produto_analitico;

-- 10_reconciliacao_volume.png
SELECT uf, meses_logistica_observados,
       ROUND(volume_logistica_litros / 1000000.0, 3) AS vol_log_milhoes,
       ROUND(volume_municipal_litros / 1000000.0, 3) AS vol_mun_milhoes,
       ROUND(diferenca_pct, 3) AS diferenca_pct,
       status_conciliacao
FROM gold_reconciliacao_volume_uf_ano
WHERE ano = 2022
  AND grupo_reconciliacao_municipal = 'ETANOL_HIDRATADO'
  AND uf IN ('MG', 'MT')
ORDER BY uf;

-- 11_cobertura_municipal.png
SELECT ano, produto_analitico,
       SUM(municipios_com_venda) AS municipios_com_venda,
       SUM(municipios_com_preco) AS municipios_com_preco,
       ROUND(100.0 * SUM(municipios_com_preco) / NULLIF(SUM(municipios_com_venda), 0), 2) AS cobertura_preco_pct
FROM gold_cobertura_pesquisa
GROUP BY ano, produto_analitico
ORDER BY ano, produto_analitico;

-- 12_rede_bandeiras.png
SELECT data_extracao, uf, bandeira, qtd_revendas,
       ROUND(participacao_rede_pct, 2) AS participacao_rede_pct
FROM gold_fato_rede_bandeira_uf_snapshot
WHERE uf = 'AM'
QUALIFY ROW_NUMBER() OVER (PARTITION BY data_extracao, uf ORDER BY qtd_revendas DESC, bandeira) <= 10
ORDER BY data_extracao DESC, qtd_revendas DESC;

-- 13_grupos_vendedores.png
WITH vendas AS (
  SELECT ano, uf, produto_analitico, grupo_economico,
         SUM(volume_liquido_litros) AS volume_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
    AND grupo_economico IN ('Vibra', 'Ipiranga', 'Raízen', 'Ale')
  GROUP BY ano, uf, produto_analitico, grupo_economico
), totais AS (
  SELECT ano, uf, produto_analitico, SUM(volume_liquido_litros) AS total_litros
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY ano, uf, produto_analitico
)
SELECT v.grupo_economico,
       ROUND(v.volume_litros / 1000000.0, 3) AS volume_milhoes_litros,
       ROUND(100.0 * v.volume_litros / t.total_litros, 3) AS participacao_pct
FROM vendas v
JOIN totais t USING (ano, uf, produto_analitico)
WHERE v.ano = 2023
  AND v.uf = 'DF'
  AND v.produto_analitico = 'ETANOL_HIDRATADO'
ORDER BY participacao_pct DESC;

-- 14_preco_uf.png
WITH resumo AS (
  SELECT uf,
         ROUND(PERCENTILE_APPROX(mediana_preco, 0.50, 10000), 3) AS mediana_preco,
         ROUND(AVG(spread_p90_p10), 3) AS spread_medio_p90_p10,
         COUNT(DISTINCT data_referencia) AS meses_com_preco,
         SUM(qtd_coletas) AS qtd_coletas,
         ROUND(AVG(qtd_postos), 1) AS postos_medio_mes
  FROM gold_mart_mercado_uf_mes
  WHERE preco_disponivel
    AND ano = 2024
    AND produto_analitico = 'ETANOL_HIDRATADO'
  GROUP BY uf
), faixa AS (
  SELECT *,
         ROW_NUMBER() OVER (ORDER BY mediana_preco DESC, uf) AS posicao_maior_preco,
         ROW_NUMBER() OVER (ORDER BY mediana_preco, uf) AS posicao_menor_preco
  FROM resumo
)
SELECT uf, mediana_preco, spread_medio_p90_p10, meses_com_preco, qtd_coletas, postos_medio_mes
FROM faixa
WHERE posicao_maior_preco <= 3 OR posicao_menor_preco <= 3
ORDER BY mediana_preco DESC, uf;

-- 15_ranking_mensal.png
WITH ranking_mensal AS (
  SELECT data_referencia, uf, produto_analitico, empresa_canonica,
         SUM(volume_liquido_litros) AS volume_litros,
         100.0 * SUM(volume_liquido_litros) / SUM(SUM(volume_liquido_litros)) OVER (
           PARTITION BY data_referencia, uf, produto_analitico
         ) AS participacao_pct,
         ROW_NUMBER() OVER (
           PARTITION BY data_referencia, uf, produto_analitico
           ORDER BY SUM(volume_liquido_litros) DESC, empresa_canonica
         ) AS posicao
  FROM gold_fato_venda_empresa_uf_mes
  WHERE volume_liquido_litros > 0
  GROUP BY data_referencia, uf, produto_analitico, empresa_canonica
)
SELECT DATE_FORMAT(data_referencia, 'yyyy-MM') AS mes, uf, posicao, empresa_canonica,
       ROUND(volume_litros / 1000000.0, 3) AS volume_milhoes_litros,
       ROUND(participacao_pct, 3) AS participacao_pct
FROM ranking_mensal
WHERE data_referencia = DATE '2023-12-01'
  AND uf = 'DF'
  AND produto_analitico = 'ETANOL_HIDRATADO'
  AND posicao <= 3
ORDER BY posicao;

-- 16_extremos_preco_dispersao_cobertura.png
WITH resumo AS (
  SELECT uf,
         ROUND(PERCENTILE_APPROX(mediana_preco, 0.50, 10000), 3) AS mediana_preco,
         ROUND(AVG(spread_p90_p10), 3) AS spread_medio_p90_p10,
         SUM(qtd_coletas) AS qtd_coletas
  FROM gold_mart_mercado_uf_mes
  WHERE preco_disponivel
    AND ano = 2024
    AND produto_analitico = 'ETANOL_HIDRATADO'
  GROUP BY uf
), faixa AS (
  SELECT *,
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

-- 03_tabelas_gold.png
SELECT table_name, table_type
FROM workspace.information_schema.tables
WHERE table_schema = 'anp_combustiveis_br_2022_2024'
ORDER BY table_name;
