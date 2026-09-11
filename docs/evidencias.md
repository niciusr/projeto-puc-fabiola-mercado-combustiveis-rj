# Evidências para a entrega

As evidências abaixo foram geradas ou refeitas no Databricks para a versão nacional. As capturas usam o schema `workspace.anp_combustiveis_br_2022_2024` e o lote `anp_br_20260909T081743Z_9b2d128e`. As consultas filtradas que produziram as telas estão em `notebooks/05_capturas_evidencias.sql`.

## Evidências obrigatórias

| Ordem | Evidência registrada | Origem | Arquivo |
|---:|---|---|---|
| 1 | Volume com as pastas `raw`, `config` e `metadata` | Data Explorer | [01_volume_arquivos.png](capturas_databricks/01_volume_arquivos.png) |
| 2 | Lote Bronze, fontes e contagens carregadas | `bronze_lote_carga` | [02_bronze_lote.png](capturas_databricks/02_bronze_lote.png) |
| 3 | 42 tabelas Bronze, Silver e Gold no schema nacional | Data Explorer e consulta de inventário | [03_tabelas_gold.png](capturas_databricks/03_tabelas_gold.png) |
| 4 | Reconciliação das etapas e lote de execução | `gold_reconciliacao_etapas` | [04_reconciliacao_etapas.png](capturas_databricks/04_reconciliacao_etapas.png) |
| 5 | Perfil observado de atributos de uma tabela Silver | `gold_catalogo_atributos` | [05_catalogo_atributos.png](capturas_databricks/05_catalogo_atributos.png) |
| 6 | Regras de qualidade e seus status | `gold_resultado_regra_qualidade` | [06_qualidade.png](capturas_databricks/06_qualidade.png) |
| 7 | Ranking anual de apoio, DF e etanol em 2023 | consulta 2A | [07_ranking_vendedores.png](capturas_databricks/07_ranking_vendedores.png) |
| 8 | HHI, Top 3 e participação do líder anual, AP e MG | consulta 4 | [08_concentracao_uf.png](capturas_databricks/08_concentracao_uf.png) |
| 9 | Relação descritiva entre preço, volume e concentração | consulta 7 | [09_preco_volume.png](capturas_databricks/09_preco_volume.png) |
| 10 | Reconciliação anual de volumes, MG e MT | consulta 8 | [10_reconciliacao_volume.png](capturas_databricks/10_reconciliacao_volume.png) |
| 11 | Cobertura de preços nos municípios com venda | consulta 9 | [11_cobertura_municipal.png](capturas_databricks/11_cobertura_municipal.png) |
| 12 | Fotografia atual da rede de bandeiras no AM | consulta 11 | [12_rede_bandeiras.png](capturas_databricks/12_rede_bandeiras.png) |
| 13 | Participação anual de Vibra, Ipiranga, Raízen e ALE no DF | consulta 3 | [13_grupos_vendedores.png](capturas_databricks/13_grupos_vendedores.png) |
| 14 | Mediana, dispersão, meses, coletas e postos do etanol por UF em 2024 | consulta 5 | [14_preco_uf.png](capturas_databricks/14_preco_uf.png) |
| 15 | Ranking mensal, DF e etanol em dezembro de 2023 | consulta 2 | [15_ranking_mensal.png](capturas_databricks/15_ranking_mensal.png) |
| 16 | Extremos de preço, dispersão e coletas no etanol em 2024 | consulta 5A | [16_extremos_preco_dispersao_cobertura.png](capturas_databricks/16_extremos_preco_dispersao_cobertura.png) |

## Registro do lote

Os valores abaixo foram extraídos do ambiente após a execução:

| Campo | Valor |
|---|---|
| Schema | `workspace.anp_combustiveis_br_2022_2024` |
| Lote Bronze | `anp_br_20260909T081743Z_9b2d128e` |
| Data da carga | `2026-09-09T18:54:22Z` |
| Período observado da Logística 02 | janeiro de 2022 a dezembro de 2024 |
| Linhas na Bronze | 225.398 na Logística 02 e 2.710.038 na série de preços; as etapas consolidadas estão na captura 04 |
| Regras aprovadas, em atenção e informativas | 19 aprovadas, 0 em atenção e 7 informativas |
| Consultas SQL utilizadas | `notebooks/04_analises.sql` e `notebooks/05_capturas_evidencias.sql` |

O nome físico do Volume, `anp_rj_2022_2024`, foi mantido do ambiente de trabalho original. O escopo efetivo é definido pelo schema, pelo lote, pelos filtros e pelas tabelas nacionais acima; o nome do Volume não é uma dimensão analítica.

## Leitura curta da execução

- Foram publicadas 42 tabelas no schema nacional e 464 registros no catálogo observado de atributos.
- O mart estadual tem 1.944 combinações de UF, mês e produto; o mart municipal anual tem 30.117 combinações.
- As sete regras informativas preservam limitações ou eventos que precisam aparecer na análise, como 941 linhas sem UF de destino válida, 337 ajustes negativos, cobertura parcial da pesquisa de preços e 43.790 outliers sinalizados por IQR. Nenhum desses casos foi apagado da camada de origem.

## Check de qualidade das imagens

- Não mostrar e-mail, token, URL com credencial ou dados pessoais fora do escopo.
- Deixar visíveis o nome da tabela, o filtro e as colunas que sustentam a conclusão.
- Em uma análise de vendedor, deixar visíveis UF, produto, período e unidade de medida.
- Em uma análise de preço, deixar visível a cobertura ou quantidade de postos e coletas.
- Em uma reconciliação, deixar visíveis os dois volumes, o percentual de diferença e o status.
- Nomear as imagens com a ordem acima e registrar no README ou apresentação de onde vieram.

## Evidências de coleta

Os manifestos locais `data/metadata/download_manifest.json` e `data/metadata/source_manifest.json` não entram no Git porque acompanham os arquivos brutos. O resumo versionado de URL, data de acesso e SHA-256 está em [registro_coleta.md](registro_coleta.md), junto com os termos de uso consultados.
