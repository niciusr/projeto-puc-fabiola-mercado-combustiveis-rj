# Evidências para a entrega

As imagens e vídeos devem mostrar a execução real no ambiente em nuvem. A lista abaixo serve como checklist; não substitui as evidências.

## Registro da execução realizada

| Campo | Valor observado |
|---|---|
| Lote Bronze | `anp_rj_20260906T022801Z_5a797990` |
| Preços carregados na Bronze | 2.710.038 linhas |
| Preços válidos no recorte RJ 2022–2024 | 212.637 linhas |
| Preços usados nas agregações Gold | 212.631 linhas |
| Combinações município-produto-ano com venda | 552 |
| Combinações integradas de preço e venda | 194 |
| Regras de qualidade | 10 aprovadas e 4 em atenção |
| Consulta salva | `04 - Análises Gerenciais ANP RJ` |

## Capturas incluídas no repositório

As imagens abaixo foram registradas durante a execução no Databricks. Elas são
complementares: em telas largas, algumas colunas aparecem em uma segunda imagem
para manter os números legíveis.

| Evidência | Arquivos |
|---|---|
| Reconciliação das etapas do pipeline | [01_reconciliacao_etapas.png](capturas_databricks/01_reconciliacao_etapas.png) |
| Regras de qualidade e respectivos status | [02_regras_qualidade.png](capturas_databricks/02_regras_qualidade.png) e [02b_status_qualidade.png](capturas_databricks/02b_status_qualidade.png) |
| Cobertura da pesquisa por ano e produto | [03_cobertura_pesquisa.png](capturas_databricks/03_cobertura_pesquisa.png) e [03b_cobertura_percentual.png](capturas_databricks/03b_cobertura_percentual.png) |
| Ranking de preços de etanol em 2022 | [04_ranking_etanol_2022.png](capturas_databricks/04_ranking_etanol_2022.png) |

Veja também o [índice das capturas](capturas_databricks/README.md), com a
descrição de cada resultado exibido.

## Checklist complementar

| Item | Evidência a guardar | Onde obter |
|---|---|---|
| Coleta | `download_manifest.json` com URL, SHA-256 e data | `data/metadata/` |
| Inspeção | `source_manifest.json` com colunas, codificação e contagem de linhas | `data/metadata/` |
| Bronze | execução concluída e contagem por tabela/lote | notebook 01 e `bronze_lote_carga` |
| Silver | amostra de preço e venda com tipos, CNPJ normalizado e código IBGE | notebook 02 |
| Modelo | tabelas `gold_dim_*` e `gold_fato_*` visíveis no catálogo | interface do Databricks |
| Linhagem | diagrama de `docs/fontes_e_linhagem.md` e metadados do lote | documentação e tabela de lote |
| Qualidade | `gold_resultado_regra_qualidade` e `gold_catalogo_atributos` | notebook 03 |
| Cobertura | resultado de `gold_cobertura_pesquisa` | consulta 2 do SQL |
| Análise | ranking, correlação, quadrantes e bandeiras | consultas 3 a 6 do SQL |
| Limitações | município sem preço, baixa cobertura e snapshot atual | discussão final |

## Sequência sugerida de capturas adicionais

1. Página do Volume com as pastas `raw` e `config`.
2. Resultado do notebook 01 com o identificador do lote e as contagens.
3. Listagem das tabelas Delta criadas.
4. Resultado da reconciliação de etapas.
5. Perfil de atributos de uma tabela Silver e de uma Gold.
6. Tabela de regras de qualidade, incluindo pelo menos uma regra em atenção se ela existir.
7. Cobertura da pesquisa por ano e produto.
8. Um ranking de preço com postos e semanas, não apenas o valor de preço.
9. Um quadrante de preço relativo e volume, com o filtro `publicar_analise` visível.
10. Texto final com fontes, método, limitações e conclusões baseadas nas consultas.

Nomeie os arquivos de forma simples, por exemplo `01_bronze_lote.png`, `02_modelo_delta.png`, `03_qualidade.png` e `04_ranking_gasolina_2024.png`.
