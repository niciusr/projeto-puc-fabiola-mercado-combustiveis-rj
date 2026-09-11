# Capturas do Databricks

As imagens numeradas de `01_` a `16_` foram geradas ou refeitas na execução nacional do lote `anp_br_20260909T081743Z_9b2d128e`, no schema `workspace.anp_combustiveis_br_2022_2024`.

| Arquivo da execução nacional | Evidência |
|---|---|
| `01_volume_arquivos.png` | organização do Volume |
| `02_bronze_lote.png` | lote Bronze, fontes e contagens carregadas |
| `04_reconciliacao_etapas.png` | reconciliação das etapas de transformação |
| `03_tabelas_gold.png` | 42 tabelas publicadas no schema |
| `05_catalogo_atributos.png` e `06_qualidade.png` | perfil por atributo e regras de qualidade |
| `07_ranking_vendedores.png` a `16_extremos_preco_dispersao_cobertura.png` | respostas analíticas das consultas SQL, com filtros visíveis |

Os arquivos históricos abaixo pertencem ao protótipo inicial, restrito ao Rio de Janeiro. Foram preservados como histórico e **não são evidência da versão nacional**.

| Arquivo histórico | Escopo a que pertence |
|---|---|
| `01_reconciliacao_etapas.png` | protótipo RJ de preço e venda municipal |
| `02_regras_qualidade.png` e `02b_status_qualidade.png` | protótipo RJ de qualidade |
| `03_cobertura_pesquisa.png` e `03b_cobertura_percentual.png` | protótipo RJ de cobertura municipal |
| `04_ranking_etanol_2022.png` | protótipo RJ de ranking de preço |

Consulte [evidencias.md](../evidencias.md) para o vínculo entre cada imagem, a tabela de origem e a conclusão apresentada. As capturas de análise foram feitas por consultas filtradas para que UF, período, produto e colunas de resultado apareçam na própria tela. O nome físico do Volume foi preservado do ambiente original; não representa o recorte da análise nacional.
