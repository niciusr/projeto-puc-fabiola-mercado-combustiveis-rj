# Roteiro de execução no Databricks

## 1. Preparar o Volume

Crie ou escolha um Volume com duas pastas:

```text
/Volumes/workspace/anp_rj_2022_2024/anp/
├── raw/
│   ├── precos/     # seis ZIPs semestrais
│   ├── vendas/     # dois CSVs municipais
│   └── cadastro/   # CSV do retrato atual de revendedores
└── config/          # os dois CSVs de configuração do projeto
```

Não envie o diretório do projeto inteiro nem dados para o GitHub. Envie somente os nove arquivos de origem e os dois CSVs da configuração. O notebook `00_preparar_arquivos_anp.py` valida os arquivos, calcula os hashes e extrai os ZIPs em `raw/precos/extraidos/`.

## 2. Importar e configurar

Importe os arquivos da pasta `notebooks/` como notebooks do Databricks. Antes de executar, confirme os widgets:

| Notebook | Widget | Exemplo |
|---|---|---|
| 00 | `database` | `workspace.anp_rj_2022_2024` |
| 00 | `raw_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/raw` |
| 00 | `metadata_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/metadata` |
| 01 | `database` | `workspace.anp_rj_2022_2024` |
| 01 | `raw_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/raw` |
| 01 | `config_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/config` |
| 02 | `database` | `workspace.anp_rj_2022_2024` |
| 02 | `ano_inicial`, `ano_final` | `2022`, `2024` |
| 02 | `min_postos`, `min_semanas` | `3`, `4` |
| 02 | `lote_id` | deixe vazio para usar o último lote Bronze |
| 03 | `database`, `config_root` | mesmos valores dos anteriores |
| 03 | `perfil_completo` | `true` para a entrega final |

Use um cluster com Delta Lake habilitado. Em Unity Catalog, o usuário precisa ter permissão para criar tabelas no schema escolhido e ler o Volume.

## 3. Ordem de execução

1. `00_preparar_arquivos_anp.py` valida os arquivos enviados, registra hashes e extrai os ZIPs de preços.
2. `01_ingestao_bronze.py` cria as cópias brutas e a tabela de lote.
3. `02_transformacao_modelo.py` cria Silver, dimensões, fatos, mart de mercado, cobertura e outliers.
4. `03_qualidade_dados.py` produz o catálogo observado e os resultados das regras.
5. Abra `04_analises.sql` em uma consulta SQL para responder às perguntas de negócio. Na execução deste projeto, a consulta foi salva como **04 - Análises Gerenciais ANP RJ**.

Se uma etapa for reexecutada, execute novamente as etapas posteriores. As tabelas analíticas são recriadas para o lote atual; `bronze_lote_carga` mantém um histórico dos lotes carregados.

## 4. Conferências antes de analisar

Execute estas verificações logo após o notebook 02:

```sql
SELECT etapa, quantidade_linhas, descricao
FROM gold_reconciliacao_etapas
ORDER BY etapa;

SELECT produto_analitico, ano, COUNT(*) AS linhas
FROM gold_fato_mercado_municipio_anual
GROUP BY produto_analitico, ano
ORDER BY produto_analitico, ano;
```

Depois do notebook 03, confira as regras em atenção. Atenção não significa que a carga falhou: pode ser uma limitação real da fonte, como município com venda, mas sem pesquisa de preço. Esse caso deve aparecer na discussão.

## 5. Resultados e texto final

Use apenas números extraídos das tabelas Gold nas conclusões. Para cada gráfico ou ranking, informe:

- período e produto;
- regra de cobertura aplicada;
- número de municípios analisados;
- se a medida é mediana, média, percentual ou correlação;
- limitação que impede interpretação causal.

Não complete resultados antes da execução. O arquivo `docs/evidencias.md` indica o que guardar como prova de cada etapa.
