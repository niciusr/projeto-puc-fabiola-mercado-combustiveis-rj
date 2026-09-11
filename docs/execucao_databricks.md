# Roteiro de execução no Databricks

## 1. Organizar o Volume

Use o Volume já criado e mantenha esta estrutura:

```text
/Volumes/workspace/anp_rj_2022_2024/anp/
├── raw/
│   ├── precos/       # seis ZIPs semestrais
│   ├── vendas/       # gasolina C e etanol por município
│   ├── logistica/    # movimentacaologistica.zip
│   └── cadastro/     # cadastro atual de revendedores
├── config/           # produto_mapeamento.csv, empresa_grupo_mapeamento.csv e catalogo_atributos.csv
└── metadata/         # gerado pelo notebook 00
```

O nome físico desse Volume foi herdado do ambiente inicial. O projeto nacional é identificado pelo schema `workspace.anp_combustiveis_br_2022_2024`, pelo lote e pelos filtros de execução; não pelo nome do diretório.

O notebook 00 extrai os CSVs de preços em `raw/precos/extraidos/` e extrai somente a Logística 02 em `raw/logistica/extraidos/vendas_mercado_brasileiro.csv`. Não envie arquivos brutos ao GitHub.

## 2. Schema e widgets

O schema novo evita misturar o projeto nacional com o protótipo anterior:

```text
workspace.anp_combustiveis_br_2022_2024
```

| Notebook | Widget | Valor padrão |
|---|---|---|
| 00 | `database` | `workspace.anp_combustiveis_br_2022_2024` |
| 00 | `raw_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/raw` |
| 00 | `metadata_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/metadata` |
| 01 | `database` | `workspace.anp_combustiveis_br_2022_2024` |
| 01 | `raw_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/raw` |
| 01 | `config_root` | `/Volumes/workspace/anp_rj_2022_2024/anp/config` |
| 02 | `database` | `workspace.anp_combustiveis_br_2022_2024` |
| 02 | `ano_inicial`, `ano_final` | `2022`, `2024` |
| 02 | `min_postos`, `min_semanas` | `3`, `4` |
| 02 | `lote_id` | vazio para usar o último lote Bronze |
| 03 | `database`, `config_root` | mesmos valores de 01 |
| 03 | `perfil_completo` | `true` para gerar o catálogo final |

Use um compute com Delta Lake e permissão para criar tabelas no schema e ler o Volume.

## 3. Ordem de execução

1. Execute `00_preparar_arquivos_anp.py` e confirme o manifesto de preparação.
2. Execute `01_ingestao_bronze.py`. Registre o `lote_id` e as contagens das sete fontes.
3. Execute `02_transformacao_modelo.py`. Ele cria as Silver, dimensões, fatos, mart estadual, mart municipal, reconciliação e outliers.
4. Execute `03_qualidade_dados.py` com perfil completo. Ele cria o catálogo observado e as regras de qualidade.
5. Abra `04_analises.sql` em uma consulta SQL para as análises completas.
6. Abra `05_capturas_evidencias.sql` para produzir as saídas filtradas usadas nas capturas da entrega.

Se o raw ou uma configuração mudar, execute novamente as etapas posteriores. A Bronze mantém lotes anteriores; as tabelas Silver e Gold representam o lote selecionado no notebook 02.

## 4. Conferências rápidas

Depois do notebook 01:

```sql
SELECT lote_id, tabela, fonte, quantidade_linhas, data_ingestao
FROM workspace.anp_combustiveis_br_2022_2024.bronze_lote_carga
ORDER BY data_ingestao DESC, tabela;
```

Depois do notebook 02:

```sql
SELECT etapa, quantidade_linhas, descricao
FROM workspace.anp_combustiveis_br_2022_2024.gold_reconciliacao_etapas
ORDER BY etapa;

SELECT ano, produto_analitico, COUNT(*) AS combinacoes_uf_mes
FROM workspace.anp_combustiveis_br_2022_2024.gold_mart_mercado_uf_mes
GROUP BY ano, produto_analitico
ORDER BY ano, produto_analitico;
```

Depois do notebook 03:

```sql
SELECT status, COUNT(*) AS regras, SUM(qtd_afetada) AS ocorrencias
FROM workspace.anp_combustiveis_br_2022_2024.gold_resultado_regra_qualidade
GROUP BY status
ORDER BY status;
```

## 5. Evidências e texto final

Use somente números do lote registrado no schema. Para cada conclusão, informe período, UF, produto, unidade, quantidade de observações e limite de interpretação. O checklist de capturas está em [evidencias.md](evidencias.md) e o roteiro de discussão está em [discussao_resultados.md](discussao_resultados.md).
