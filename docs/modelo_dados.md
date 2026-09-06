# Modelo de dados

O modelo é uma constelação de fatos com dimensões conformadas. A tabela de mercado anual é um mart analítico; ela não substitui as tabelas detalhadas que permitem auditoria.

```text
dim_tempo ─────────────┐
dim_municipio ─────────┼── fato_preco_coletado ── fato_preco_municipio_semana
dim_produto ───────────┤                                  │
dim_revenda ───────────┘                                  ▼
                                         fato_preco_municipio_anual
                                                     │
fato_venda_municipio_anual ─────────────────────────┼── fato_mercado_municipio_anual
                                                     │
fato_cadastro_revenda_snapshot ─────────────────────┘  (uso contextual, sem inferência histórica)
```

## Dimensões

| Tabela | Chave | Conteúdo |
|---|---|---|
| `gold_dim_tempo` | `data` | ano, mês, semana, trimestre e semestre |
| `gold_dim_municipio` | `codigo_ibge` | município, município normalizado, UF e grande região |
| `gold_dim_produto` | `produto_analitico` | produto canônico, família, compatibilidades de origem e uso no cruzamento |
| `gold_ref_mapeamento_produto` | fonte × produto de origem | ponte entre o produto publicado e o produto canônico |
| `gold_dim_revenda` | `cnpj` | revenda observada, bandeira, município, UF e intervalo de coleta |
| `gold_dim_bandeira` | `bandeira` | bandeira normalizada e indicador de bandeira branca |
| `gold_dim_lote_carga` | `lote_id` | fonte, tabela carregada, data de ingestão e quantidade de linhas |
| `gold_ref_conciliacao_municipio` | município de origem × UF | resultado da ligação por nome normalizado e código IBGE |

## Fatos e granularidades

| Tabela | Granularidade | Métricas e uso |
|---|---|---|
| `gold_fato_preco_coletado` | CNPJ × produto × data de coleta | preço de venda, flags de validade, duplicidade e conciliação |
| `gold_fato_preco_municipio_semana` | código IBGE × produto × `semana_inicio` | mediana, média, percentis, CV, amplitude, postos e coletas |
| `gold_fato_preco_municipio_anual` | código IBGE × produto × ano | mediana das semanas, dispersão, cobertura e preço relativo estadual |
| `gold_fato_venda_municipio_anual` | código IBGE × produto × ano | volume anual em litros e flag de chave duplicada |
| `gold_fato_mercado_municipio_anual` | código IBGE × produto × ano | volume, preço, cobertura e disponibilidade das duas fontes |
| `gold_fato_cadastro_revenda_snapshot` | CNPJ × data de extração | razão social, endereço, bandeira, vínculo e presença no histórico de preços |

## Chaves e integridade

- A chave natural de preço é `cnpj + produto_origem_norm + data_coleta`. Repetições permanecem na Silver e no fato detalhado; para as agregações, uma linha é escolhida de forma determinística por arquivo de origem e preço.
- A chave da venda municipal é `codigo_ibge + produto_analitico + ano`.
- O mart de mercado é construído por junção externa entre preço anual e vendas para evidenciar lacunas de cobertura.
- `codigo_ibge` e `produto_analitico` são as chaves conformadas entre as fontes.
- Tabelas Bronze, Silver e fatos detalhados recebem `lote_id` e `data_ingestao`. Agregações mantêm `lote_id`; dimensões derivadas são rastreáveis pelo lote e pelo manifesto de origem.

## Estratégia de agregação de preço

1. Preservar o valor por posto e data na tabela detalhada;
2. calcular estatísticas por município-produto-semana;
3. usar a mediana semanal como insumo da medida anual;
4. guardar semanas pesquisadas, postos distintos e quantidade de coletas junto do resultado anual.

Essa sequência reduz o risco de um município parecer mais importante apenas porque foi visitado mais vezes em uma semana.
