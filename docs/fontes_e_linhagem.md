# Fontes e linhagem dos dados

## Inventário das fontes

| Origem | Arquivo ou endpoint | Período usado | Chave/granularidade | Destino principal |
|---|---|---|---|---|
| Pesquisa semanal de preços | ZIPs semestrais de combustíveis automotivos | 2022–2024 | CNPJ × produto × data | `bronze_preco` → `silver_preco_coletado` |
| Vendas municipais de gasolina C | CSV anual por município | 2022–2024 | código IBGE × ano | `bronze_venda_gasolina_c` → `silver_venda_municipio` |
| Vendas municipais de etanol hidratado | CSV anual por município | 2022–2024 | código IBGE × ano | `bronze_venda_etanol` → `silver_venda_municipio` |
| Cadastro de revendedores | CSV cadastral da ANP | fotografia atual | CNPJ × data de extração | `bronze_cadastro_revenda` → `gold_fato_cadastro_revenda_snapshot` |
| Mapeamento de produto | arquivo versionado pelo projeto | vigente na execução | fonte × produto de origem | `bronze_ref_produto_mapeamento` → `gold_dim_produto` |

Links oficiais:

- [Série histórica de preços](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)
- [Metadados da série de preços](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/metadados-serie-historica-precos-combustiveis.pdf)
- [Vendas de derivados e biocombustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis)
- [Cadastro de revendedores](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos)

O arquivo `data/metadata/download_manifest.json`, gerado na coleta, registra URL, data/hora, nome local, tamanho e SHA-256. O arquivo `source_manifest.json`, gerado na inspeção, acrescenta codificação, delimitador, colunas e número de linhas. Os dois devem ser preservados como evidência da carga.

## Fluxo de linhagem

```text
ANP: ZIP/CSV de preços ──┐
ANP: CSV de vendas ──────┼──> Bronze: cópia bruta + lote + arquivo + data de ingestão
ANP: cadastro atual ─────┘
                                   │
                                   ▼
                    Silver: tipos, CNPJ, datas, produtos e municípios normalizados
                                   │
                      ┌────────────┴─────────────┐
                      ▼                          ▼
        dimensões conformadas                fatos por preço, semana e venda
                      └────────────┬─────────────┘
                                   ▼
              Gold: mercado anual, cobertura, outliers, perfil e regras de qualidade
```

## Transformações rastreáveis

| Regra | Motivo | Evidência/tabela |
|---|---|---|
| Remover caracteres não numéricos do CNPJ | a chave chega com formatações diferentes | `cnpj`, `cnpj_formato_valido` |
| Converter vírgula decimal para decimal Spark | preços e vendas são publicados no formato brasileiro | `preco_venda`, `volume_litros` |
| Normalizar município com maiúsculas e sem acentos | a pesquisa não traz código IBGE | `municipio_norm`, `municipio_conciliado` |
| Usar vendas como referência municipal | o CSV de vendas possui código IBGE | `gold_dim_municipio` |
| Mapear produto por tabela de referência | evita regra escondida em código | `config/produto_mapeamento.csv` |
| Agregar preço primeiro por semana | reduz peso de semanas com mais coletas | `gold_fato_preco_municipio_semana` |
| Separar cadastro atual do histórico | evita inferência temporal indevida | `gold_fato_cadastro_revenda_snapshot` |

## Regra de integração principal

O cruzamento de mercado usa apenas as combinações marcadas como `usar_cruzamento_principal = true` no mapeamento:

```text
codigo_ibge + ano + produto_analitico
```

Antes dessa junção, o preço é agregado por município-produto-semana e depois por município-produto-ano. Registros sem município conciliado, produto mapeado ou cobertura suficiente permanecem nas tabelas de qualidade, mas não são publicados em rankings comparativos.

O denominador da cobertura parte de município-produto-ano com venda válida. Portanto, um município sem venda de etanol em determinado ano não é contado como “sem pesquisa de preço” para aquele produto.

## Pontos que não devem ser confundidos

- O nome de município da pesquisa é usado apenas para localizar o código IBGE da dimensão.
- CNPJ é uma chave pública de negócio, mas não é necessário expor identificadores individuais nos gráficos.
- A fotografia atual do cadastro pode ser ligada ao CNPJ para conferir cobertura, porém não representa o status de um posto em 2022, 2023 ou 2024.
