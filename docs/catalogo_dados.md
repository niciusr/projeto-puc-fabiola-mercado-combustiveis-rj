# Catálogo de dados

O catálogo tem duas partes: este dicionário de negócio e a tabela `gold_catalogo_atributos`, produzida pelo notebook de qualidade. A tabela gerada registra, para **cada coluna** das tabelas Silver e Gold, tipo Spark, total de registros, nulos, distintos, mínimo, máximo e categorias mais frequentes na execução efetiva. As duas tabelas produzidas pelo próprio notebook de qualidade também são perfiladas no fim da execução; apenas o catálogo não perfila a si mesmo.

Assim, valores mínimos, máximos e categorias não são preenchidos antecipadamente ou inventados antes da carga. Eles passam a fazer parte do resultado reproduzível do pipeline.

## Fonte: pesquisa de preços

| Campo original | Descrição | Tratamento |
|---|---|---|
| `Regiao - Sigla`, `Estado - Sigla` | localização administrativa | filtro para `RJ`; região preservada |
| `Municipio` | município da coleta | normalização e conciliação com código IBGE |
| `Revenda`, `CNPJ da Revenda` | identificação do posto | CNPJ sem formatação e flag de 14 dígitos |
| endereço, bairro e CEP | localização descritiva | preservados na camada detalhada |
| `Produto` | combustível pesquisado | mapeado por tabela de referência |
| `Data da Coleta` | data da observação | conversão de `dd/MM/yyyy` para data |
| `Valor de Venda` | preço de revenda | conversão de vírgula decimal; domínio positivo |
| `Valor de Compra` | preço de compra | preservado no Bronze, fora da análise principal |
| `Unidade de Medida`, `Bandeira` | contexto comercial | preservados e normalizados |

## Fonte: vendas municipais

| Campo original | Descrição | Tratamento |
|---|---|---|
| `ANO` | ano de referência | filtro 2022–2024 |
| `GRANDE REGIÃO`, `UF` | localização administrativa | filtro `RJ`; região preservada |
| `PRODUTO` | combustível vendido | mapeado por tabela de referência |
| `CÓDIGO IBGE`, `MUNICÍPIO` | identificação municipal | código IBGE torna-se chave canônica |
| `VENDAS` | volume anual | decimal em litros; domínio não negativo |

## Fonte: cadastro atual de revendedores

| Campo original | Uso |
|---|---|
| `CODIGOISIMP`, `AUTORIZACAO`, `DATAPUBLICACAO` | rastreabilidade cadastral |
| `RAZAOSOCIAL`, `CNPJ` | identificação da fotografia atual |
| endereço, complemento, bairro, CEP, UF e município | localização atual |
| `BANDEIRA`, `DATAVINCULACAO` | vínculo comercial atual |

## Tabelas analíticas e domínios

Os campos de negócio têm definições, domínio esperado, unidade e linhagem em [config/catalogo_atributos.csv](../config/catalogo_atributos.csv). Para os campos técnicos de carga, o notebook gera uma definição e linhagem pela própria tabela, sem deixar colunas fora do perfil observado.

| Tabela | Finalidade | Chave/granularidade |
|---|---|---|
| `silver_preco_coletado` | preço tipado e conciliado, com flags | observação de preço |
| `silver_venda_municipio` | venda anual tipada e mapeada | código IBGE × produto × ano |
| `gold_dim_tempo` | calendário das coletas | data |
| `gold_dim_municipio` | referência geográfica | código IBGE |
| `gold_dim_produto` | equivalência entre fontes | produto analítico |
| `gold_ref_mapeamento_produto` | regras entre produto publicado e produto analítico | fonte × produto de origem |
| `gold_dim_revenda` | revendas observadas no histórico | CNPJ |
| `gold_ref_conciliacao_municipio` | auditoria de nomes de município e código IBGE | município de origem × UF |
| `gold_fato_preco_coletado` | auditoria da observação | CNPJ × produto × data |
| `gold_fato_preco_municipio_semana` | estatísticas semanais | município × produto × semana |
| `gold_fato_preco_municipio_anual` | estatísticas anuais com cobertura | município × produto × ano |
| `gold_fato_venda_municipio_anual` | venda anual municipal | município × produto × ano |
| `gold_fato_mercado_municipio_anual` | mart de preço e volume | município × produto × ano |
| `gold_fato_cadastro_revenda_snapshot` | cadastro atual isolado | CNPJ × data de extração |

## Convenções de domínio

- Datas de preço devem estar entre 2022-01-01 e 2024-12-31.
- `UF` deve ser `RJ` após o filtro do projeto.
- Preço válido é decimal maior que zero. Volume válido é decimal maior ou igual a zero.
- `codigo_ibge` deve ter sete dígitos quando presente.
- CNPJ válido em formato tem 14 dígitos; a validação de dígitos verificadores pode ser adicionada como regra complementar, sem descartar registros brutos.
- `produto_analitico` e `compatibilidade_analitica` são controlados por `produto_mapeamento.csv`.
- Métricas de preço são publicadas apenas quando a cobertura mínima está atendida; dados de baixa cobertura continuam disponíveis para auditoria.
- A flag de outlier por IQR só é avaliada em grupos com pelo menos quatro observações; em grupos menores ela permanece falsa e `outlier_iqr_avaliavel` informa a limitação.
