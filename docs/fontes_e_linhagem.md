# Fontes e linhagem dos dados

## Fontes utilizadas

| Origem | Arquivo ou endpoint | Campos que sustentam a análise | Granularidade de origem | Papel no modelo |
|---|---|---|---|---|
| ANP — Logística 02 | [movimentacaologistica.zip](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/mdpg/movimentacaologistica.zip) | `Período`, `UF Destino`, `Produto`, `Vendedor`, `Qtd Produto Líquido` | vendedor × UF destino × produto × mês | volume, participação e concentração |
| ANP — série histórica de preços | [página da série](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis) | produto, data, UF, município, CNPJ, revenda, bandeira, preço de venda | posto × produto × data de coleta | preço e cobertura de pesquisa |
| ANP — vendas de derivados e biocombustíveis | [página de dados abertos](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis) | ano, UF, código IBGE, município, produto, vendas | município × produto × ano | escala municipal e reconciliação anual |
| ANP — cadastro de revendedores | [página de dados cadastrais](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos) | CNPJ, razão social, UF, município, bandeira, datas cadastrais | revendedor × fotografia de extração | contexto da rede varejista |
| Projeto | `config/produto_mapeamento.csv` | fonte, produto de origem, produto analítico, grupo de reconciliação | produto publicado | padronização explícita |
| Projeto | `config/empresa_grupo_mapeamento.csv` | vendedor normalizado, empresa canônica, grupo econômico | vendedor publicado | leitura dos grupos selecionados |

O metadado oficial da base logística está disponível em [Metadado unificado — Logística](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/mdpg/metadado-unificado-logistica.pdf). O script de coleta registra a URL efetivamente utilizada, horário de acesso, arquivo local, tamanho e hash SHA-256. A inspeção posterior registra codificação, delimitador, colunas, período observado e número de linhas em `data/metadata/`. O resumo público da coleta, os hashes e os termos de uso consultados estão em [registro_coleta.md](registro_coleta.md).

## Papel e cautelas de cada fonte

### Logística 02 — núcleo do projeto

Essa fonte fornece o volume líquido informado por vendedor para uma UF de destino, produto e período. Ela sustenta o indicador de participação e as medidas de concentração. A carga preserva o nome publicado do vendedor e cria uma chave normalizada apenas para mapeamentos controlados de grupos econômicos.

O volume é tratado como dado declaratório da fonte. Registros com quantidade negativa, caso existentes, são marcados como ajuste e permanecem auditáveis; o projeto não os elimina para melhorar artificialmente os totais. UFs não informadas ou fora do domínio esperado também permanecem na camada Silver e são excluídas somente dos agregados estaduais.

### Preços de revenda — contexto agregado

A série de preços representa visitas de pesquisa a postos. Ela permite observar nível e dispersão de preços, bem como cobertura de município, semana e UF. Como não traz o mesmo agente econômico da Logística 02, o cruzamento é feito apenas depois da agregação por `UF + mês + produto_analitico`.

`Bandeira` e CNPJ de revenda permanecem disponíveis para análises varejistas próprias. Não são utilizados para deduzir que uma bandeira corresponde ao vendedor da base logística.

### Vendas municipais — comparação anual de escopo

As vendas municipais têm granularidade anual e código IBGE. Servem para avaliar escala municipal e para confrontar, por UF e ano, totais obtidos por duas fontes distintas. A reconciliação é documentada por produto e cobertura de meses; diferença não é corrigida por uma regra implícita.

Para gasolina C, o grupo de reconciliação pode incorporar as variantes de gasolina presentes na fonte logística quando a definição publicada exigir isso. Para etanol, qualquer diferença observada permanece destacada como limite de comparabilidade até que a documentação das fontes justifique outra decisão.

### Cadastro de revendedores — fotografia atual

O cadastro é usado para descrever a rede no momento da extração e apoiar auditorias de CNPJ, UF e bandeira. Não é usado como evidência de que um posto operava, tinha determinada bandeira ou atendia a um vendedor em 2022, 2023 ou 2024.

## Fluxo de linhagem

```text
ANP: Logística 02 ───────────┐
ANP: preços semanais ─────────┼──> Volume / raw ──> Bronze (arquivo, hash, lote)
ANP: vendas municipais ───────┤                              |
ANP: cadastro de revendedores ┘                              v
configurações versionadas ─────────────────────────> Silver (tipos, chaves e flags)
                                                               |
                                     ┌─────────────────────────┴────────────────────────┐
                                     v                                                  v
                          dimensões conformadas                              fatos por assunto
                                     \                                                  /
                                      \                                                /
                                       └──> Gold: mercado UF/mês, reconciliação, qualidade
```

## Transformações rastreáveis

| Transformação | Regra | Motivo | Evidência |
|---|---|---|---|
| Identificação do arquivo | URL, hash, data de acesso e lote | reproduzir a origem exata | manifesto e `bronze_lote_carga` |
| Cabeçalhos e codificação | normalização técnica sem alterar valor de negócio | permitir leitura consistente | Bronze e manifesto de inspeção |
| Período logístico | conversão para mês de referência | conformar a dimensão de tempo | `silver_venda_empresa_uf_mes` |
| Quantidade líquida | conversão decimal e flag de ajuste negativo | preservar medidas e exceções | `volume_liquido_litros`, `ajuste_negativo` |
| UF e produto | normalização controlada por tabelas de referência | assegurar chaves comparáveis | `gold_dim_uf`, `gold_dim_produto` |
| Vendedor | nome de origem preservado; chave normalizada para mapeamento opcional | evitar agrupamentos escondidos | `gold_dim_empresa_vendedora` |
| Preço | conversão de data e decimal; agregação de observações | formar medidas por UF/mês/produto | fatos de preço |
| Município | código IBGE nas vendas e nome normalizado na pesquisa | manter a chave geográfica mais confiável | `gold_dim_municipio` |
| Participação | saldo positivo do vendedor dividido pelo total positivo da mesma UF/mês/produto | cálculo reproduzível de concentração sem ocultar ajustes negativos | fatos de participação e concentração |
| Reconciliação | comparação anual por UF e grupo de produto | explicitar compatibilidade e diferença | `gold_reconciliacao_volume_uf_ano` |

## Chaves de integração

| Integração | Chave | Regra |
|---|---|---|
| Volume e participação | `data_referencia + uf + produto_analitico + vendedor` | permanece no nível mensal da fonte logística |
| Volume e preço | `data_referencia + uf + produto_analitico` | cada lado é agregado antes da junção |
| Logística e vendas municipais | `ano + uf + grupo_reconciliacao_municipal` | comparativo anual com cobertura de meses |
| Preço e vendas municipais | `ano + codigo_ibge + produto_analitico` | utilizado no mart municipal, separado do mart mensal |
| Cadastro e preço | CNPJ somente para auditoria de fotografia | não infere histórico cadastral |

## Limites de linhagem que o projeto preserva

- A origem do volume é sempre identificável por arquivo e lote, inclusive após reprocessamento.
- O mapeamento de Vibra, Ipiranga, Raízen e ALE é explícito e não substitui o nome original do vendedor.
- Registros não mapeados não são descartados: aparecem como não classificados quando o agrupamento econômico não é conhecido.
- A dimensão de bandeira não alimenta participação de vendedor e não constitui uma ponte entre venda logística e posto pesquisado.
- Toda tabela Gold aponta para o lote usado na sua construção. A análise final deve informar esse lote e a data de acesso às fontes.
