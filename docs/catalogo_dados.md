# Catálogo de dados

O catálogo final é produzido pelo notebook `03_qualidade_dados.py` na tabela `gold_catalogo_atributos`. Para cada atributo de todas as tabelas Silver e Gold, ele registra tipo Spark, quantidade de linhas, nulos, distintos, mínimo, máximo e, para campos categóricos, as categorias mais frequentes observadas no lote executado.

As descrições de negócio, domínios esperados, unidades e linhagem ficam em `config/catalogo_atributos.csv`. Nesta revisão, o arquivo versionado recebeu entradas completas de `gold_dim_lote_carga`, `gold_fato_cadastro_revenda_snapshot`, `gold_ref_conciliacao_municipio` e `gold_reconciliacao_etapas`, que sustentam a rastreabilidade da entrega. Elas passam a compor `gold_catalogo_atributos` na próxima execução do notebook 03; o lote registrado em 9 de setembro mantém o perfil observado naquela execução. Quando uma coluna técnica não tem entrada manual, o notebook completa uma descrição e domínio compatíveis com seu tipo. Mínimos, máximos e categorias vêm sempre da execução real, não de valores preenchidos no dicionário.

## Dicionário das fontes

| Fonte | Campos principais | Tratamento na Silver |
|---|---|---|
| Logística 02 | `Período`, `UF Destino`, `Produto`, `Vendedor`, `Qtd Produto Líquido` | período vira `data_referencia`; quantidade vira decimal; vendedor recebe chave normalizada; UF, volume, produto e duplicidade recebem flags |
| Preços semanais | UF, município, revenda, CNPJ, produto, data, valor de venda, unidade e bandeira | datas e decimais tipados; município conciliado ao IBGE; produto mapeado; chave natural marcada |
| Vendas municipais | ano, grande região, UF, produto, código IBGE, município e vendas | código IBGE vira chave geográfica; volume é decimal; produto é mapeado |
| Cadastro de revendedores | CNPJ, razão social, UF, município, bandeira e datas cadastrais | retrato de extração separado do histórico de preços e volumes |

## Domínios que orientam a qualidade

| Atributo | Domínio esperado | Tratamento se houver exceção |
|---|---|---|
| `data_referencia` | mês entre 2022-01-01 e 2024-12-01 | linha preservada na Silver e sinalizada em regra de qualidade |
| `uf` na logística | uma das 27 siglas de UF | valores como `N/A` ficam na Silver, mas não entram em agregados estaduais |
| `volume_liquido_litros` | número; negativos podem ser ajustes declarados | flag `ajuste_negativo`; nunca apagado para melhorar o resultado |
| `volume_litros` municipal | número maior ou igual a zero | linha sinalizada quando inválida |
| `preco_venda` | decimal maior que zero | linha fica auditável; só preço válido entra nas agregações |
| `codigo_ibge` | sete dígitos quando conciliado | ausência gera flag, não substituição por nome livre |
| `cnpj` | 14 dígitos quando válido | a formatação é removida e a validade é registrada |
| `produto_analitico` | valor presente no mapeamento ou `NAO_MAPEADO` | o valor de origem é preservado e a cobertura do mapeamento é mensurada |
| `participacao_pct` | 0 a 100 | calculada somente com saldo positivo de vendedor e total positivo de UF |
| `hhi` | 0 a 10.000 | deriva das participações por vendedor |

## Tabelas principais

| Tabela | Finalidade | Granularidade |
|---|---|---|
| `silver_venda_empresa_uf_mes` | venda logística padronizada com flags | linha recebida da Logística 02 |
| `silver_preco_coletado` | preço de revenda padronizado | posto × produto × data |
| `silver_venda_municipio` | venda anual municipal padronizada | município × produto × ano |
| `silver_cadastro_revenda` | fotografia cadastral padronizada | CNPJ × data de extração |
| `gold_fato_venda_empresa_uf_mes` | volume líquido por vendedor | vendedor × UF × produto × mês |
| `gold_fato_participacao_vendedor_uf_mes` | participação e ranking | vendedor × UF × produto × mês |
| `gold_fato_concentracao_uf_mes` | HHI, líder e Top 3 | UF × produto × mês |
| `gold_fato_preco_uf_mes` | preço e dispersão por UF | UF × produto × mês |
| `gold_mart_mercado_uf_mes` | contexto integrado de volume, concentração e preço | UF × produto × mês |
| `gold_fato_mercado_municipio_anual` | comparação municipal de preço e volume | município × produto × ano |
| `gold_reconciliacao_volume_uf_ano` | comparação das duas fontes de volume | UF × grupo de produto × ano |
| `gold_catalogo_atributos` | perfil por atributo e dicionário aplicado | tabela × coluna |
| `gold_resultado_regra_qualidade` | resultado das regras de qualidade | regra × tabela |

## Como usar o catálogo na entrega

1. Execute o notebook de qualidade com `perfil_completo = true`.
2. Registre a contagem, os nulos, o mínimo, o máximo e as categorias observadas para os campos usados na apresentação.
3. Mostre pelo menos uma captura de `gold_catalogo_atributos` e uma de `gold_resultado_regra_qualidade`.
4. Ao discutir uma ressalva, cite a regra, a tabela e a quantidade afetada. Não substitua a ressalva por uma limpeza silenciosa.
