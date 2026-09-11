# Modelo de dados

O projeto usa uma constelação de fatos em Delta Lake. Cada fonte permanece na sua própria granularidade; os marts só unem medidas depois de agregadas por uma chave compatível. Isso evita, por exemplo, atribuir o preço de um posto a um vendedor da fonte logística.

```text
                           gold_dim_tempo
                                |
gold_dim_uf ---- gold_fato_venda_empresa_uf_mes ---- gold_fato_participacao_vendedor_uf_mes
     |                  |                                      |
     |                  +---- gold_dim_empresa_vendedora ------+---- gold_fato_concentracao_uf_mes
     |                                                                         |
     +---- gold_fato_preco_uf_mes ------------------------------------ gold_mart_mercado_uf_mes

gold_dim_municipio -- gold_fato_preco_municipio_semana -- gold_fato_preco_municipio_anual
        |                                                              |
        +------------------- gold_fato_venda_municipio_anual ---------+-- gold_fato_mercado_municipio_anual

gold_dim_revenda -- gold_fato_cadastro_revenda_snapshot -- gold_fato_rede_bandeira_uf_snapshot -- gold_dim_bandeira
```

## Camadas

| Camada | Conteúdo | Regra de persistência |
|---|---|---|
| Bronze | cópia dos CSVs lidos, arquivo de origem, data de ingestão e `lote_id` | append por lote |
| Silver | tipos, chaves normalizadas, produtos mapeados e flags de qualidade | recálculo do lote selecionado |
| Gold | dimensões, fatos, marts, reconciliação, qualidade e catálogo | recálculo do lote selecionado |

O `lote_id` liga a camada Bronze às tabelas analíticas. A carga Bronze não é sobrescrita em reexecuções; o notebook de transformação usa o último lote, salvo quando o widget `lote_id` é informado.

## Dimensões e referências

| Tabela | Chave | Conteúdo |
|---|---|---|
| `gold_dim_tempo` | `data` | ano, mês, semana, trimestre e semestre entre 2022 e 2024 |
| `gold_dim_uf` | `uf` | sigla da UF e grande região quando disponível |
| `gold_dim_municipio` | `codigo_ibge + uf` | município, nome normalizado e grande região |
| `gold_dim_produto` | `produto_analitico` | família, grupo de reconciliação e uso no mart principal |
| `gold_dim_empresa_vendedora` | `vendedor_chave` | razão social original, nome canônico e grupo econômico quando há regra explícita |
| `gold_dim_revenda` | `cnpj` | fotografia atual de razão social, UF, município e bandeira |
| `gold_dim_bandeira` | `bandeira` | bandeira observada e indicador de bandeira branca |
| `gold_dim_lote_carga` | `lote_id + tabela` | contagens, fonte, data e caminho da carga |
| `gold_ref_mapeamento_produto` | `fonte + produto_origem_normalizado` | equivalência de produto versionada |
| `gold_ref_empresa_grupo` | `vendedor_chave` | agrupamentos usados para leitura gerencial |
| `gold_ref_conciliacao_municipio` | município de origem + UF | resultado da busca de código IBGE para preços |

## Fatos e granularidades

| Tabela | Granularidade | Uso |
|---|---|---|
| `gold_fato_preco_coletado` | posto × produto × data de coleta | auditoria de preço, CNPJ, bandeira e conciliação municipal |
| `gold_fato_preco_municipio_semana` | município × produto × semana | mediana, média, percentis, dispersão e coletas |
| `gold_fato_preco_municipio_anual` | município × produto × ano | medida anual e cobertura mínima |
| `gold_fato_preco_uf_mes` | UF × produto × mês | medida de preço usada no mart estadual |
| `gold_fato_venda_municipio_anual` | município × produto × ano | volume municipal publicado |
| `gold_fato_venda_empresa_uf_mes` | vendedor × UF × produto × mês | saldo líquido declarado e ajustes negativos preservados |
| `gold_fato_participacao_vendedor_uf_mes` | vendedor × UF × produto × mês | participação no volume positivo e posição no ranking |
| `gold_fato_concentracao_uf_mes` | UF × produto × mês | HHI, líder e participação das três maiores |
| `gold_mart_mercado_uf_mes` | UF × produto × mês | volume, concentração, preço e dispersão já compatibilizados |
| `gold_fato_mercado_municipio_anual` | município × produto × ano | preço e volume municipal para análise complementar |
| `gold_reconciliacao_volume_uf_ano` | UF × grupo de produto × ano | comparação anual das fontes de volume |
| `gold_fato_cadastro_revenda_snapshot` | CNPJ × data de extração | fotografia cadastral atual |
| `gold_fato_rede_bandeira_uf_snapshot` | UF × bandeira × data de extração | tamanho relativo da rede atual |

## Chaves e regras de integração

| Relação | Chave | Condição de uso |
|---|---|---|
| preço e município | `municipio_norm + uf` | a fonte municipal fornece o código IBGE de referência |
| preço e volume municipal | `codigo_ibge + ano + produto_analitico` | mart anual, com cobertura explícita |
| volume e participação | `data_referencia + uf + produto_analitico + vendedor_chave` | fato mensal da Logística 02 |
| volume e preço por UF | `data_referencia + uf + produto_analitico` | ambos agregados antes da junção |
| logística e vendas municipais | `ano + uf + grupo_reconciliacao_municipal` | reconciliação anual, nunca cálculo de participação mensal |
| cadastro e preços | `cnpj` | conferência de fotografia atual, sem inferência histórica |

`Vendedor` e `Bandeira` não compõem uma chave de integração. O primeiro é o declarante da venda logística; a segunda caracteriza uma revenda da pesquisa ou do cadastro. A dimensão de bandeira é contextual e não altera o ranking de vendedores.

## Cálculo de participação e concentração

Para cada UF, mês e produto, o fato de venda soma o volume líquido por vendedor. Linhas negativas são mantidas como ajustes na tabela de origem. O ranking utiliza apenas saldos positivos e calcula:

```text
participacao_pct = volume líquido positivo do vendedor / total positivo da UF
HHI = Σ(participacao_pct / 100)² × 10.000
Top 3 = soma das três maiores participações
```

O total líquido continua disponível no mart. Portanto, a decisão de não colocar ajustes negativos no denominador do ranking é observável e não fica escondida no código.

## Agregação de preço

1. o preço é preservado por posto, produto e data;
2. observações repetidas da chave natural recebem flag e uma seleção determinística para agregação;
3. estatísticas semanais são calculadas por município;
4. a mediana das medianas semanais forma a medida anual municipal;
5. para o mart estadual, observações válidas são agregadas por UF, mês e produto com mediana, percentis, postos, municípios e coletas.

Assim, a pesquisa amostral de preços é acompanhada por sua cobertura, em vez de ser tratada como censo da rede de postos.
