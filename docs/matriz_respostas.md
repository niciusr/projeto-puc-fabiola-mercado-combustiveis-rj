# Matriz de perguntas, evidências e limites

Esta matriz preserva as perguntas do objetivo e mostra exatamente onde cada uma é respondida. A unidade de volume é litro; preço é expresso em R$/litro quando a fonte informa essa unidade.

| Pergunta | Tabela e consulta | Evidência | Leitura permitida |
|---|---|---|---|
| 1. Quem declarou os maiores volumes? | `gold_fato_venda_empresa_uf_mes`, consulta 2; consolidação em 2A | `15_ranking_mensal.png` e `07_ranking_vendedores.png` | ranking por UF, mês, produto e empresa; a tela anual é apenas uma leitura consolidada |
| 2. Como evoluiu a concentração? | `gold_fato_concentracao_uf_mes`, consultas 4 e 6 | `08_concentracao_uf.png` | HHI, Top 3 e participação do líder anual; a consulta 6 preserva a série mensal |
| 3. Como se comportaram Vibra, Ipiranga, Raízen e ALE? | `gold_fato_venda_empresa_uf_mes`, consulta 3 | `13_grupos_vendedores.png` | volume e participação anual dos grupos mapeados, sem apagar empresas não mapeadas |
| 4. Onde preço, dispersão e cobertura foram maiores ou menores? | `gold_mart_mercado_uf_mes`, consultas 5 e 5A | `14_preco_uf.png` e `16_extremos_preco_dispersao_cobertura.png` | comparação por UF e extremos anuais de mediana, spread e coletas |
| 5. Como volume, concentração e preço variaram conjuntamente? | `gold_mart_mercado_uf_mes`, consulta 7 | `09_preco_volume.png` | correlação descritiva por ano e produto; não mede causalidade |
| 6. Os volumes das fontes são conciliáveis? | `gold_reconciliacao_volume_uf_ano`, consulta 8 | `10_reconciliacao_volume.png` | diferença percentual, meses observados e status de compatibilidade ou escopo |
| 7. O que o cadastro acrescenta? | `gold_fato_rede_bandeira_uf_snapshot`, consulta 11 | `12_rede_bandeiras.png` | fotografia da rede na data de extração, sem inferir situação histórica |

## Regras de interpretação

- `Vendedor` da Logística 02 não é sinônimo de bandeira ou de posto de revenda.
- A concentração anual é calculada a partir dos volumes anuais por empresa, e não pela média de indicadores mensais.
- A pesquisa de preços é amostral. Ausência de preço na amostra não representa ausência de mercado.
- Vendas municipais são anuais; por isso, a reconciliação é anual e não é usada para inventar volume mensal.
- Correlação apresenta associação nos dados carregados. Ela não demonstra causa e efeito.
