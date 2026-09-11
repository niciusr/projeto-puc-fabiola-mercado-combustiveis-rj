# Discussão dos resultados

Esta discussão usa somente o lote nacional `anp_br_20260909T081743Z_9b2d128e`, executado em 9 de setembro de 2026. O lote contém 225.398 linhas de Logística 02, 2.710.038 observações de preços e 141.622 vendas por vendedor tipadas. Foram publicadas 42 tabelas e 464 perfis de atributo.

As perguntas permanecem as do [objetivo](objetivo.md). A [matriz de respostas](matriz_respostas.md) liga cada conclusão à consulta e à captura correspondente.

## 1. Quem declarou os maiores volumes?

O ranking principal é mensal. Em dezembro de 2023, no etanol hidratado do Distrito Federal, Ipiranga declarou 5,817 milhões de litros (30,213%), Vibra Energia 4,970 milhões (25,814%) e Raízen 4,605 milhões (23,919%). A [captura 15](capturas_databricks/15_ranking_mensal.png) mostra período, UF, posições, volume e participação.

Como leitura complementar, o ranking anual consolida os meses antes de calcular a participação. No mesmo recorte em 2023, as três primeiras empresas foram Vibra Energia, com 38,610 milhões de litros (27,980%), Ipiranga, com 37,326 milhões (27,050%), e Raízen, com 36,989 milhões (26,806%). A [captura 07](capturas_databricks/07_ranking_vendedores.png) é esse resumo anual, não substituto do ranking mensal.

O indicador é participação no volume declarado por vendedor. Ele não representa automaticamente participação de marca no varejo.

## 2. Como a concentração variou?

HHI, Top 3 e líder anual foram recalculados a partir das vendas anuais por empresa. Não foi usada média de HHI mensal para responder a uma pergunta anual.

No etanol hidratado de 2022, o Amapá teve quatro vendedores positivos, HHI de 9.235 e Top 3 de 99,06%; a empresa líder foi Petrotorque JC, com 96,07%. Minas Gerais teve 83 vendedores positivos, HHI de 1.248 e Top 3 de 58,04%; a líder foi Vibra Energia, com 21,42%. Os dois registros aparecem lado a lado na [captura 08](capturas_databricks/08_concentracao_uf.png). A evolução mensal continua disponível na seção 6 de `04_analises.sql`.

## 3. Como se comportaram Vibra, Ipiranga, Raízen e ALE?

No mesmo recorte do Distrito Federal, as três primeiras empresas do grupo somaram participações próximas: Vibra (27,980%), Ipiranga (27,050%) e Raízen (26,806%). ALE aparece com 135.000 litros e 0,098%. A leitura é um recorte auditável do grupo econômico mapeado, não uma lista fechada do mercado: as demais empresas permanecem no ranking geral. A [captura 13](capturas_databricks/13_grupos_vendedores.png) registra esse ponto.

## 4. Onde preço, dispersão e cobertura foram maiores ou menores?

O preço é resumido como mediana das medianas mensais por UF. No etanol hidratado em 2024, Rondônia teve a maior mediana (R$ 4,990/litro) e Mato Grosso a menor (R$ 3,590/litro). A maior dispersão média pelo intervalo P90–P10 foi no Pará (R$ 1,043/litro) e a menor em Roraima (R$ 0,100/litro). Em intensidade de coleta, São Paulo concentrou 62.850 observações no ano e o Amapá 112. Esses extremos estão na [captura 16](capturas_databricks/16_extremos_preco_dispersao_cobertura.png); a [captura 14](capturas_databricks/14_preco_uf.png) exibe o contexto de meses, coletas e postos para as UFs de maior e menor mediana.

Esse resultado compara a amostra da ANP. Ele não transforma quantidade de coletas em tamanho do mercado nem atribui causalidade à diferença de preço.

## 5. Como preço, concentração e volume variaram conjuntamente?

Cada combinação de ano e produto teve 324 observações UF-mês com preço e volume disponíveis. As correlações abaixo são descritivas.

| Ano | Produto | log(volume) × preço | HHI × preço | Top 3 × preço |
|---:|---|---:|---:|---:|
| 2022 | Etanol hidratado | -0,3485 | 0,3474 | 0,3659 |
| 2022 | Gasolina C | -0,1015 | -0,0436 | -0,0476 |
| 2023 | Etanol hidratado | -0,7589 | 0,6166 | 0,5950 |
| 2023 | Gasolina C | -0,2463 | 0,1143 | 0,0993 |
| 2024 | Etanol hidratado | -0,6690 | 0,4964 | 0,4971 |
| 2024 | Gasolina C | -0,3593 | 0,1937 | 0,2148 |

A [captura 09](capturas_databricks/09_preco_volume.png) contém os mesmos coeficientes. Correlação não demonstra que preço, concentração ou volume tenha causado o outro.

## 6. Os totais das fontes são conciliáveis?

Foram produzidas 162 comparações anuais por UF e grupo de produto. Em 76 delas, o status foi `DIVERGENCIA_DE_ESCOPO`; a diferença é apresentada, não compensada. Em 2022, o etanol de Mato Grosso ficou `COMPATIVEL`, com 12 meses observados e diferença de 0,133%. Minas Gerais também teve 12 meses, mas diferença de -1,864% e status `DIVERGENCIA_DE_ESCOPO`. A [captura 10](capturas_databricks/10_reconciliacao_volume.png) traz os dois volumes, o percentual e o status.

## 7. O que o cadastro atual acrescenta?

O cadastro é uma fotografia de 9 de setembro de 2026. No Amazonas, as primeiras bandeiras foram Bandeira Branca (302 revendas; 35,20%), ATEM'S (245; 28,55%), Equador (126; 14,69%), Vibra (62; 7,23%) e Ipiranga (50; 5,83%). A [captura 12](capturas_databricks/12_rede_bandeiras.png) mostra a data de extração e o recorte de UF.

Esses números descrevem o cadastro disponível na data da carga. Não provam que a mesma bandeira estava no mesmo posto em 2022, 2023 ou 2024, nem ligam bandeira ao vendedor da Logística 02.

## Qualidade e limites que acompanham a análise

- Das 26 regras, 19 foram aprovadas e 7 foram informativas; não houve regra em atenção.
- 941 linhas sem UF válida e 337 ajustes negativos foram preservados na Silver para auditoria.
- Foram identificadas 28 chaves repetidas de preço (0,0010%) e 43.790 outliers por IQR (1,6159%). A agregação faz desempate determinístico da chave repetida, e os outliers continuam disponíveis na tabela de auditoria.
- A cobertura municipal de preços é parcial: 48 combinações ficaram abaixo do critério de publicação. Ausência de preço na pesquisa não significa ausência de mercado.
- Preço, volume, vendedor e bandeira têm granularidades e agentes distintos. Por isso, os cruzamentos são agregados e acompanhados de sua limitação.
