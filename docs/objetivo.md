# Objetivo e questões do projeto

## Objetivo geral

Construir um pipeline de dados em nuvem para integrar preços de revenda pesquisados pela ANP, vendas anuais municipais e um cadastro atual de revendedores. A análise descreve diferenças de preço e de dispersão no mercado de combustíveis do Rio de Janeiro entre 2022 e 2024.

## Unidade de análise

| Camada | Unidade de análise |
|---|---|
| Preço bruto | posto × produto × data de coleta |
| Preço agregado | município × produto × semana e município × produto × ano |
| Vendas | município × produto × ano |
| Mercado integrado | município × produto × ano |
| Cadastro atual | revendedor × data de extração |

## Perguntas

1. Quais municípios têm preços medianos mais altos e mais baixos para gasolina C e etanol hidratado?
2. Onde há maior dispersão de preço entre postos e ao longo das semanas pesquisadas?
3. Como o volume anual vendido se associa descritivamente ao preço mediano e à dispersão?
4. Que municípios combinam volume alto com preço relativo alto, ou volume baixo com preço relativo baixo?
5. Dentro de um mesmo município, semana e produto, como a bandeira se posiciona em relação à mediana local?
6. Qual é a cobertura da pesquisa de preços diante do universo de municípios com vendas registradas?

## Recorte

- Estado: Rio de Janeiro (`UF = RJ`);
- anos: 2022, 2023 e 2024;
- produtos do cruzamento: gasolina C e etanol hidratado;
- regra de publicação: três ou mais postos distintos e quatro ou mais semanas pesquisadas por município-produto-ano.

O recorte anual respeita a granularidade da venda municipal. A análise mensal é possível para a pesquisa de preços, mas não deve ser comparada diretamente com a venda municipal anual.

## Hipóteses de trabalho

- Municípios com maior volume podem ter perfil de preço e dispersão diferente dos demais, mas o dado não permite atribuir causa.
- Parte relevante da diferença observada pode estar associada à cobertura da pesquisa. Por isso, cobertura é métrica de resultado e não apenas etapa técnica.
- A comparação entre bandeiras deve controlar município, semana e produto para não transformar diferenças de localização em diferença de bandeira.

## Limitações assumidas

- A pesquisa de preços é amostral; ausência de observação não significa ausência de posto ou de venda.
- Nem todo município presente nas vendas terá coleta de preços.
- O cadastro de revendedores é uma fotografia obtida na data da extração, não uma reconstrução do cadastro histórico.
- `Valor de Compra` não será usado para cálculo de margem: o campo não tem preenchimento adequado no período.
- Gasolina aditivada, diesel, diesel S10 e GNV são mantidos na camada de preços, mas não entram no cruzamento principal por incompatibilidade conceitual ou de granularidade.
