# Discussão dos resultados da execução

Os resultados abaixo foram obtidos na execução do lote
`anp_rj_20260906T022801Z_5a797990`, com preços semanais da ANP e vendas
municipais de gasolina C e etanol hidratado no Rio de Janeiro, entre 2022 e
2024.

## Cobertura e integração

Foram carregadas 2.710.038 observações de preço. Depois do filtro de UF e
período, permaneceram 212.637 registros de preço válidos no RJ; 212.631 foram
usados nas agregações após o desempate da chave natural. A base de vendas gerou
552 combinações município-produto-ano e o cruzamento com as duas fontes ficou
disponível em 194 combinações.

O universo de comparação tem 92 municípios para cada produto e ano. A pesquisa
de preços estava disponível em 33 municípios em 2022 (35,87%) e em 32 em 2023
e 2024 (34,78%), tanto para gasolina C quanto para etanol hidratado. Em 2022,
32 municípios por produto atenderam ao critério de publicação; em 2023 e 2024,
foram 32. A ausência de preço deve ser lida como ausência na amostra de
pesquisa, e não como ausência de mercado no município.

## Nível e dispersão de preços

Os rankings usam a mediana anual e só incluem observações com a cobertura
mínima definida no projeto. Um exemplo é o etanol hidratado em 2022: Angra dos
Reis apresentou mediana anual de R$ 6,79, aproximadamente 21,5% acima da
referência estadual, com 47 semanas pesquisadas e 15 postos distintos. Esse
tipo de leitura deve sempre acompanhar a quantidade de semanas e de postos,
pois a comparação não equivale a um censo de todos os revendedores.

## Preço e volume

Nas seis combinações produto-ano publicáveis, a associação entre o log do
volume vendido e a mediana anual de preço foi negativa e fraca a moderada. Por
exemplo, a correlação foi -0,3847 para etanol hidratado em 2023 e -0,2741 para
gasolina C em 2023, ambas calculadas sobre 32 municípios. O resultado é
descritivo: ele não permite concluir que o volume causou o preço ou o contrário.

## Qualidade e limites

Das 14 regras executadas, 10 foram aprovadas e 4 ficaram em atenção. Não houve
preço inválido, CNPJ fora do formato, município sem conciliação ao código IBGE
ou produto sem mapeamento. As atenções representam características ou sinais
para investigação, e não falha da carga:

- 358 combinações município-produto-ano tinham venda, mas não tinham observação
  de preço (64,8551% do universo de mercado);
- 2 combinações ficaram abaixo do critério de cobertura de publicação;
- 12 observações repetiram a chave CNPJ + produto + data de coleta (0,0056%);
- 3.093 observações ficaram fora dos limites de 1,5 IQR (1,4546%).

As duplicidades foram resolvidas por uma regra determinística para as
agregações, enquanto os outliers permanecem identificados em
`gold_preco_outlier`; não foram apagados do dado detalhado. O cadastro de
revendedores é tratado como fotografia atual, portanto não sustenta inferência
sobre o status histórico de um posto.

As consultas que permitem reproduzir os números e aprofundar os rankings,
quadrantes e comparação por bandeira estão em `04_analises.sql` e na consulta
salva **04 - Análises Gerenciais ANP RJ** no Databricks.
