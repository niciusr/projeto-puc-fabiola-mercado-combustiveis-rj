# Mercado de combustíveis no Brasil: volume comercializado, concentração e preços de revenda por UF (2022–2024)

Projeto de Engenharia de Dados desenvolvido para a disciplina de MVP, por Fabiola Dias Carvalho. A entrega organiza dados públicos da Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP) em um lakehouse no Databricks e produz tabelas analíticas reproduzíveis.

O trabalho parte de uma pergunta de negócio que orienta o recorte: como evoluíram os volumes declarados por vendedor nas Unidades da Federação, qual foi a concentração observada nesses mercados e como esse contexto se relaciona, de forma descritiva, com os preços de revenda pesquisados pela ANP e com a escala das vendas municipais?

## Pergunta central e questões de análise

**Pergunta central.** Como evoluiu o volume de gasolina C e etanol hidratado declarado por vendedor nas UFs brasileiras entre 2022 e 2024, qual é a estrutura de concentração observada e como ela se relaciona descritivamente com preços de revenda e escala municipal de vendas?

1. Quais vendedores concentram os maiores volumes declarados em cada UF, mês e produto?
2. Como evoluem a participação do vendedor líder, a participação dos três maiores e o HHI por UF, mês e produto?
3. Como se comportam Vibra, Ipiranga, Raízen e ALE nos estados onde aparecem?
4. Quais UFs apresentam preços medianos mais altos, maior dispersão e maior ou menor cobertura de coleta?
5. Como concentração, preço de revenda e escala de volume variam conjuntamente na mesma UF, mês e produto?
6. Em que medida os totais anuais da Logística 02 e das vendas municipais são conciliáveis por UF e produto?
7. O que o cadastro atual de revendedores e bandeiras acrescenta como contexto, sem reconstituir a rede histórica?

As perguntas, o recorte e as limitações estão registrados em [docs/objetivo.md](docs/objetivo.md). A matriz que liga cada pergunta à consulta e à evidência está em [docs/matriz_respostas.md](docs/matriz_respostas.md).

## Recorte e escolha das fontes

- **Período:** janeiro de 2022 a dezembro de 2024, com checagem da cobertura observada no arquivo recebido.
- **Unidade principal de análise:** UF × mês × produto × vendedor declarado.
- **Produtos principais:** gasolina C comum e etanol hidratado comum. Produtos adicionais de gasolina entram somente na reconciliação de volume, quando necessário.
- **Fonte principal de volume:** base **Logística 02 — Vendas no Mercado Brasileiro de Combustíveis**, da ANP, com os campos `Período`, `UF Destino`, `Produto`, `Vendedor` e `Qtd Produto Líquido`.
- **Fontes complementares:** série semanal de preços de revenda; vendas municipais anuais; e cadastro atual de revendedores.

O termo usado no projeto é **participação no volume declarado por vendedor**, e não participação de mercado de uma marca de posto. O campo `Vendedor` identifica quem declarou o volume na fonte logística e pode incluir distribuidoras, produtores ou importadores. Já `Bandeira` é uma informação de revenda varejista, disponível na pesquisa de preços e no cadastro. As duas dimensões não são tratadas como equivalentes.

Por essa razão, o cruzamento entre volume e preço é agregado em `UF + mês + produto`: participação e concentração são calculadas no volume declarado por vendedor; preço é resumido a partir das observações de revenda da mesma UF, mês e produto. Não há uma tentativa de ligar uma empresa vendedora a um posto, a uma bandeira ou a um preço individual sem uma chave pública compatível.

## Fontes oficiais

| Fonte | Papel no projeto | Granularidade de origem |
|---|---|---|
| [Painel da logística do abastecimento nacional](https://www.gov.br/anp/pt-br/centrais-de-conteudo/paineis-dinamicos-da-anp/paineis-dinamicos-do-abastecimento/painel-dinamico-da-logistica-do-abastecimento-nacional-de-combustiveis) e [arquivo Logística 02](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/mdpg/movimentacaologistica.zip) | volume líquido declarado, vendedor, produto, UF destino e período | vendedor × UF × produto × mês |
| [Série histórica de preços](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis) | preço de venda pesquisado, produto, data, município e bandeira | posto × produto × data de coleta |
| [Vendas de derivados e biocombustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis) | volume anual municipal para escala e reconciliação | município × produto × ano |
| [Cadastro de revendedores](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos) | fotografia atual da rede varejista | revendedor × data de extração |

O coletor grava URL, data de acesso, tamanho e SHA-256 no manifesto local. O resumo público da coleta e os termos de uso consultados estão em [docs/registro_coleta.md](docs/registro_coleta.md). Dados brutos não são publicados no repositório.

## Arquitetura

```text
ANP (ZIPs e CSVs) -> Volume do Databricks -> Bronze -> Silver -> Gold / consultas SQL
                                      |          |         |
                                      |          |         +-- mart UF/mês/produto
                                      |          +------------ dados padronizados e reconciliados
                                      +----------------------- arquivos, hashes e lote de carga
```

A camada Gold é uma constelação de fatos com dimensões conformadas. Os principais resultados são:

- volume líquido por vendedor, UF, mês e produto;
- participação, ranking, concentração Top 3 e HHI por UF, mês e produto;
- preço mediano, dispersão e cobertura da pesquisa por UF, mês e produto;
- mart integrado de mercado por UF, mês e produto;
- reconciliação anual entre a fonte logística e as vendas municipais;
- fotografia de bandeiras e revendedores, mantida como contexto separado.

O desenho completo, as chaves e a linhagem estão em [docs/modelo_dados.md](docs/modelo_dados.md) e [docs/fontes_e_linhagem.md](docs/fontes_e_linhagem.md).

## Estrutura do repositório

```text
.
├── config/                 # mapeamentos versionados de produto e grupo econômico
├── data/                   # arquivos locais não versionados
├── docs/                   # objetivo, fontes, modelo, catálogo, evidências e autoavaliação
├── notebooks/              # preparação, Bronze, transformação, qualidade e consultas
├── scripts/                # coleta e inspeção das fontes oficiais
└── apresentacao/           # apresentação HTML da entrega
```

## Execução

1. Execute `python3 scripts/download_anp.py` para baixar os arquivos oficiais.
2. Execute `python3 scripts/inspect_raw.py` para gerar o manifesto técnico de cada arquivo.
3. Envie os arquivos brutos e os CSVs de `config/` para as pastas correspondentes de um Volume no Databricks.
4. Importe e execute, nesta ordem, `00_preparar_arquivos_anp.py`, `01_ingestao_bronze.py`, `02_transformacao_modelo.py` e `03_qualidade_dados.py`.
5. Execute `04_analises.sql` em uma consulta SQL para as análises completas.
6. Execute `05_capturas_evidencias.sql` para as saídas filtradas usadas nas evidências.

O roteiro com caminhos, widgets, verificações e critérios de captura está em [docs/execucao_databricks.md](docs/execucao_databricks.md). A discussão registra exclusivamente números do lote executado em [docs/discussao_resultados.md](docs/discussao_resultados.md).

## Execução registrada

O pipeline foi executado no schema `workspace.anp_combustiveis_br_2022_2024` com o lote `anp_br_20260909T081743Z_9b2d128e`, em 9 de setembro de 2026.

- 225.398 registros de Logística 02 e 2.710.038 registros de preços foram registrados na Bronze;
- a transformação publicou 42 tabelas e 1.944 combinações no mart estadual de UF, mês e produto;
- o catálogo observado contém 464 atributos perfilados;
- as 26 regras de qualidade resultaram em 19 aprovações, 7 ocorrências informativas e nenhuma regra em atenção;
- as consultas finais e as capturas correspondentes estão em [docs/evidencias.md](docs/evidencias.md).

Os números acima são parte da execução nacional e devem ser lidos junto das limitações documentadas: vendedor não é bandeira, o preço é amostral e a análise de preço, volume e concentração é descritiva.

## Entrega e reprodutibilidade

O repositório versiona código, configurações, documentação e evidências sem dados brutos, credenciais ou arquivos pessoais. Antes da entrega, confirme:

- manifestação da coleta e identificação do lote;
- tabelas Bronze, Silver e Gold visíveis no Databricks;
- perfil por atributo, regras de qualidade e suas evidências;
- consultas que respondem às perguntas de negócio;
- capturas da versão nacional, sem credenciais expostas;
- autoavaliação preenchida com links para as evidências reais;
- envio da versão final ao repositório remoto.

Consulte [docs/evidencias.md](docs/evidencias.md) para o checklist, [docs/registro_coleta.md](docs/registro_coleta.md) para a rastreabilidade das fontes e [docs/autoavaliacao.md](docs/autoavaliacao.md) para o fechamento da entrega.
