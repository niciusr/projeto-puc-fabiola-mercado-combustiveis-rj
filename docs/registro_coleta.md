# Registro público da coleta

Os dados brutos permanecem fora do GitHub. Este registro resume a execução que originou o lote nacional `anp_br_20260909T081743Z_9b2d128e` e permite conferir origem, data de acesso e integridade sem republicar os arquivos da ANP.

## Arquivos selecionados

| Fonte | Arquivo selecionado | Acesso UTC | SHA-256 |
|---|---|---|---|
| Vendas municipais — gasolina C | `vendas-anuais-de-gasolina-c-por-municipio.csv` | 2026-09-05 18:53:04 | `1469a81527e813dd4e446251a0095fdfdef8b03c1d7300b84e91814c2c6e4b72` |
| Vendas municipais — etanol hidratado | `vendas-anuais-de-etanol-hidratado-por-municipio.csv` | 2026-09-05 18:53:07 | `bc45221d1dfd9d35cf0309ee41fe32083c12dddddce382caabcbb7851600c8d5` |
| Preços 2022, 1º semestre | `precos-semestrais-ca.zip` | 2026-09-05 18:54:01 | `fdc40e6b02230066c9a9e164a049ec2c8ddabb442bff8a2b52ff500ed797e8d6` |
| Preços 2022, 2º semestre | `ca-2022-02.zip` | 2026-09-05 18:54:03 | `fd5ce68fa887ca88f38cb07d818bf28c5eeeee85a5bac1ff7e22eb1f53a4de59` |
| Preços 2023, 1º semestre | `ca-2023-01.zip` | 2026-09-05 18:54:06 | `9c19b4fa8c1beea8488f9382fa67796068716d09df7edbc3e6664f8781521599` |
| Preços 2023, 2º semestre | `ca-2023-02.zip` | 2026-09-05 18:54:08 | `80c0b361e5a50161830806b85bd2f2a4f806bde3a86f59351bc215319c60a12d` |
| Preços 2024, 1º semestre | `ca-2024-01.zip` | 2026-09-05 18:54:08 | `5f8468096dad5582633ab172bbb101529be8235292487f0d412f5930b310b12a` |
| Preços 2024, 2º semestre | `ca-2024-02.zip` | 2026-09-05 18:54:11 | `63952c6786fe96289a643fdfbf4cf15e29264fb6a4c0be916a0708687aed71d4` |
| Cadastro de revendedores | `dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv` | 2026-09-05 18:54:14 | `afb1b1b422c72a3939f0ff8eb45b776f6fcc8c16247832998233b45f28660558` |
| Logística 02 | `movimentacaologistica.zip` | 2026-09-07 20:27:23 | `c377ed483387c076649bacb49998812ac5a0ca0aa776a555f8e4b2afba628810` |

## Endpoints oficiais

| Fonte | Endpoint usado | Uso no projeto |
|---|---|---|
| Logística 02 | [movimentacaologistica.zip](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/mdpg/movimentacaologistica.zip) | volume declarado por vendedor, UF, produto e mês |
| Preços | [série histórica de preços](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis) | preços semanais de revenda, postos, municípios e bandeiras |
| Vendas municipais | [vendas de derivados e biocombustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis) | escala municipal anual e reconciliação |
| Cadastro | [dados cadastrais de revendedores](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos) | fotografia atual da rede de revenda |

## Uso e licença

- As páginas consultadas fazem parte da [Central de Dados Abertos da ANP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-abertos). A página da série de preços identifica o levantamento como dado aberto e remete ao Decreto nº 8.777/2016.
- A página da [Série Histórica de Preços](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis) declara que o conteúdo do site é publicado sob Creative Commons Atribuição–SemDerivações 3.0 Não Adaptada. Os [termos de uso do portal da ANP](https://www.gov.br/anp/pt-br/acesso-a-informacao/termos-de-uso-privacidade-e-seguranca-1/termos-uso-portal-anp) também foram consultados.
- Esta entrega não republica CSVs ou ZIPs, nem bases derivadas que contenham dados pessoais. Ela conserva código, metadados de integridade, links para a origem e capturas de tabelas agregadas.
- Em nova coleta, a pessoa que executar o projeto deve revisar os termos vigentes na página de cada fonte e atualizar o manifesto. URLs e arquivos públicos podem ser alterados pela ANP.

## Como conferir a execução

1. Execute `scripts/download_anp.py` e `scripts/inspect_raw.py`.
2. Compare os hashes produzidos com os valores acima quando o arquivo ainda estiver disponível na mesma versão.
3. Carregue o conjunto no Volume e confira o `lote_id` em `bronze_lote_carga`.
4. Acompanhe a passagem Bronze → Silver → Gold pelas contagens de `gold_reconciliacao_etapas`.
