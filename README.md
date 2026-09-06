# MVP — Mercado de combustíveis no Rio de Janeiro (2022–2024)

Este projeto constrói um pipeline em nuvem com dados abertos da Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP). O foco é relacionar, de forma descritiva, preços de revenda pesquisados semanalmente e vendas anuais de combustíveis por município no estado do Rio de Janeiro.

O cadastro atual de revendedores entra como uma camada de contexto e rastreabilidade. Ele não é usado para afirmar como era a rede de postos no passado.

## Pergunta central

Como os preços de revenda de gasolina C e etanol hidratado variaram entre os municípios do Rio de Janeiro entre 2022 e 2024 e como esses padrões se associam ao volume anual comercializado?

## Perguntas de análise

1. Quais municípios tiveram os maiores e menores preços medianos, com cobertura mínima da pesquisa?
2. Onde a dispersão de preços entre postos e semanas foi maior?
3. Como preço mediano, dispersão e volume anual se comportam conjuntamente por município, produto e ano?
4. Quais municípios são casos atípicos de preço relativo e volume?
5. Há diferença entre bandeiras quando a comparação ocorre dentro do mesmo município, semana e produto?
6. Qual parte dos municípios com vendas possui observações de preço e qual parte fica fora da amostra?

O estudo é exploratório: correlações entre preço e volume não são interpretadas como causalidade.

## Recorte e decisões metodológicas

- **UF:** Rio de Janeiro.
- **Período:** 2022, 2023 e 2024.
- **Cruzamento principal:** `GASOLINA` da pesquisa de preços com `GASOLINA C` das vendas; `ETANOL` com `ETANOL HIDRATADO`.
- **Produtos apenas na análise de preços:** gasolina aditivada, diesel, diesel S10 e GNV, quando presentes.
- **Chave geográfica:** código IBGE. A pesquisa de preços traz o município por nome; a venda municipal é a fonte de referência para a conciliação.
- **Cobertura mínima para ranking e comparação:** pelo menos 3 postos distintos e 4 semanas pesquisadas no município-produto-ano.

`Valor de Compra` não é usado. O campo não está disponível de forma utilizável no período recente, portanto não permitiria estimar margem de revenda com consistência.

## Fontes oficiais

| Fonte | Uso no projeto | Granularidade |
|---|---|---|
| [Série histórica de preços](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis) | preço de venda, produto, bandeira, CNPJ e data de coleta | posto × produto × data |
| [Vendas de derivados e biocombustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis) | vendas anuais municipais de gasolina C e etanol hidratado | município × produto × ano |
| [Dados cadastrais de revendedores](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos) | fotografia atual do cadastro de postos | revendedor × data de extração |

As URLs diretas usadas pelo coletor estão em `scripts/download_anp.py`. A ANP informa a licença CC BY-ND 3.0 para os dados cadastrais; registre a data de acesso no manifesto gerado e mantenha os arquivos brutos fora do Git.

## Estrutura

```text
mvp_anp_combustiveis_rj_2022_2024/
├── config/                 # mapeamento de produtos e dicionário operacional
├── data/                   # dados locais não versionados
├── docs/                   # objetivo, linhagem, modelo, catálogo e evidências
├── notebooks/              # notebooks Python/SQL para importar no Databricks
└── scripts/                # coleta e inspeção dos arquivos oficiais
```

## Execução local e em nuvem

1. Instale apenas Python 3.10+; os scripts usam a biblioteca padrão.
2. Execute `python3 scripts/download_anp.py`. O coletor salva os arquivos brutos e extrai os ZIPs semestrais de preços.
3. Execute `python3 scripts/inspect_raw.py`. Isso produz `data/metadata/source_manifest.json` com hashes, cabeçalhos e contagem de linhas.
4. Envie os seis ZIPs de preços, os dois CSVs de vendas e o CSV de cadastro para as subpastas indicadas de um Volume do Databricks. Envie também os dois CSVs da pasta `config/`.
5. Importe os notebooks e execute, nesta ordem: `00_preparar_arquivos_anp.py`, `01_ingestao_bronze.py`, `02_transformacao_modelo.py` e `03_qualidade_dados.py`. Depois, abra `04_analises.sql` em uma consulta SQL (nesta execução, ela foi salva como **04 - Análises Gerenciais ANP RJ**).
6. Confirme os widgets `database`, `raw_root` e `config_root` antes da primeira execução. Os valores padrão já correspondem ao workspace preparado para este projeto.
7. Guarde as capturas indicadas em `docs/evidencias.md`.

Alguns servidores públicos bloqueiam automação fora de um navegador. Se a coleta retornar 403, baixe o mesmo arquivo a partir da página oficial, salve-o na pasta indicada pelo erro e execute novamente o script de inspeção. Não altere os arquivos brutos.

## Publicação no GitHub

O repositório deve conter código, configurações, documentação e as capturas da execução. Os arquivos brutos, resultados locais, credenciais e configurações pessoais ficam fora do histórico pelo `.gitignore`.

Depois de criar um repositório vazio no GitHub, associe a URL dele ao repositório local e envie a branch principal:

```bash
git remote add origin <URL_DO_REPOSITORIO>
git push -u origin main
```

Antes de compartilhar o link, confira se `data/raw/`, `data/processed/`, `data/metadata/` e arquivos `.env` não aparecem na lista de arquivos preparados para envio.

## Entregáveis gerados pelo pipeline

- Camada Bronze com origem, lote, arquivo e data de ingestão;
- Camada Silver com dados padronizados, conciliação municipal e flags de qualidade;
- Modelo dimensional em Delta, com fatos detalhados, semanais e anuais;
- Perfil automático por atributo e resultados das regras de qualidade;
- Tabelas de cobertura, reconciliação e outliers;
- Consultas SQL que respondem às perguntas do projeto.

Consulte [docs/execucao_databricks.md](docs/execucao_databricks.md) para o roteiro de execução, [docs/discussao_resultados.md](docs/discussao_resultados.md) para a discussão já baseada no lote executado e [docs/autoavaliacao.md](docs/autoavaliacao.md) antes da entrega.

## Apresentação

Abra [apresentacao/index.html](apresentacao/index.html) em um navegador para
visualizar a apresentação final. Ela usa as capturas reais do Databricks,
permite navegar pelas setas do teclado, exibir notas de apresentação e ampliar
as evidências. Sem JavaScript, o arquivo continua disponível como documento
vertical e também possui um modo de impressão.
