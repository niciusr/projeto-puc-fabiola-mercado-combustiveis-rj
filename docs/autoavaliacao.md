# Autoavaliação para preencher antes da entrega

Preencha após executar o pipeline e organizar as evidências. Não marque item como concluído sem indicar uma captura, tabela ou arquivo que o comprove.

| Critério | Evidência | Situação |
|---|---|---|
| Objetivo e perguntas definidos antes da análise | `docs/objetivo.md` e README | revisado |
| Coleta em fonte oficial e em nuvem | manifesto, Volume, notebook 01 e capturas em docs/capturas_databricks/ | executado |
| Modelo e catálogo | `docs/modelo_dados.md`, `gold_catalogo_atributos` | executado |
| Carga e documentação de ETL | notebooks 01 e 02, `gold_reconciliacao_etapas` | executado no lote `anp_rj_20260906T022801Z_5a797990` |
| Qualidade por atributo | notebook 03 e `gold_resultado_regra_qualidade` | executado; 10 regras aprovadas e 4 em atenção |
| Análise e discussão | consulta SQL e `docs/discussao_resultados.md` | executado |
| Organização e apresentação | README, estrutura do repositório, evidências e apresentação HTML | executado; revisar o link público do GitHub antes da submissão |

## Pontos fortes esperados

- Fontes oficiais, arquivos brutos preservados e manifesto com hash;
- integração por código IBGE, em vez de nome livre de município;
- tabela explícita de equivalência entre produtos;
- fato detalhado e agregações semanais/anuais separadas;
- cobertura tratada como resultado analítico;
- qualidade executada por atributo, regras de domínio, integridade e outlier;
- limites temporais do cadastro atual documentados.

## Antes de publicar no GitHub

- confirme que `data/raw/`, `data/processed/` e manifestos locais continuam ignorados;
- inclua as fontes oficiais no README;
- não publique capturas com credenciais, tokens ou caminhos pessoais;
- remova resultados de teste que não façam parte da execução final;
- substitua qualquer texto provisório da discussão por resultados efetivamente calculados.
