# Dados locais

Esta pasta recebe os arquivos brutos baixados da ANP, o manifesto da coleta e resultados locais opcionais. Eles não são versionados.

```text
data/
├── raw/        # ZIPs, CSVs extraídos e JSON de cadastro
├── metadata/   # manifestos de download e inspeção
└── processed/  # saídas locais opcionais
```

Os notebooks leem uma cópia dessas pastas enviada para um Volume do Databricks.
