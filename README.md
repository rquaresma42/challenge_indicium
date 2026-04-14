# Northwind Traders — Analytics Challenge

## Objetivo

Desafio técnico para a posição de **Analista de Dados / Engenheiro de Analytics** na Indicium. O objetivo é demonstrar capacidade de construir uma pipeline de dados completa — da ingestão em camadas Bronze/Silver/Gold no Databricks até a entrega de um relatório executivo em Power BI e Python, partindo dos dados históricos da fictícia empresa Northwind Traders (distribuidora de alimentos).

---

## Estrutura de Pastas

```
challenge_indicium/
│
├── data/                          # CSVs fonte (14 tabelas do banco Northwind)
│   ├── orders.csv
│   ├── order_details.csv
│   ├── products.csv
│   └── ...
│
├── databricks/                    # Pipeline ETL — notebooks Databricks
│   ├── 01_bronze_ingestion.ipynb  # Ingestão bruta dos CSVs para Delta (Bronze)
│   ├── 02_silver_transformations.ipynb  # Limpeza, tipagem e fact table (Silver)
│   └── 03_gold_star_schema.ipynb  # Star schema dimensional (Gold)
│
├── dashboard/                     # Relatório Power BI
│   ├── Northwind Traders.pbip     # Arquivo de projeto Power BI
│   ├── Northwind Traders.Report/  # Definição das páginas e visuais
│   └── Northwind Traders.SemanticModel/  # Modelo semântico (TMDL + DAX)
│
├── executivo_assets/              # PNGs gerados para o PDF executivo
│
├── analise_northwind_databricks.ipynb  # Notebook de análise exploratória + geração do PDF
├── analise_northwind_executivo.pdf     # Relatório executivo final (gerado pelo notebook)
│
└── Desafio Analista de Dados_Engenheiro de Analytics.docx  # Enunciado original
```

---

## Pipeline de Dados (Databricks)

A arquitetura segue o padrão **Medallion** em três camadas:

| Camada | Notebook | O que faz |
|--------|----------|-----------|
| **Bronze** | `01_bronze_ingestion.ipynb` | Lê os CSVs do volume DBFS e salva como Delta sem transformações, adicionando `ingestion_ts` e `source_file` |
| **Silver** | `02_silver_transformations.ipynb` | Aplica tipagem, filtros de qualidade e calcula métricas derivadas (`gross_sales`, `net_sales`, `shipped_on_time_flag`, `shipping_delay_days`) |
| **Gold** | `03_gold_star_schema.ipynb` | Constrói o star schema: dimensões desnormalizadas (`dim_product` com categoria inline, `dim_territory` com região inline, `fact_sales` com shipper e employee inline) e `dim_calendar` gerada programaticamente |

---

## Dashboard Power BI

O relatório possui **5 páginas**:

1. **Executive Overview** — KPIs principais, receita mensal e receita por categoria
2. **Avg Order Value & Product Mix** — Ticket médio, scatter de produtos (Orders × AOV), top 15 produtos
3. **Customers and Churn** — Retenção, novos clientes, clientes em risco, distribuição de inatividade
4. **Commercial Performance** — Ranking de vendedores, performance por território
5. **Logistics Efficiency & Service Level** — % entregas no prazo, distribuição de atrasos, gauge de nível de serviço

O modelo semântico usa o **Gold schema do Databricks** como fonte via conector nativo, com medidas DAX calculadas na camada `calculations`.

---

## Relatório Executivo (PDF)

O notebook `analise_northwind_databricks.ipynb` conecta ao Databricks via SQL, reproduz as análises das 5 páginas do Power BI em Python (Plotly) e gera `analise_northwind_executivo.pdf` — um documento executivo com KPIs e 8 gráficos estáticos, pronto para apresentação.

Para regenerar o PDF basta executar todas as células do notebook em ordem.
