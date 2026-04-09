# Prompt Pronto Para Databricks GenAI (Setup do Projeto)

Cole o texto abaixo no Databricks Assistant:

```text
Você é um especialista em Databricks (Free Tier) e engenharia de dados.
Quero que você me guie e gere o código completo para setup de um projeto Lakehouse com camadas Bronze, Silver e Gold, usando CSVs já enviados ao DBFS.

Contexto do projeto:
- Workspace: Databricks Free Tier
- Origem dos arquivos CSV: /Volumes/workspace/indicium_bronze/raw_data
- Delimitador dos CSVs: ;
- Header: true
- Formato de destino: Delta
- Catálogo: hive_metastore
- Schemas desejados:
  - hive_metastore.indicium_bronze
  - hive_metastore.indicium_silver
  - hive_metastore.indicium_gold

Arquivos disponíveis na origem:
- categories.csv
- customers.csv
- customer_customer_demo.csv
- customer_demographics.csv
- employees.csv
- employee_territories.csv
- orders.csv
- order_details.csv
- products.csv
- region.csv
- shippers.csv
- suppliers.csv
- territories.csv
- us_states.csv

Objetivo técnico:
1) Criar schemas bronze/silver/gold.
2) Ingerir todos os CSVs na Bronze como tabelas Delta com auditoria:
   - ingestion_ts (current_timestamp)
   - source_file (input_file_name)
3) Criar Silver com limpeza e tipagem:
   - orders_clean
   - order_details_clean
   - sales_line (join de orders_clean + order_details_clean no nível item de pedido)
4) Criar Gold em modelo estrela para Power BI:
   - fact_sales
   - dim_date
   - dim_product
   - dim_category
   - dim_customer
   - dim_employee
   - dim_shipper
   - dim_territory
   - dim_region
   - bridge_employee_territory

Regras de qualidade mínimas:
- order_id não nulo
- product_id não nulo em order_details
- quantity > 0
- unit_price >= 0
- discount entre 0 e 1

Campos calculados esperados (sales_line / fact_sales):
- gross_sales = unit_price * quantity
- discount_value = gross_sales * discount
- net_sales = gross_sales - discount_value
- shipped_on_time_flag = shipped_date <= required_date
- shipping_delay_days = datediff(shipped_date, required_date)

O que eu quero que você me entregue:
A) Plano de execução em 3 notebooks (01 Bronze, 02 Silver, 03 Gold)
B) Código PySpark completo de cada notebook, com comentários curtos
C) Bloco final de validação com contagem de linhas por tabela criada
D) Instruções para conectar no Power BI (tabelas Gold e chaves)
E) Dicas de performance compatíveis com Free Tier (simples e objetivas)

Requisitos de resposta:
- Responda em português.
- Não use Delta Live Tables neste primeiro setup.
- Use apenas recursos que funcionem no Free Tier.
- Traga código pronto para copiar e colar.
- Se houver suposições, liste explicitamente antes do código.
```

## Opcional: versão SQL-first (se você preferir SQL)

```text
Refaça a solução priorizando SQL (Spark SQL) em vez de PySpark, mantendo exatamente as mesmas camadas, regras de qualidade, tabelas finais e validações.
```
