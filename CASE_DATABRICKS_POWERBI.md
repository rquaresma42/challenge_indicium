# Case Pratico: Analytics de Vendas com Databricks + Power BI

## 1) Objetivo do case
Construir uma solucao de analytics ponta a ponta para responder:
- Como as vendas evoluem ao longo do tempo?
- Quais produtos, categorias e clientes mais geram receita e margem potencial?
- Qual o desempenho por vendedor e por regiao?
- Onde existem gargalos operacionais (atraso de envio, desconto excessivo)?

Foco tecnico:
- Engenharia e transformacao no Databricks (Lakehouse + Medallion)
- Consumo analitico no Power BI com modelo estrela e medidas DAX

## 2) Dataset validado
Arquivos CSV lidos em data/ com separador ;.

Volumes identificados:
- orders: 830 linhas
- order_details: 2155 linhas
- products: 77 linhas
- customers: 91 linhas
- employees: 9 linhas
- Janela temporal de order_date: 1996-07-04 a 1998-05-06

Tabelas principais:
- Fato transacional: order_details + orders
- Dimensoes: products, categories, customers, employees, shippers, territories, region, suppliers

## 3) Arquitetura proposta (Databricks)
## 3.1 Ingestao
- Origem: arquivos CSV em DBFS ou ADLS/S3
- Notebook/Job Databricks para carga inicial e incremental
- Persistencia em Delta Lake

## 3.2 Camadas Medallion
- Bronze:
  - Copia bruta dos CSVs sem regra de negocio
  - Auditoria: ingest_timestamp, source_file
- Silver:
  - Tipagem de colunas
  - Tratamento de nulos e chaves
  - Padronizacao de datas
  - Join entre orders e order_details para granularidade de item de pedido
- Gold:
  - Modelo analitico pronto para BI
  - Fato de vendas + dimensoes conformadas
  - Tabelas agregadas para performance (ex.: vendas mensais por categoria)

## 3.3 Orquestracao e governanca
- Databricks Workflows para agendamento diario
- Delta Live Tables (opcional) para pipeline declarativo
- Unity Catalog para governanca de objetos
- Qualidade de dados com regras basicas:
  - order_id nao nulo
  - quantity > 0
  - unit_price >= 0
  - desconto entre 0 e 1

## 4) Modelo de dados para Power BI
## 4.1 Grao da fato
Uma linha por item de pedido (order_id + product_id).

## 4.2 Tabelas
- Fato:
  - fact_sales
- Dimensoes:
  - dim_date
  - dim_product
  - dim_category
  - dim_customer
  - dim_employee
  - dim_shipper
  - dim_territory
  - dim_region

## 4.3 Campos calculados no Gold
- gross_sales = unit_price * quantity
- discount_value = gross_sales * discount
- net_sales = gross_sales - discount_value
- shipped_on_time_flag = shipped_date <= required_date
- shipping_delay_days = datediff(shipped_date, required_date)

## 5) KPIs para o dashboard (Power BI)
- Receita liquida (Net Sales)
- Pedidos
- Ticket medio
- % pedidos enviados no prazo
- Desconto medio
- Receita por categoria
- Top 10 clientes
- Desempenho por vendedor
- Receita por pais/cidade

Medidas DAX base:
```DAX
Net Sales = SUM(fact_sales[net_sales])

Orders = DISTINCTCOUNT(fact_sales[order_id])

Average Ticket = DIVIDE([Net Sales], [Orders])

On Time % =
DIVIDE(
    CALCULATE(COUNTROWS(fact_sales), fact_sales[shipped_on_time_flag] = TRUE()),
    COUNTROWS(fact_sales)
)
```

## 6) Storytelling sugerido para apresentacao
- Pagina 1: Visao executiva (KPIs + tendencia mensal)
- Pagina 2: Produtos e categorias (pareto e mix)
- Pagina 3: Clientes e geografico (top clientes, mapa)
- Pagina 4: Operacao logistica (prazo, atraso, transportadora)
- Pagina 5: Vendas por vendedor e territorio

## 7) Diferenciais para entrevista/case tecnico
- Incremental load em Delta (merge/upsert)
- Monitoramento de qualidade e alertas de falha
- Otimizacoes de performance:
  - Z-ORDER nas colunas de filtro (order_date, customer_id)
  - Tabelas agregadas para consumo no Power BI
- Parametrizacao de ambiente (dev/hml/prod)
- Versionamento do codigo (Git) e CI/CD basico

## 8) Roadmap de execucao (5 dias)
- Dia 1: Ingestao Bronze e catalogacao
- Dia 2: Limpeza Silver + regras de qualidade
- Dia 3: Gold + modelo estrela
- Dia 4: Dashboard Power BI + DAX
- Dia 5: Refino, performance, documentacao e pitch final

## 9) Entregaveis
- Notebooks Databricks (ingestao, silver, gold)
- Tabelas Delta no catalog/schema
- Arquivo Power BI (.pbix) com 4-5 paginas
- Documento de arquitetura e decisoes tecnicas
- Video curto (5-8 min) explicando abordagem e insights

## 10) Proximos passos praticos neste repositorio
1. Criar notebook de ingestao para ler data/*.csv com delimiter=';'.
2. Criar tabela fato unificando orders + order_details.
3. Gerar dimensao calendario a partir de order_date.
4. Publicar camada Gold para consumo do Power BI.
5. Montar dashboard e validar KPIs com amostras do dataset.
