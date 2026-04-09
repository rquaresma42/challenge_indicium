# Northwind Traders - Setup Power BI com Databricks

## 1) Conexao com Databricks
No Power BI Desktop:
1. Home > Get data > Azure > Azure Databricks
2. Preencha:
   - Server hostname: <SEU_SERVER_HOSTNAME>
   - HTTP path: <SEU_HTTP_PATH>
3. Authentication: Organizational account
4. Data Connectivity mode: Import

## 2) Tabelas para importar (camada Gold)
Importe as tabelas abaixo do catalog workspace:
- workspace.indicium_gold.fact_sales
- workspace.indicium_gold.dim_date
- workspace.indicium_gold.dim_product
- workspace.indicium_gold.dim_category
- workspace.indicium_gold.dim_customer
- workspace.indicium_gold.dim_employee
- workspace.indicium_gold.dim_shipper
- workspace.indicium_gold.dim_territory
- workspace.indicium_gold.dim_region
- workspace.indicium_gold.bridge_employee_territory

## 3) Relacionamentos recomendados
Crie os relacionamentos abaixo no Model view:
1. fact_sales[order_date_key] -> dim_date[date_key] (Many-to-one, single)
2. fact_sales[product_id] -> dim_product[product_id] (Many-to-one, single)
3. dim_product[category_id] -> dim_category[category_id] (Many-to-one, single)
4. fact_sales[customer_id] -> dim_customer[customer_id] (Many-to-one, single)
5. fact_sales[employee_id] -> dim_employee[employee_id] (Many-to-one, single)
6. fact_sales[shipper_id] -> dim_shipper[shipper_id] (Many-to-one, single)
7. bridge_employee_territory[employee_id] -> dim_employee[employee_id] (Many-to-one, single)
8. bridge_employee_territory[territory_id] -> dim_territory[territory_id] (Many-to-one, single)
9. dim_territory[region_id] -> dim_region[region_id] (Many-to-one, single)

Observacao:
- Para evitar ambiguidade, mantenha os filtros com direcao Single.
- Se precisar analisar por territorio na fato, use medidas com TREATAS (ao inves de filtro bidirecional global).

## 4) Medidas DAX iniciais
Crie as medidas na tabela fact_sales:

Net Sales =
SUM(fact_sales[net_sales])

Gross Sales =
SUM(fact_sales[gross_sales])

Discount Value =
SUM(fact_sales[discount_value])

Orders =
DISTINCTCOUNT(fact_sales[order_id])

Average Ticket =
DIVIDE([Net Sales], [Orders])

On Time Orders =
CALCULATE(
    COUNTROWS(fact_sales),
    fact_sales[shipped_on_time_flag] = TRUE()
)

On Time % =
DIVIDE([On Time Orders], COUNTROWS(fact_sales))

## 5) Checagem rapida de qualidade no Power BI
Depois de carregar:
1. Compare o total de linhas da fact_sales com Databricks.
2. Confira se nao existem relacionamentos many-to-many inesperados.
3. Valide se as medidas Net Sales e Orders retornam valores em uma matriz por Year e Category.

## 6) Estrutura de paginas sugerida
1. Executive Overview: KPIs (Net Sales, Orders, Average Ticket, On Time %)
2. Produtos e Categorias: Pareto de receita, mix por categoria
3. Clientes: Top clientes e distribuicao geografica
4. Operacao Logistica: prazo de envio, atraso medio e ranking de transportadoras
5. Vendedores e Territorios: receita e volume por vendedor

## 7) Publicacao
1. Salve como PBIP (preview habilitado no Desktop)
2. Commit no Git do projeto
3. Publish para o workspace no Power BI Service

## 8) Se quiser automatizar depois
- Migrar para DirectQuery em tabelas maiores
- Criar incremental refresh
- Parametrizar catalog/schema para DEV e PROD
