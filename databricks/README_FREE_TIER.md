# Databricks Free Tier: Setup + Execucao do Case

Este guia inicia o projeto no Databricks usando os CSVs da pasta local data/.

## 1. Pre-requisitos
- Conta no Databricks Free Edition (free tier)
- Workspace ativo
- Compute iniciado (single node ja e suficiente para este case)
- Arquivos CSV locais em data/

## 2. Criar schemas no metastore
No Databricks, abra um notebook e rode:

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.indicium_bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.indicium_silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.indicium_gold")
```

## 3. Subir os arquivos para o Databricks
Voce pode usar um dos dois metodos abaixo.

Como voce ja carregou no Volume, o caminho recomendado para este projeto e:
- /Volumes/workspace/indicium_bronze/raw_data

Os notebooks ja estao preparados com esse caminho por padrao.

### Metodo A (UI - mais simples)
1. No Databricks, abra Catalog e navegue ate workspace > indicium_bronze > Volumes > raw_data.
2. Faça upload de todos os CSVs da pasta local data/ para esse volume.
4. Valide com:

```python
display(dbutils.fs.ls("/Volumes/workspace/indicium_bronze/raw_data"))
```

### Metodo B (CLI - opcional)
No seu terminal local, com Databricks CLI configurado:

```powershell
databricks fs mkdirs dbfs:/FileStore/indicium
databricks fs cp --recursive .\data dbfs:/FileStore/indicium/
```

Se usar esse metodo, confirme no notebook:

```python
display(dbutils.fs.ls("dbfs:/FileStore/indicium/data/"))
```

Observacao:
- No setup recomendado com Volume: /Volumes/workspace/indicium_bronze/raw_data/arquivo.csv.
- No Metodo B (CLI) acima, os arquivos podem ficar em dbfs:/FileStore/indicium/data/arquivo.csv.

## 4. Importar notebooks deste repositorio
Arquivos prontos para importar no workspace Databricks:
- databricks/01_bronze_ingestion.py
- databricks/02_silver_transformations.py
- databricks/03_gold_star_schema.py

## 5. Ordem de execucao
1. Rode `01_bronze_ingestion.py`
2. Rode `02_silver_transformations.py`
3. Rode `03_gold_star_schema.py`

## 6. Parametros importantes
No notebook 01, ajuste:
- `base_path` para onde os CSVs realmente ficaram (ex.: /Volumes/workspace/indicium_bronze/raw_data)
- `catalog` e schemas se quiser outro nome

## 7. Checagens rapidas
Depois da carga Gold:

```python
spark.table("workspace.indicium_gold.fact_sales").count()
spark.table("workspace.indicium_gold.dim_product").count()
spark.table("workspace.indicium_gold.dim_customer").count()
```

## 8. Conectar no Power BI
No Power BI Desktop:
1. Get Data > Azure > Azure Databricks
2. Informe Server Hostname e HTTP Path do SQL Warehouse/Compute
3. Selecione as tabelas da camada Gold:
   - `indicium_gold.fact_sales`
   - `indicium_gold.dim_date`
   - `indicium_gold.dim_product`
   - `indicium_gold.dim_customer`
   - `indicium_gold.dim_employee`
   - `indicium_gold.dim_shipper`
   - `indicium_gold.dim_territory`
   - `indicium_gold.dim_region`

## 9. Dica para free tier
- Use poucas transformacoes por notebook e salve em Delta a cada etapa.
- Evite jobs simultaneos e mantenha o compute pequeno para nao atingir limites rapidamente.
