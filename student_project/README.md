# iPhone Sales Analytics Platform

## Technologies
- HDFS
- PySpark
- Hive
- Parquet
- Medallion Architecture

## Architecture
Bronze → Silver → Gold

## Layers

### Bronze
Raw CSV data ingested from HDFS and stored as Parquet tables.

### Silver
Data cleaned, schema enforced, and partitioned.

### Gold
Star schema with dimension tables and fact table for analytics.

## Tables

### Bronze
bronze_customers  
bronze_products  
bronze_stores  
bronze_sales  

### Silver
silver_customers  
silver_products  
silver_stores  
silver_sales  

### Gold
dim_customer  
dim_product  
dim_store  
dim_date  
fact_sales  

## Example Queries
Revenue by product  
Revenue by store