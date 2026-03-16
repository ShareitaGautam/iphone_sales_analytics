# iPhone Sales Analytics Platform

iPhone Sales Analytics Platform built using **HDFS, PySpark, Hive, and Medallion Architecture** with a **Star Schema data model**.

---

## Project Overview

This project builds a simple data pipeline to analyze iPhone sales across stores, customers, and products.

The pipeline reads raw CSV data, processes it using PySpark, stores the results in Hive tables, and organizes the data using **Medallion Architecture (Bronze → Silver → Gold)**.

The goal is to transform raw sales data into a structured format that can be used for analytics queries such as revenue by product or revenue by store.

---

## Technologies Used

- HDFS
- PySpark
- Hive
- Parquet
- Medallion Architecture
- Star Schema

---

## Data Pipeline Architecture

```text
CSV Files
   ↓
HDFS
   ↓
Bronze Layer (raw data)
   ↓
Silver Layer (cleaned data)
   ↓
Gold Layer (analytics tables)

Each layer prepares the data for the next stage.

Step 1 – Raw Data (CSV)

Four CSV files were created to simulate store sales data.

Files

customers.csv

products.csv

stores.csv

sales.csv

These files were created in the Docker terminal and uploaded to HDFS.

Example HDFS location:

/data/iphone/

These files contain information about:

customers

products

stores

sales transactions

Step 2 – Bronze Layer (Raw Ingestion)

The Bronze layer stores the raw data exactly as it is.

CSV files are read from HDFS using PySpark and saved as Hive tables in Parquet format.

Bronze Tables
bronze_customers
bronze_products
bronze_stores
bronze_sales
Purpose

Keep raw data

Convert CSV to Parquet

Prepare data for transformation

Code Location
student_project/bronze/bronze_ingestion.py
Step 3 – Silver Layer (Data Cleaning)

The Silver layer cleans and standardizes the data.

Tasks Performed

Enforce schema

Convert column data types

Prepare tables for joins

Partition the sales table by date

Silver Tables
silver_customers
silver_products
silver_stores
silver_sales

The sales table is partitioned by:

sale_date
Code Location
student_project/silver/silver_transform.py
Step 4 – Gold Layer (Star Schema)

The Gold layer creates analytics tables using a Star Schema.

Schema Structure
          dim_date
             |
dim_customer -- fact_sales -- dim_product
             |
          dim_store
Dimension Tables
dim_customer
dim_product
dim_store
dim_date
Fact Table
fact_sales
Fact Table Columns
sale_id
customer_id
product_id
store_id
date_key
quantity
total_amount

total_amount is calculated as:

quantity × unit_price

The fact table is partitioned by:

date_key
Code Location
student_project/gold/gold_loader.py
Hive Tables in HDFS

All Hive tables are stored in:

/data/hive/warehouse/iphone_analytics.db/

This directory contains:

Bronze tables

Silver tables

Dimension tables

Fact table

This confirms that the Medallion Architecture is implemented.

Example Business Queries
Revenue by Product
SELECT p.product_name, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p
ON f.product_id = p.product_id
GROUP BY p.product_name;
Revenue by Store
SELECT s.store_name, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_store s
ON f.store_id = s.store_id
GROUP BY s.store_name;

These queries demonstrate how the processed data can be used for analytics.

Data Validation

Row counts were checked to ensure data integrity.

SELECT COUNT(*) FROM bronze_sales;
SELECT COUNT(*) FROM silver_sales;
SELECT COUNT(*) FROM fact_sales;

Result:

4 rows in each table

This confirms there was no data loss during processing.

Project Folder Structure
iphone-sales-project
│
├── common_utils
│   ├── schemas
│   ├── spark_utils.py
│   └── constants.py
│
├── student_project
│   ├── bronze
│   │   └── bronze_ingestion.py
│   ├── silver
│   │   └── silver_transform.py
│   ├── gold
│   │   │── gold_loader.py
│   ├── sql
│   │   └── analytics_queries.sql
│   └── README.md
│
└── screenshots
Summary

In this project we built a simple data engineering pipeline that:

Reads raw sales data from CSV files

Stores the raw data in Bronze tables

Cleans and standardizes the data in Silver tables

Creates analytics tables in the Gold layer

Runs business queries using Hive

The final result is a structured dataset that can be used for sales analysis.
