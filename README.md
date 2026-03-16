# iphone_sales_analytics
iPhone Sales Analytics Platform built using HDFS, PySpark, Hive, and Medallion Architecture with a Star Schema data model.

Project Overview

This project builds a simple data pipeline to analyze iPhone sales across stores, customers, and products.
The pipeline reads raw CSV data, processes it using PySpark, stores it in Hive tables, and organizes the data using Medallion Architecture (Bronze → Silver → Gold).

The goal is to make the raw sales data ready for analytics queries like revenue by product or revenue by store.

Technologies used:

HDFS

PySpark

Hive

Parquet format

Medallion Architecture

Star Schema

Data Flow Architecture

The pipeline follows this flow:

CSV Files
   ↓
HDFS
   ↓
Bronze Layer (raw data)
   ↓
Silver Layer (cleaned data)
   ↓
Gold Layer (analytics tables)

Each layer has a specific purpose.

Step 1 – Raw Data (CSV)

We created four CSV files that represent sales data.

Files:

customers.csv

products.csv

stores.csv

sales.csv

These files were created in the Docker terminal and uploaded to HDFS.

Example location in HDFS:

/data/iphone/

These files contain information about:

customers

products

stores

sales transactions

Step 2 – Bronze Layer (Raw Ingestion)

The Bronze layer stores the raw data exactly as it is.

Here we read CSV files from HDFS using PySpark and save them as Hive tables in Parquet format.

Bronze tables created:

bronze_customers
bronze_products
bronze_stores
bronze_sales

Purpose:

keep raw data

convert CSV → Parquet

prepare data for transformation

Code location:

student_project/bronze/bronze_ingestion.py
Step 3 – Silver Layer (Data Cleaning)

The Silver layer cleans and standardizes the data.

Tasks performed:

enforce schema

convert column data types

prepare tables for joins

partition the sales table by date

Silver tables created:

silver_customers
silver_products
silver_stores
silver_sales

sales table is partitioned by:

sale_date

Code location:

student_project/silver/silver_transform.py
Step 4 – Gold Layer (Star Schema)

The Gold layer creates analytics tables using a Star Schema.

Structure:

          dim_date
             |
dim_customer -- fact_sales -- dim_product
             |
          dim_store

Dimension tables:

dim_customer
dim_product
dim_store
dim_date

Fact table:

fact_sales

The fact table contains:

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

Code location:

student_project/gold/gold_loader.py
Hive Tables in HDFS

All tables are stored in Hive warehouse:

/data/hive/warehouse/iphone_analytics.db/

This directory contains:

bronze_tables
silver_tables
dimension_tables
fact_sales

This proves the Medallion architecture is implemented.

Business Queries

Example analytics queries we ran in Hive.

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

These queries show how the data can be used for analytics.

Data Validation

We validated the pipeline by checking row counts.

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
│   │   └── gold_loader.py
│   ├── sql
│   │   └── analytics_queries.sql
│   └── README.md
│
└── screenshots
Summary

In this project we built a small data engineering pipeline that:

Reads raw sales data from CSV files

Stores the raw data in Bronze tables

Cleans and standardizes the data in Silver tables

Creates analytics tables in the Gold layer

Runs business queries using Hive

The final result is a structured dataset that can be easily used for sales analytics.
