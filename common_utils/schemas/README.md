# Schemas Folder

This folder is reserved for reusable PySpark schema definitions.

In this project, schema enforcement was handled in the Silver layer using column casting, so separate schema files were not required.

Examples of schema handling used in this project:
- customer_id cast to int
- product_id cast to int
- store_id cast to int
- quantity cast to int
- sale_date converted to date