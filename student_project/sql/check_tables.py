"""
Utility Script: check_tables.py

Purpose:
This script is used to quickly verify that all tables in the data pipeline
(Bronze → Silver → Gold) were created successfully in Hive.

It performs the following checks:
1. Lists all tables in the Hive database
2. Displays sample records from important tables
3. Validates row counts to confirm no data loss occurred
"""

from common_utils.spark_utils import get_spark_session
from common_utils.constants import DATABASE_NAME


# ---------------------------------------------------
# Start Spark Session
# This creates a Spark session with Hive support
# so we can query Hive tables using Spark SQL.
# ---------------------------------------------------
spark = get_spark_session()

# ---------------------------------------------------
# Select the correct Hive database
# All project tables are stored in iphone_analytics
# ---------------------------------------------------
spark.sql(f"USE {DATABASE_NAME}")

# ---------------------------------------------------
# Show all tables in the database
# This helps confirm Bronze, Silver, and Gold tables
# were created successfully.
# ---------------------------------------------------
print("\n---- All Tables in Database ----")
spark.sql("SHOW TABLES").show()

# ---------------------------------------------------
# Display sample records from the Silver layer
# This confirms the cleaned data is stored correctly.
# ---------------------------------------------------
print("\n---- Sample Data: silver_sales ----")
spark.sql("SELECT * FROM silver_sales").show()

# ---------------------------------------------------
# Display sample records from the Gold layer fact table
# This confirms that the fact table was built correctly
# and contains calculated metrics like total_amount.
# ---------------------------------------------------
print("\n---- Sample Data: fact_sales ----")
spark.sql("SELECT * FROM fact_sales").show()

# ---------------------------------------------------
# Validate row counts across layers
# This confirms that the pipeline did not lose data
# during Bronze → Silver → Gold transformations.
# ---------------------------------------------------
print("\n---- Row Count Validation ----")
spark.sql("SELECT COUNT(*) FROM bronze_sales").show()
spark.sql("SELECT COUNT(*) FROM silver_sales").show()
spark.sql("SELECT COUNT(*) FROM fact_sales").show()

# ---------------------------------------------------
# Optional: Show schema of the fact table
# This helps verify table structure and column types.
# ---------------------------------------------------
print("\n---- Fact Table Schema ----")
spark.sql("DESCRIBE fact_sales").show()