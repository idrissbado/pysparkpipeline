import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from sqlalchemy import text
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, concat, upper, lit, when, coalesce, round, expr, row_number, lag, lower
from pyspark.sql.types import DoubleType
import os  # For environment variables

# Database connection details using environment variables

source_db_params = {
    'host': '',
    'port': '3306',
    'user': 'idrissolivier_bado',
    'password': '',
    'database': 'money_transferdb'
}

target_db_params_postgres = {
    'host': '',
    'port': '5432',
    'user': 'postgres',
    'password': '',
    'database': 'datawarehouse'
}
target_db_params_mysql = {
    'host': '',
    'port': '3306',
    'user': 'bscv3',
    'password': '',
    'database': 'bsc_v3'
}
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bizao Data Processing") \
    .config("spark.jars", 
            "C:/Users/Idriss Olivier BADO/Downloads/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
    .getOrCreate()

# Database configurations
source_db_url = f"jdbc:mysql://{source_db_params['host']}:{source_db_params['port']}/{source_db_params['database']}"
target_postgres_db_url = f"jdbc:postgresql://{target_db_params_postgres['host']}:{target_db_params_postgres['port']}/{target_db_params_postgres['database']}"
target_mysql_db_url = f"jdbc:mysql://{target_db_params_mysql['host']}:{target_db_params_mysql['port']}/{target_db_params_mysql['database']}"

# JDBC properties
source_db_properties = {
    "user": source_db_params['user'],
    "password": source_db_params['password'],
    "driver": "com.mysql.cj.jdbc.Driver"
}

target_postgres_properties = {
    "user": target_db_params_postgres['user'],
    "password": target_db_params_postgres['password'],
    "driver": "org.postgresql.Driver"
}

# Load data from source MySQL database
query = """
    SELECT 
        a.order_id,
        a.transaction_id,
        CONCAT('Bizao', '_', UPPER(a.account_type), '_', 'daily_', UPPER(a.merchant_reference), '_', DATE(a.created_at)) AS settlement_name,
        a.created_at AS date,
        t.operator_transaction_id AS operator_transaction_id,
        DATE(a.created_at) AS setlement_date,
        MONTH(a.created_at) AS mois,
        a.merchant_reference AS marchand,
        a.account_number,
        a.currency_code AS currency,
        a.operation_type AS operation_type,
        a.transfer_amount AS volume,
        COALESCE(a.fee_amount, 0) AS fees,
        a.balance + a.transfer_amount + COALESCE(a.fee_amount, 0) AS balance_before,
        a.balance AS balance_finish,
        UPPER(a.account_type) AS account_type,
        UPPER(t.mno_name) AS mno_name,
        UPPER(t.country_code) AS country_code
    FROM 
        money_transferdb.mt_account_history a
    LEFT JOIN
        money_transferdb.mt_transactions t
    ON 
        UPPER(t.transaction_id) = UPPER(a.transaction_id)
    WHERE
        DATE(a.created_at) = CURDATE() - INTERVAL 1 DAY
        AND t.status = 'Successful'
        AND DATE(t.updated_date) = CURDATE() - INTERVAL 1 DAY
    ORDER BY 
        a.merchant_reference, a.created_at ASC
"""

try:
    df = spark.read.jdbc(url=source_db_url, table=f"({query}) as tmp", properties=source_db_properties)
except Exception as e:
    print(f"Error reading from MySQL: {e}")
    spark.stop()
    exit(1)

# Additional Calculations
df = df.withColumn("tva", when(col("country_code") == "CM", lit(0.1925)).otherwise(lit(0.18)))
df = df.withColumn("commission_ht", when(col("country_code") == "CM", col("fees") / 1.1925).otherwise(col("fees") / 1.18))
df = df.withColumn("reversement", col("volume") - col("fees"))

# Adding 'audit_desc' column
window_spec = Window.partitionBy("marchand").orderBy("date")
df = df.withColumn("audit_desc", when(row_number().over(window_spec) == 1, lit("cloture")).otherwise(
    when(col("balance_finish") == lag(col("balance_before")).over(window_spec), lit("0")).otherwise(lit("1"))
))

# Load 'taux_marchand' data from PostgreSQL
taux_query = """
    SELECT tid, 
           CASE WHEN fees_type <> 'FIXED_FEE' THEN taux_marchand ELSE fixed_fees END AS taux_marchand,
           CASE WHEN fees_type <> 'FIXED_FEE' THEN fixed_fees ELSE fixed_fees END AS fixed_fees
    FROM bsc_v3.mt_taux
"""
try:
    taux_data = spark.read.jdbc(url=target_postgres_db_url, table=f"({taux_query}) as taux", properties=target_postgres_properties)
except Exception as e:
    print(f"Error reading from PostgreSQL: {e}")
    spark.stop()
    exit(1)

# Create 'tid' in the main DataFrame for joining with taux_data
df = df.withColumn("tid", concat(upper(col("account_number")), lower(col("mno_name")), upper(col("country_code"))))

# Join the taux data
df = df.join(taux_data, on="tid", how="left").drop("tid")

# Calculate 'taux_marchand' as 'fees / volume' with rounding
df = df.withColumn("taux_marchand", round((col("fees") / col("volume")).cast(DoubleType()), 5))

# Generate 'eme_name' based on 'mno_name' and 'country_code'
df = df.withColumn("eme_name", 
    when(col("country_code") == "CI", concat(col("mno_name"), lit(" MONEY "), lit("Côte d'Ivoire"))) \
    .when(col("country_code") == "BJ", concat(col("mno_name"), lit(" MONEY "), lit("Benin"))) \
    .when(col("country_code") == "TG", concat(col("mno_name"), lit(" MONEY "), lit("Togo"))) \
    .when(col("country_code") == "SN", concat(col("mno_name"), lit(" MONEY "), lit("Senegal"))) \
    .when(col("country_code") == "BF", concat(col("mno_name"), lit(" MONEY "), lit("Burkina Faso"))) \
    .when(col("country_code") == "CM", concat(col("mno_name"), lit(" MONEY "), lit("Cameroon"))) \
    .when(col("country_code") == "CD", concat(col("mno_name"), lit(" MONEY "), lit("Democratic Republic of the Congo"))) \
    .otherwise(concat(col("mno_name"), lit(" MONEY "), lit("Unknown"))))

# Save to PostgreSQL

