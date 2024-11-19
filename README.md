# pysparkpipeline
This project is set up to process and transform data from a source MySQL database related to Bizao's Money Transfer transactions, perform necessary transformations, and load the enriched data into a target PostgreSQL database. Here's an overview of the main components and objectives of this project:

Project Description: Bizao Money Transfer Data Processing
The goal of this project is to build a data pipeline that extracts money transfer transaction data from a source MySQL database, enriches and transforms it using PySpark, and then loads it into a PostgreSQL data warehouse for further analysis or reporting.

1. Project Environment Setup
Environment Variables: The code uses os to manage secure database credentials from environment variables.
Database Configurations: Configuration details for MySQL (source) and PostgreSQL/MySQL (target) connections are set up using JDBC properties.
Spark Session: Initiates a Spark session with MySQL JDBC driver configuration.
2. Data Extraction from MySQL
A complex SQL query fetches daily transaction data, including key fields like order_id, transaction_id, account_type, merchant_reference, created_at, transfer_amount, fees, and related transaction identifiers.
The query ensures that only successful transactions updated in the last day are retrieved and organized by merchant reference and timestamp.
3. Data Transformations Using PySpark
Column Additions: Calculations and column additions include:
tva: Value-added tax based on the country code.
commission_ht: Fees excluding VAT.
reversement: Volume minus fees.
Audit Description: A new column, audit_desc, indicates transaction auditing status using window functions and row comparisons.
Data Enrichment: Generates a tid column for joining with additional fee data.
4. Join with Fee Data from PostgreSQL
Data from the mt_taux table is loaded from the target PostgreSQL database to calculate merchant rates.
Join Operation: Joins are performed using the tid key, and the fee data is integrated into the main DataFrame.
5. Further Data Transformations
Calculates taux_marchand (rounded rate) based on fees and volume.
Constructs a descriptive eme_name column based on the country and mobile network operator (MNO).
6. Data Loading to PostgreSQL
Once all transformations are complete, the enriched data is saved back to the target PostgreSQL database for use in reports or analytics.
Key Components:
Source Data: Money transfer and account history data from MySQL.
Data Enrichment: Using fee rates from PostgreSQL, VAT calculations, and audit status determination.
Data Transformation: Utilizing PySpark for performance and scalability.
Destination: PostgreSQL data warehouse for business analysis.
Execution Notes:
JDBC Driver: Ensure the correct path to the MySQL JDBC driver is specified for Spark to connect to the database.
Performance Considerations: PySpark is used to efficiently handle large volumes of data.
Error Handling: Basic error handling ensures Spark stops if data extraction fails.
This setup facilitates efficient and scalable data processing and is adaptable for future data transformations or integration with other sources. The project provides a robust framework for handling Bizao's transactional data needs and serves as a foundation for deeper data analytics tasks.
