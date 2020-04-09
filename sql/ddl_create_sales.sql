--*************************************
--CREATE Parquet TABLES on Sales Data
--*************************************


SET VAR:database_name=zeroes_and_ones_sales;

SET VAR:source_database=zeroes_and_ones_sales_raw;

Create Database IF NOT EXISTS ${var:database_name}
COMMENT 'Parquet Sales data imported from Sales raw database';

--Create Parquet Customers Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.customers
COMMENT 'Parquet customers table'
STORED AS Parquet
AS
SELECT customer_id,first_name,last_name FROM ${var:source_database}.customers;

--Create Parquet Products Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.products
STORED AS Parquet
AS
SELECT * FROM ${var:source_database}.products;

--Create Parquet Sales Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.sales
COMMENT 'Parquet sales table'
STORED AS Parquet
AS
SELECT * FROM ${var:source_database}.sales;

--Create Parquet Employees Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.employees
COMMENT 'Parquet employees table'
STORED AS Parquet
AS
SELECT employee_id,first_name,middle_initial,last_name,lower(region) AS region FROM ${var:source_database}.employees;

invalidate metadata;
compute stats ${var:database_name}.customers;
compute stats ${var:database_name}.sales;
compute stats ${var:database_name}.employees;
compute stats ${var:database_name}.products;