
--*************************************
--CREATE EXTERNAL TABLES on Raw Train Data
--*************************************

SET VAR:database_name=zeroes_and_ones_sales_raw;

Create Database IF NOT EXISTS ${var:database_name}
COMMENT 'Raw sales data imported from salesdb';


--Create External customers Table

Create EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.customers (
customer_id int,
first_name varchar,
middle_initial varchar,
last_name varchar)
COMMENT 'Sales Customers table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/salesdb/Customers2/'
TBLPROPERTIES ("skip.header.line.count"="1");



--Create External Products Table

CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.products(
product_id int,
name varchar,
price DOUBLE)
COMMENT 'Sales products table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/salesdb/Products/'
TBLPROPERTIES ("skip.header.line.count"="1");

--Create External Products Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.sales (
order_id int,
sales_person_id int,
customer_id int,
product_id int,
quantity int,
sale_date timestamp)
COMMENT 'Sales table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/salesdb/Sales2/'
TBLPROPERTIES ("skip.header.line.count"="1");

--Create External Employees Table
CREATE EXTERNAL TABLE IF NOT EXISTS ${var:database_name}.employees (
employee_id int,
first_name varchar,
middle_initial varchar,
last_name varchar,
region varchar)
COMMENT 'trains direction table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/salesdb/Employees2/'
TBLPROPERTIES ("skip.header.line.count"="1");

invalidate metadata;
compute stats ${var:database_name}.customers;
compute stats ${var:database_name}.products;
compute stats ${var:database_name}.sales;
compute stats ${var:database_name}.employees;
