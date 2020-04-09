--*************************************
--CREATE Parquet TABLES on Sales Data
--*************************************


SET VAR:database_name=zeroes_and_ones_sales;

--Create Parquet Product Region Sales Partitioned Table

CREATE TABLE IF NOT EXISTS ${var:database_name}.product_region_sales_partition
PARTITIONED BY (region,sales_year,sales_month)
COMMENT 'Parquet Product, Region and Sales partitioned table'
STORED AS Parquet
AS
Select se.order_id,se.sales_person_id,se.customer_id,se.product_id,se.product_name,
se.product_price,se.quantity,se.total_sales_amount, se.order_date,e.region,se.sales_year,se.sales_month
From ${var:database_name}.product_sales_partition se 
	join ${var:database_name}.employees e
	on(se.sales_person_id=e.employee_id);;

invalidate metadata;
compute stats ${var:database_name}.product_region_sales_partition; 