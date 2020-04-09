--*************************************
--CREATE Parquet TABLES on Sales Data
--*************************************


SET VAR:database_name=zeroes_and_ones_sales;

--Create Parquet Product Sales Partitioned Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.product_sales_partition
PARTITIONED BY (sales_year,sales_month)
COMMENT 'Parquet Product and Sales partitioned table'
STORED AS Parquet
AS
SELECT s.order_id,s.sales_person_id,s.customer_id,s.product_id,p.name as product_name,cast(p.price as decimal(15,2)) as product_price,
s.quantity,cast(s.quantity*p.price as decimal(15,2)) as total_sales_amount, s.sale_date as order_date,date_part('year',s.sale_date) as sales_year,
decode(date_part('month',s.sale_date), 1, "Jan", 2, "Feb", 3, "Mar",4,"Apr",5,"May",6,"Jun",7,"Jul",8,"Aug",9,"Sep",10,"Oct",11,"Nov",12,"Dec") as sales_month
FROM ${var:database_name}.sales s JOIN ${var:database_name}.products p USING(product_id);

invalidate metadata;
compute stats ${var:database_name}.product_sales_partition;
