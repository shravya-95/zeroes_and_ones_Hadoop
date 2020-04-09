--*************************************
--CREATE Parquet TABLES on Sales Data
--*************************************


SET VAR:database_name=zeroes_and_ones_sales;

--Create Parquet Product Region Sales Partitioned Table
CREATE TABLE IF NOT EXISTS ${var:database_name}.product_region_sales_partition
PARTITIONED BY (region,sales_year,sales_month)
STORED AS Parquet
AS
Select se.order_id,se.sales_person_id,se.customer_id,se.product_id,p.name as product_name,cast(p.price as decimal(15,2)) as product_price,
se.quantity,cast(se.quantity*p.price as decimal(15,2)) as total_sales_amount, se.sale_date as order_date,se.region,date_part('year',se.sale_date) as sales_year,
decode(date_part('month',se.sale_date), 1, "Jan", 2, "Feb", 3, "Mar",4,"Apr",5,"May",6,"Jun",7,"Jul",8,"Aug",9,"Sep",10,"Oct",11,"Nov",12,"Dec") as sales_month
From (Select s.*,e.region From ${var:database_name}.sales s 
	join ${var:database_name}.employees e on(s.sales_person_id=e.employee_id)) as se
	join ${var:database_name}.products p
	using(product_id);;

invalidate metadata;
compute stats ${var:database_name}.product_region_sales_partition;