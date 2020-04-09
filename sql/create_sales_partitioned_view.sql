--*************************************
--CREATE Views on Sales Data
--*************************************


SET VAR:database_name=zeroes_and_ones_sales;
--View: customer_monthly_sales_2019_partitioned_view
--Customer id, customer last name, customer first name,
--year, month, aggregate total amount
--of all products purchased by month for 2019.

CREATE VIEW IF NOT EXISTS ${var:database_name}.customer_monthly_sales_2019_partitioned_view as
Select s.customer_id,c.last_name,c.first_name,s.sales_year,s.sales_month,cast(sum(s.total_sales_amount) as decimal(15,2)) as agg_total_amount
From  ${var:database_name}.product_sales_partition s 
	join ${var:database_name}.customers c
	using(customer_id)
	where s.sales_year=2019
	group by s.customer_id,c.last_name,c.first_name,s.sales_year,s.sales_month;;

