--*************************************
--CREATE Views on Sales Data
--*************************************


SET VAR:database_name=zeroes_and_ones_sales;
--View: customer_monthly_sales_2019_view
--Customer id, customer last name, customer first name,
--year, month, aggregate total amount
--of all products purchased by month for 2019.

CREATE VIEW IF NOT EXISTS ${var:database_name}.customer_monthly_sales_2019_view as
Select c.customer_id,c.last_name,c.first_name,st.year,st.month,cast(sum(st.total_amount) as decimal(15,2)) as agg_total_amount
From (Select s.customer_id,date_part('year',s.sale_date) as year,
	decode(date_part('month',s.sale_date), 1, "Jan", 2, "Feb", 3, "Mar",4,"Apr",5,"May",6,"Jun",7,"Jul",8,"Aug",9,"Sep",10,"Oct",11,"Nov",12,"Dec") as month,
	(s.quantity*p.price) as total_amount From ${var:database_name}.sales s 
	join ${var:database_name}.products p using(product_id) where date_part('year',s.sale_date)=2019) as st 
	join ${var:database_name}.customers c
	using(customer_id) 
	group by c.customer_id,c.last_name,c.first_name,st.year,st.month;;

--View: top_ten_customers_amount_view
--Customer id, customer last name, customer first name,
--total lifetime purchased aggregate total amount
--of all products purchased by month for 2019.


CREATE VIEW IF NOT EXISTS ${var:database_name}.top_ten_customers_amount_view as
Select c.customer_id,c.last_name,c.first_name,cast(sum(st.total_amount) as decimal(15,2)) as total_lifetime_amount
From (Select s.customer_id, (s.quantity*p.price) as total_amount From ${var:database_name}.sales s 
	join ${var:database_name}.products p using(product_id)) as st 
	join ${var:database_name}.customers c
	using(customer_id) 
	group by c.customer_id,c.last_name,c.first_name
	ORDER BY total_lifetime_amount DESC
	LIMIT 10;;
