--Query to Create a query that will give the total dollar amount sold by year



SELECT sum(p.price) as total_dollar, date_part('year',s.sale_date) as year FROM kudu_products p JOIN kudu_sales s ON p.product_id=s.product_id GROUP BY date_part('year',s.sale_date);
