--***************************************
--INSERT DATA INTO KUDU SALES TABLE
--***************************************


---inserting parquet sales tables into kudu sales table

INSERT INTO kudu_sales
SELECT * FROM zeroes_and_ones_sales.sales;

INSERT INTO kudu_products
SELECT * FROM zeroes_and_ones_sales.products;
