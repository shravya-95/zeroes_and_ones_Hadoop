---**********************************---
---CREATE SALES TABLE IN KUDU---
---**********************************---

---Create empty kudu table for sales with partitioning in order id range 
--Order id is increasing with time so this will also partition records based on year making it easier for map reduce 


CREATE TABLE IF NOT EXISTS kudu_sales (
   order_id int,
  sales_person_id int,
  customer_id int,
 product_id int,
quantity int,
sale_date timestamp,

  PRIMARY KEY (order_id)
)
PARTITION BY RANGE (order_id)
(
  PARTITION VALUES < 1000000,
  PARTITION 1000000 <= VALUES <2000000 ,
  PARTITION 2000000 <= VALUES <3000000,
PARTITION 3000000 <= VALUES <4000000,
  PARTITION 4000000 <= VALUES <5000000,
PARTITION 5000000 <= VALUES <6000000,
PARTITION 6000000 <= VALUES 
)
STORED AS KUDU;

CREATE  TABLE IF NOT EXISTS kudu_products(
product_id int,
name varchar,
price DOUBLE,
PRIMARY KEY (product_id)
)
PARTITION BY HASH PARTITIONS 4
STORED AS KUDU;


