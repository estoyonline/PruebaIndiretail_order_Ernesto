-- 01 - Master tables.sql


/**************************************
  actual_stock.parquet - 51.815
  
- StoreId (integer): store code.
- ProductId (integer): product code.
- Stock (long):  units of stock of the product at the store.
*/
DROP TABLE IF EXISTS temp.parq_actual_stock;
CREATE EXTERNAL TABLE temp.parq_actual_stock(StoreId int, ProductId int, Stock number)
AS COPY FROM '/home/dbadmin/Documents/data/actual_stock.parquet'
PARQUET(do_soft_schema_match_by_name='True');


/************************************** 
  receptions.parquet - 118

- StoreId (integer): store code.
- ProductId (integer): product code.
- Week (string):  week of the reception.
- Reception (long):  units of stock to receive.
*/
DROP TABLE IF EXISTS temp.parq_receptions;
CREATE EXTERNAL TABLE temp.parq_receptions(StoreId int, ProductId int, Week varchar, Reception number)
AS COPY FROM '/home/dbadmin/Documents/data/receptions.parquet'
PARQUET(do_soft_schema_match_by_name='True');


/**************************************
  projection.parquet - 171.660

- StoreId (integer): store code.
- ProductId (integer): product code.
- Week (string): week of the year.
- Projection (long):  projected sales.
*/
DROP TABLE IF EXISTS temp.parq_projection;
CREATE EXTERNAL TABLE temp.parq_projection(StoreId int, ProductId int, Week varchar, Projection number)
AS COPY FROM '/home/dbadmin/Documents/data/projection.parquet'
PARQUET(do_soft_schema_match_by_name='True');


-- LOG
SELECT 'temp.parq_actual_stock' AS tabla, count(*) AS registros FROM temp.parq_actual_stock UNION ALL 
SELECT 'temp.parq_receptions' AS tabla, count(*) AS registros FROM temp.parq_receptions UNION ALL 
SELECT 'temp.parq_projection' AS tabla, count(*) AS registros FROM temp.parq_projection;


-- ************************************
-- temp.dim_date
-- ************************************
CREATE OR REPLACE VIEW temp.dim_date AS
SELECT 
	ts::date AS d_date,
	TO_CHAR(ts::date, 'Day') d_day,
	EXTRACT('Day' FROM ts::date) d_day_of_month,
	EXTRACT('DOY' FROM ts::date) d_day_of_year,
	EXTRACT('Week' FROM ts::date) d_week_of_year,
	EXTRACT('Quarter' FROM ts::date) d_quarter,
	EXTRACT('DOQ' FROM ts::date) d_day_of_quarter
FROM (
	SELECT '01-JAN-2022'::TIMESTAMP as tm 
	UNION SELECT '31-DEC-2022'::TIMESTAMP as tm
) AS t
TIMESERIES ts as '1 DAY' OVER (ORDER BY tm);


-- ************************************
-- temp.dim_store - 36
-- ************************************
DROP TABLE IF EXISTS temp.dim_store;
SELECT StoreId
INTO temp.dim_store
FROM (	
	SELECT DISTINCT StoreId FROM parq_actual_stock
	UNION 
	SELECT DISTINCT StoreId FROM parq_receptions
	UNION 
	SELECT DISTINCT StoreId FROM parq_projection
) AS s;


-- ************************************
-- temp.dim_product - 8.674
-- ************************************
DROP TABLE IF EXISTS temp.dim_product;
SELECT ProductId
INTO temp.dim_product
FROM ( 
	SELECT DISTINCT ProductId FROM temp.parq_actual_stock
	UNION 
	SELECT DISTINCT ProductId FROM temp.parq_receptions
	UNION 
	SELECT DISTINCT ProductId FROM temp.parq_projection
) AS p;


-- LOG
SELECT 'temp.dim_store' AS tabla, count(*) AS registros FROM temp.dim_store UNION ALL 
SELECT 'temp.dim_product' AS tabla, count(*) AS registros FROM temp.dim_product UNION ALL 
SELECT 'temp.dim_date' AS tabla, count(*) AS registros FROM temp.dim_date;

