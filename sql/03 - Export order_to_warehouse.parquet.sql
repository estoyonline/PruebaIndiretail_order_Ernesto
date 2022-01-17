-- 03 - Export order_to_warehouse.parquet.sql

-- ************************************
-- order_to_warehouse.parquet - 16.549.992 - 2m 10s
-- ************************************
EXPORT TO PARQUET(directory = '/home/dbadmin/Documents/data/order_to_warehouse.parquet') 
OVER(PARTITION BY StoreId ORDER BY ProductId) 
AS SELECT  
	Week,
	StoreId,
	ProductId,
	Projection,
	Reception,
	Stock AS AvailableStock,
	StockNeed,
	StockOrder AS 'Order'
FROM temp.ods_stock_order;
