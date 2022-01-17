# PruebaIndiretail_order_Ernesto
Data Engineer technical assessment (order to warehouse)

## Description
We generate the Order results columns using SQL statements and auxiliary tables in the Vertica columnar database.

## Files
- **/sql/01 - Master tables.sql** Master tables generation: 
	- Load Staging tables:
		- `temp.parq_actual_stock`
		- `temp.parq_receptions`
		- `temp.parq_projection`
	- Load Dimension tables:
		- `temp.dim_store`
		- `temp.dim_product`
		- `temp.dim_date`
- **/sql/02 - Calculate Order.sql** Process SQL to calculate columns *AvailableStock*, *StockNeed*, *Order*.
	1. Join main tables data in `temp.ods_stock_ori`.
	2. Calculate temp column with out of stock data in `temp.ods_stock_out`.
	3. Calculate final columns results in `temp.ods_stock_order`.
-  **/sql/03 - Export order_to_warehouse.parquet.sql** Export from `temp.ods_stock_order` to parquet file result `order_to_warehouse.parquet`.
-  **/sql/04 - Calculate Order TEST1.sql** Test example 1.
- **/data** Data output

## Enviroment
- Host
	- Intel I5
	- Windows 10
- Virtual Machine
	- 6 nucleos CPU
	- 12 GB RAM
	- Centos 7
	- Vertica Community Edition 11.0.2

## Execution results

| Process | Average execution time|
| ------ | ------ |
| Create Master tables SQL| **250ms** |
| Calculate columns results| **2m 5s** |
| Export order_to_warehouse.parquet.sql | **2m 10s** |
