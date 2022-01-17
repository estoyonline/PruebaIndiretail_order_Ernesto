-- 02 - Calculate Order.sql


-- ************************************
-- temp.ods_stock_ori - 16.549.992 - 32s
-- ************************************
DROP TABLE IF EXISTS temp.ods_stock_ori;
SELECT 
	d.Week,
	s.StoreId,
	p.ProductId,
	COALESCE(j.PROJECTION, 0) AS Projection,
	COALESCE(r.Reception, 0) AS Reception,
	COALESCE(k.Stock, 0) AS StockOri
INTO temp.ods_stock_ori
FROM (
	SELECT DISTINCT d_week_of_year AS Week
	FROM temp.dim_date AS d
) AS d
INNER JOIN temp.dim_store AS s
	ON 1=1
INNER JOIN temp.dim_product AS p
	ON 1=1	
LEFT JOIN temp.parq_projection AS j
	ON d.Week = j.Week 
	AND s.StoreId = j.StoreId 
	AND p.ProductId = j.ProductId
LEFT JOIN temp.parq_receptions AS r
	ON d.Week = r.Week 
	AND s.StoreId = r.StoreId 
	AND p.ProductId = r.ProductId
LEFT JOIN (
	SELECT StoreId, ProductId, Stock
	FROM temp.parq_actual_stock AS k
) AS k
	ON s.StoreId = k.StoreId AND p.ProductId = k.ProductId	
--WHERE s.StoreId = 2 AND p.ProductId = 14631 --AND d.Week < 13
ORDER BY d.Week, s.StoreId;


-- ************************************
-- temp.ods_stock_out - 16.549.992 - 1m 15s
-- ************************************
DROP TABLE temp.ods_stock_out;
SELECT 
	t.Week,
	t.StoreId,
	t.ProductId,
	t.Projection,
	t.Reception,
	t.ProjectionAgg,
	t.ReceptionAgg,
	t.StockAgg,
	t.StockT1,
	t.StockOut,
	sum(sum(t.StockOut)) OVER (ORDER BY t.Week) AS StockOutAgg,
	t.StockT1 + sum(sum(t.StockOut)) OVER (ORDER BY t.Week) AS Stock
INTO temp.ods_stock_out
FROM (
	SELECT 
		a.Week, 
		a.StoreId,
		a.ProductId,
		a.StockOri, 
		a.Projection, 
		a.Reception, 
		sum(b.Projection) ProjectionAgg, 
		sum(b.Reception) ReceptionAgg, 
		sum(b.Reception) + a.StockOri AS StockAgg,
		(sum(b.Reception) + a.StockOri) - sum(b.Projection) AS StockT1,
		CASE
			WHEN ((sum(b.Reception) + a.StockOri) - sum(b.Projection)) < 0 THEN 
				abs((sum(b.Reception) + a.StockOri) - sum(b.Projection))
			ELSE 0
		END AS StockOut
	FROM temp.ods_stock_ori AS a
	LEFT JOIN temp.ods_stock_ori AS b
		ON a.Week >= b.Week
	GROUP BY 
		a.Week, 
		a.StoreId,
		a.ProductId,
		a.StockOri, 
		a.Projection,
		a.Reception 
) AS t
GROUP BY 
	t.Week,
	t.StoreId,
	t.ProductId,
	t.Projection,
	t.Reception,
	t.ProjectionAgg,
	t.ReceptionAgg,
	t.StockAgg,
	t.StockT1,
	t.StockOut
ORDER BY t.Week;


-- ************************************
-- temp.ods_stock_order - 16.549.992 - 18s
-- ************************************
DROP TABLE IF EXISTS temp.ods_stock_order;
SELECT 
	t.Week,
	t.StoreId,
	t.ProductId,
	t.Projection,
	t.Reception,
	t.Stock,
	CASE 
		WHEN (t.Projection - t.Stock) > 0 THEN t.Projection - t.Stock
		ELSE 0
	END AS StockNeed,
	COALESCE(o.StockOrder, 0) AS StockOrder
INTO temp.ods_stock_order
FROM temp.ods_stock_out AS t
LEFT JOIN (
	SELECT 
		t.Week - 1 AS WeekOrder,
		t.StoreId,
		t.ProductId,
		CASE 
			WHEN (t.Projection - t.Stock) > 0 THEN t.Projection - t.Stock
			ELSE 0
		END AS StockOrder
	FROM temp.ods_stock_out AS t
) AS o
	ON t.Week = o.WeekOrder
	AND t.StoreId = o.StoreId
	AND t.ProductId = o.ProductId;


-- LOG
SELECT 'temp.ods_stock_ori' AS tabla, count(*) AS registros FROM temp.ods_stock_ori UNION ALL 
SELECT 'temp.ods_stock_out' AS tabla, count(*) AS registros FROM temp.ods_stock_out UNION ALL 
SELECT 'temp.ods_stock_order' AS tabla, count(*) AS registros FROM temp.ods_stock_order;

