-- 04 - Calculate Order TEST1.sql


-- ************************************
-- temp.ods_stock_ori 
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
INNER JOIN (SELECT 0 AS StoreId) AS s
	ON 1=1
INNER JOIN (SELECT 1 AS ProductId) AS p
	ON 1=1	
LEFT JOIN (
	-- Projections TEST
	SELECT 0 AS StoreId, 1 AS ProductId,  4 AS Week, 5  AS Projection union all 
	SELECT 0 AS StoreId, 1 AS ProductId,  5 AS Week, 2  AS Projection union all 
	SELECT 0 AS StoreId, 1 AS ProductId,  6 AS Week, 12 AS Projection union all 
	SELECT 0 AS StoreId, 1 AS ProductId,  7 AS Week, 28 AS Projection union all 
	SELECT 0 AS StoreId, 1 AS ProductId,  8 AS Week, 80 AS Projection union all 
	SELECT 0 AS StoreId, 1 AS ProductId,  9 AS Week, 50 AS Projection union all 
	SELECT 0 AS StoreId, 1 AS ProductId, 12 AS Week, 35 AS Projection  
) AS j
	ON d.Week = j.Week 
	AND s.StoreId = j.StoreId 
	AND p.ProductId = j.ProductId
LEFT JOIN (
	-- Receptions
	SELECT 0 AS StoreId, 1 AS ProductId, 10 AS Week, 100 AS Reception
) AS r
	ON d.Week = r.Week 
	AND s.StoreId = r.StoreId 
	AND p.ProductId = r.ProductId
LEFT JOIN (
	-- Stock
	SELECT 0 AS StoreId, 1 AS ProductId, 150 AS Stock
) AS k
	ON s.StoreId = k.StoreId AND p.ProductId = k.ProductId	
WHERE d.Week < 13
ORDER BY d.Week, s.StoreId;

SELECT * FROM temp.ods_stock_ori ORDER BY Week;
SELECT count(*) FROM temp.ods_stock_ori;


-- ************************************
-- temp.ods_stock_out 
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

SELECT * FROM temp.ods_stock_out ORDER BY Week;
SELECT count(*) FROM temp.ods_stock_out;


-- ************************************
-- temp.ods_stock_order 
-- ************************************
--DROP TABLE IF EXISTS temp.ods_stock_order;
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
--INTO temp.ods_stock_order
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

