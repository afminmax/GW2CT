/* PARSE AND RETRIEVE UTC ISO8601 TIMESTAMP */
DECLARE @dts datetimeoffset(6)
SET @dts =
(
	SELECT dts
	FROM OPENROWSET (BULK 'C:\gw2ct\testsql\run_2020-01-08 19.27.39_Ingots_prices.json', SINGLE_CLOB) as js
		CROSS APPLY OPENJSON(BulkColumn)
		WITH 
		(
			dts datetimeoffset
		) as id
		WHERE dts is not NULL
)


/* PARSE JSON */
SELECT t1.gw2_id, buy_quantity, buy_price, sell_quantity, sell_price, @dts as dts
	--INTO temp_json
FROM
(
	SELECT id as gw2_id, quantity as buy_quantity, unit_price as buy_price

	FROM OPENROWSET (BULK 'C:\gw2ct\testsql\run_2020-01-08 19.27.39_Ingots_prices.json', SINGLE_CLOB) as js
	CROSS APPLY OPENJSON(BulkColumn)
	WITH 
	(
		id int, 
		buys nvarchar(max) as JSON,
		sells nvarchar(max) as JSON
	) as id

	CROSS APPLY OPENJSON(id.buys)
	WITH 
	(
	quantity int, 
	unit_price int
	) as buys
) t1

INNER JOIN

(
	SELECT id as gw2_id, quantity as sell_quantity, unit_price as sell_price
	FROM OPENROWSET (BULK 'C:\gw2ct\testsql\run_2020-01-08 19.27.39_Ingots_prices.json', SINGLE_CLOB) as js
	CROSS APPLY OPENJSON(BulkColumn)
	WITH 
	(
		id int,
		sells nvarchar(max) as JSON
	) as id
	CROSS APPLY OPENJSON(id.sells)
	WITH 
	(
	quantity int, 
	unit_price int
	) as sells
) t2

ON t1.gw2_id = t2.gw2_id
