DECLARE @json_file_name nvarchar(250);
DECLARE @fucking_openrowset nvarchar(500);
DECLARE @fucking_top_select nvarchar(500);
DECLARE @fucking_bottom_select nvarchar(500);

SET @json_file_name = '''C:\gw2ct\testsql\run_2020-01-07 14.51.12_Ingots_prices.json''';
SET @fucking_openrowset = 'FROM OPENROWSET (BULK ' + @json_file_name + ', SINGLE_CLOB) as js' 
SET @fucking_top_select = 'SELECT id as gw2_id, quantity as buy_quantity, unit_price as buy_unit_price ' + @fucking_openrowset
SET @fucking_bottom_select = 'SELECT id as gw2_id, quantity as sell_quantity, unit_price as sell_unit_price' + @fucking_openrowset

PRINT(@json_file_name)
PRINT(@fucking_openrowset)
PRINT(@fucking_top_select)
PRINT(@fucking_bottom_select)

SELECT t1.gw2_id, buy_unit_price, sell_quantity, sell_unit_price
	--INTO temp_json
FROM
(
	--SELECT id as gw2_id, quantity as buy_quantity, unit_price as buy_unit_price
	--FROM OPENROWSET (BULK 'C:\gw2ct\testsql\run_2020-01-07 14.51.12_Ingots_prices.json', SINGLE_CLOB) as js
	@fucking_top_select
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
	--SELECT id as gw2_id, quantity as sell_quantity, unit_price as sell_unit_price
	--FROM OPENROWSET (BULK 'C:\gw2ct\testsql\run_2020-01-07 14.51.12_Ingots_prices.json', SINGLE_CLOB) as js
	@fucking_bottom_select
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