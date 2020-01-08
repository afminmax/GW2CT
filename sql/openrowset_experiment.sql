/* Test openrowset - alt1: works! */

CREATE TABLE #json_bulk_temp1
(
	BulkColumn nvarchar(max),
	json_key int,
	value nvarchar(max),
	type int
)
INSERT INTO #json_bulk_temp1
SELECT *
FROM OPENROWSET (BULK 'C:\gw2ct\testsql\run_2020-01-08 19.27.39_Ingots_prices.json', SINGLE_CLOB) as js
CROSS APPLY OPENJSON(BulkColumn)

SELECT * FROM  #json_bulk_temp1

--DROP TABLE #json_bulk_temp1


/* Test openrowset - alt2: only bulk column, works too!*/
CREATE TABLE #json_bulk_temp2
(
	BulkColumn nvarchar(max),
)
INSERT INTO #json_bulk_temp2
SELECT BulkColumn
FROM OPENROWSET (BULK 'C:\gw2ct\testsql\run_2020-01-08 19.27.39_Ingots_prices.json', SINGLE_CLOB) as js
CROSS APPLY OPENJSON(BulkColumn) -- can also be removed for a single row json dump

SELECT * FROM  #json_bulk_temp2

--DROP TABLE #json_bulk_temp2


/* Test openrowset - alt3: dynamic sql only bulk column, works too!*/
DECLARE @file_to_parse nvarchar(256) = 'run_2020-01-08 19.27.39_Ingots_prices.json' -- this will come from while loop
DECLARE	@select_statement nvarchar(max)
SET		@select_statement ='SELECT value FROM OPENROWSET (BULK ''C:\gw2ct\testsql\' + @file_to_parse + ''', SINGLE_CLOB) as js CROSS APPLY OPENJSON(BulkColumn)'
PRINT(@select_statement)

CREATE TABLE #json_bulk_temp3
(
	value nvarchar(max),
)
INSERT INTO #json_bulk_temp3
EXECUTE(@select_statement)


SELECT * FROM  #json_bulk_temp3

DROP TABLE #json_bulk_temp3
