
/* */
/* READ FILES INTO A TEMP TABLE */
CREATE TABLE #json_files_temp
(
	filename nvarchar(max),
	depth int,
	fileop int
)
INSERT INTO #json_files_temp
EXEC xp_dirtree 'C:\gw2ct\json_get', 2, 1

/* COUNT THE NUMBER OF FILES TO PROCESS */
DECLARE @num_rows int
SET @num_rows = (SELECT COUNT(*) FROM #json_files_temp)
PRINT('The number of json files to process is: ' + CAST(@num_rows as nvarchar(6)))

/* GET THE NAME OF THE API QUERY RUN FOR LOGGING */
DECLARE @run_name_string nvarchar(64)
DECLARE @run_name nvarchar(64)
SET @run_name_string = (SELECT TOP 1 filename FROM #json_files_temp WHERE fileop = 1)
--PRINT(@run_name_string)
SET @run_name = (SELECT LEFT(@run_name_string, LEN(@run_name_string) - CHARINDEX('_', @run_name_string) +1))
--PRINT(@run_name)

/* WRITE FIRST PART OF LOG DATA ANF GET THE IDENTITY ROW FOR LATER */
DECLARE @inserted_job_tid TABLE (inserted_job_tid int)  /* One must use a variable table for this */
DECLARE @fetched_job_tid int
INSERT INTO job_import_log(job_run_name, num_to_process)
OUTPUT INSERTED.job_tid INTO @inserted_job_tid
VALUES(@run_name, @num_rows)
SET @fetched_job_tid = (SELECT * FROM @inserted_job_tid)
PRINT(@fetched_job_tid)

/* LOOP THROUGH THE FILE LIST */
DECLARE @row_counter int = 0
WHILE @row_counter < @num_rows
BEGIN
	DECLARE @file_to_parse nvarchar(256) 
	SET @file_to_parse = (SELECT TOP 1 filename FROM #json_files_temp WHERE fileop = 1)
	PRINT(@file_to_parse)

	/* this block is for testing and can be removed later 
	we actually don't care to update fileop but its good for testing*/
	UPDATE #json_files_temp
	SET fileop = 0
	WHERE filename=@file_to_parse
	/* block above */

	/****  **************  ****/ 
	/* SET FILE TO PARSE HERE */
	--DECLARE @file_to_parse nvarchar(256) = 'run_2020-01-09 10.21.12_Ingots_prices.json'

	/* GET THE GW2 ID AND SELL VALUES */
	DECLARE	@sql_cmd_id_buys nvarchar(max)
	SET		@sql_cmd_id_buys ='SELECT id as gw2_id, quantity as buy_quantity, unit_price as buy_price FROM OPENROWSET (BULK ''C:\gw2ct\json_get\' + @file_to_parse + ''', SINGLE_CLOB) as js 
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
			) as buys'

	/* GET THE SELL VALUES */
	DECLARE	@sql_cmd_sells nvarchar(max)
	SET		@sql_cmd_sells = 'SELECT id as gw2_id, quantity as sell_quantity, unit_price as sell_price FROM OPENROWSET (BULK ''C:\gw2ct\json_get\' + @file_to_parse + ''', SINGLE_CLOB) as js 
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
			) as sells'

	/* INSERT THE VALUES INTO THE BUY AND SELL TABLES */
	INSERT INTO tp_buys_01 (gw2_id, buy_quantity, buy_price)
	EXEC sp_executesql @sql_cmd_id_buys

	INSERT INTO tp_sells_01 (gw2_id, sell_quantity, sell_price)
	EXEC sp_executesql @sql_cmd_sells
	
	/****  **************  ****/ 

	SET @row_counter = @row_counter + 1
END

PRINT('The run has completed. Writing to log file')
UPDATE job_import_log SET num_completed = @row_counter, job_dts_finished = GETUTCDATE() WHERE job_tid = @fetched_job_tid



--SELECT * FROM #json_files_temp

--DROP TABLE #json_files_temp


--UPDATE #json_files_temp
--	SET fileop = 1