

USE GW2CAT
GO

CREATE PROC spApiJsonSqlLoad
AS
BEGIN

	/***** CHANGE LOG *****/
	/***** Jan 10, 2020 21:00 CET by A.F. - Created SP As Below *****/

	/***** SECTION 1 - JOB AND FILE PREPARATION *****/

	/* 1.1 - CREATE THE JOB */
	/* Generate a new job id and pass it to the foreign key in the job file tracking table */
	DECLARE @tmp1 TABLE(id int)
	INSERT INTO job_import_log (job_dts_start)
	OUTPUT INSERTED.job_tid INTO @tmp1 (id)
	VALUES (getutcdate())
	DECLARE @job_tid int
	SET @job_tid = (SELECT * FROM @tmp1) -- to be used later


	/* 1.2 - GET THE FILES TO PROCESS */
	/* Use the builtin sp file trawler to fetch files into a temp table and then insert them into the job file tracking table */
	DECLARE @json_file_scan TABLE (
		job_tid int,
		tmp_filename nvarchar(128),
		tmp_job_file_processed int,
		unused_col int
	)
	 /* Note 1: If files are not moved after processing, the files will be added cumulatively to the job file table - undesirable */
	 /* Best would be to create an IF/ELSE based check for more flexibility */
	 /* Note 2: Parameterize the folder string at some point for more flexibility */
	INSERT INTO @json_file_scan (tmp_filename, tmp_job_file_processed, unused_col)
	EXEC xp_dirtree 'C:\gw2ct\json_get', 2, 1 --note parametrize the folder this later
	UPDATE @json_file_scan SET job_tid = @job_tid
	UPDATE @json_file_scan SET tmp_job_file_processed = 0 --reverse the initial 1 written in by the system stored procedure
	--select * from @json_file_scan

	INSERT INTO job_file_log (job_tid, job_file_name, job_file_processed)
	SELECT job_tid, tmp_filename, tmp_job_file_processed
	FROM @json_file_scan
	--select* from job_file_log 

	/* 1.3 COUNT THE NUMBER OF FILES TO PROCESS AND LOG IT */
	DECLARE @num_rows int
	SET @num_rows = (SELECT COUNT(*) FROM job_file_log WHERE job_file_processed = 0)
	PRINT('The number of json files to process is: ' + CAST(@num_rows as nvarchar(6)))
	--INSERT INTO job_import_log(num_to_process)
	UPDATE job_import_log SET num_to_process = @num_rows


	/***** SECTION 2 - PARSE THE JSON AND LOG THE DATA TO THE CORRESPONDING BUY AND SELL TABLES *****/

	/* 2.1 LOOP THROUGH THE FILE LIST */
	DECLARE @row_counter int = 0
	WHILE @row_counter < @num_rows
	BEGIN
		DECLARE @file_to_parse nvarchar(128) 
		SET @file_to_parse = (SELECT TOP 1 job_file_name FROM job_file_log WHERE job_file_processed = 0)
		PRINT('Parsing file: ' + @file_to_parse)
	   
		/* 2.2 GET THE GW2 ID AND SELL VALUES */
		DECLARE	@sql_cmd_id_buys nvarchar(max)
		SET		@sql_cmd_id_buys ='SELECT id as gw2_id, job_tid = '+CAST(@job_tid AS varchar(20))+', quantity as buy_quantity, unit_price as buy_price 
				FROM OPENROWSET (BULK ''C:\gw2ct\json_get\' + @file_to_parse + ''', SINGLE_CLOB) as js 
				CROSS APPLY OPENJSON(BulkColumn)
				WITH 
				(
					id int,
					job_tid int,
					buys nvarchar(max) as JSON
				) as id
				CROSS APPLY OPENJSON(id.buys)
				WITH 
				(
					quantity int, 
					unit_price int
				) as buys'

		/* 2.3 GET THE SELL VALUES */
		DECLARE	@sql_cmd_sells nvarchar(max)
		SET		@sql_cmd_sells = 'SELECT id as gw2_id, job_tid = '+CAST(@job_tid AS varchar(20))+', quantity as sell_quantity, unit_price as sell_price 
				FROM OPENROWSET (BULK ''C:\gw2ct\json_get\' + @file_to_parse + ''', SINGLE_CLOB) as js 
				CROSS APPLY OPENJSON(BulkColumn)
				WITH 
				(
					id int,
					job_tid int,
					sells nvarchar(max) as JSON
				) as id
				CROSS APPLY OPENJSON(id.sells)
				WITH 
				(
					quantity int, 
					unit_price int
				) as sells'

		/* 2.4 INSERT THE VALUES INTO THE BUY AND SELL TABLES */
		INSERT INTO tp_buys (gw2_id, job_tid, buy_quantity, buy_price)
		EXEC sp_executesql @sql_cmd_id_buys
		--UPDATE tp_buys SET job_tid = @job_tid

		INSERT INTO tp_sells (gw2_id, job_tid, sell_quantity, sell_price)
		EXEC sp_executesql @sql_cmd_sells
		--UPDATE tp_buys SET job_tid = @job_tid
	
		SET @row_counter = @row_counter + 1

		/* 2.5 SET FLAG TO INDICATE THE FILE HAS BEEN PROCESSED */
		UPDATE job_file_log	
		SET job_file_processed = 1
		WHERE job_file_name=@file_to_parse
	END

	/***** SECTION 3 - FINALIZE THE JOB *****/
	/* 3.1 UPDATE THE JOB LOG WITH THE FINAL PROCESSED FILE COUNT AND FINISHING DTS */
	PRINT('The run has completed. Writing to log file')
	UPDATE job_import_log SET num_completed = @row_counter, job_dts_finished = GETUTCDATE() WHERE job_tid = @job_tid


	/* 3.2 CALCULATE THE JOB RUN TIME */
	DECLARE @EndTime AS DATETIME, @StartTime AS DATETIME
	SET @StartTime = (SELECT job_dts_start FROM job_import_log WHERE job_tid = @job_tid)
	SET @EndTime = (SELECT job_dts_finished FROM job_import_log  WHERE job_tid = @job_tid)
	UPDATE job_import_log SET job_time = (CAST(@EndTime - @StartTime AS TIME)) WHERE job_tid = @job_tid

END