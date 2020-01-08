
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

/* GET THE FILECOUNT NAME OF THE FIRST FILE */
DECLARE @num_rows int
DECLARE @row_counter int = 1

SET @num_rows = (SELECT COUNT(*) FROM #json_files_temp)
PRINT('The number of json files to process is: ' + CAST(@num_rows as nvarchar(6)))

/* LOOP THROUGH THE FILE LIST */

WHILE @row_counter <= @num_rows
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

	/*
	above gives a variable name to the json extract query
	DO OUR STUFF HERE
	*/ 


	SET @row_counter = @row_counter + 1
END

--SELECT * FROM #json_files_temp

--DROP TABLE #json_files_temp