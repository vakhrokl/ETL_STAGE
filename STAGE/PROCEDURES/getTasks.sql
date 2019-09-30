﻿CREATE PROCEDURE [ETL].[getTasks]
	@DataSourceId int = 0
AS
BEGIN
	SELECT 
		T.ID AS TASK_ID,
		T.[NAME] AS TASK_NAME,
		T.SOURCE_ID,
		T.DATA_QUERY,
		S.CONNECTION AS SOURCE_CONNECTION,
		S.[NAME] AS SOURCE_NAME,
		T.[TARGET_TABLE],
		D.CONNECTION AS TARGET_CONNECTION,
		D.[NAME] AS TARGET_NAME
	FROM ETL.TASKS T
	INNER JOIN ETL.DATASOURCE S ON T.SOURCE_ID = S.ID
	INNER JOIN ETL.DATASOURCE D ON T.TARGET_ID = D.ID
	WHERE T.SOURCE_ID = @DataSourceId OR @DataSourceId = 0
END
