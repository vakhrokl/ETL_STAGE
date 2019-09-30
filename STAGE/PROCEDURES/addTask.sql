CREATE PROCEDURE [ETL].[addTask]
	@DataSourceId int,
	@TaskName nvarchar(50),
	@DataTargetId int,
	@TargetName nvarchar(50),
	@DataExtractingQuery nvarchar(4000)
AS
BEGIN
	IF(NOT EXISTS(SELECT * FROM ETL.TASKS 
								WHERE NAME = @TaskName 
								AND SOURCE_ID = @DataSourceId
								AND TARGET_ID = @DataTargetId)
	)
	BEGIN
		INSERT INTO ETL.TASKS( SOURCE_ID, NAME, TARGET_ID, [TARGET_TABLE],  DATA_QUERY)
		VALUES(
			@DataSourceId,
			@TaskName,
			@DataTargetId,
			@TargetName,
			@DataExtractingQuery
			)
		SELECT * FROM ETL.TASKS WHERE ID  = @@IDENTITY
	END	
	ELSE 
	BEGIN
		PRINT 'THIS TASK IS ALREADY EXISTS';
		SELECT * FROM ETL.TASKS WHERE NAME = @TaskName AND SOURCE_ID = @DataSourceId AND TARGET_ID = @DataTargetId;
	END
END
