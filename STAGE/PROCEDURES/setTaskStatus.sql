CREATE PROCEDURE [ETL].[setTaskStatus]
	@TaskId int = 0,
	@Status bit = 1
AS
BEGIN
	UPDATE ETL.TASKS
	SET [STATUS] = @Status
	WHERE ID = @TaskId
END

