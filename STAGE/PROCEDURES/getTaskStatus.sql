CREATE PROCEDURE [ETL].[getTaskStatus]
	@TaskId int 
AS
	select [STATUS]
	from ETL.TASKS
	where ID=@TaskId
RETURN 0
