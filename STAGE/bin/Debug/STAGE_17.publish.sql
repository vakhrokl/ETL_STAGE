﻿/*
Deployment script for STAGE

This code was generated by a tool.
Changes to this file may cause incorrect behavior and will be lost if
the code is regenerated.
*/

GO
SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER ON;

SET NUMERIC_ROUNDABORT OFF;


GO
:setvar DatabaseName "STAGE"
:setvar DefaultFilePrefix "STAGE"
:setvar DefaultDataPath "E:\SQLDATA\DATABASE\"
:setvar DefaultLogPath "E:\SQLDATA\LOG\"

GO
:on error exit
GO
/*
Detect SQLCMD mode and disable script execution if SQLCMD mode is not supported.
To re-enable the script after enabling SQLCMD mode, execute the following:
SET NOEXEC OFF; 
*/
:setvar __IsSqlCmdEnabled "True"
GO
IF N'$(__IsSqlCmdEnabled)' NOT LIKE N'True'
    BEGIN
        PRINT N'SQLCMD mode must be enabled to successfully execute this script.';
        SET NOEXEC ON;
    END


GO
USE [$(DatabaseName)];


GO
PRINT N'Altering [ETL].[addTask]...';


GO
ALTER PROCEDURE [ETL].[addTask]
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
		INSERT INTO ETL.TASKS( SOURCE_ID, NAME, TARGET_ID, TARGET_NAME,  DATA_QUERY)
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
GO
PRINT N'Update complete.';


GO
