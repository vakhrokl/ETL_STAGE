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
PRINT N'Altering [ETL].[getTasks]...';


GO
ALTER PROCEDURE [ETL].[getTasks]
	@DataSourceId int = 0
AS
BEGIN
	SELECT 
		T.ID AS TASK_ID,
		T.[NAME] AS TASK_NAME,
		T.[SCHEMA] AS TASK_SCHEMA,
		T.TARGET_NAME,
		T.SOURCE_ID,
		T.DATA_QUERY,
		S.CONNETION
	FROM ETL.TASKS T
	INNER JOIN ETL.DATASOURCE S ON T.SOURCE_ID = S.ID
	WHERE T.SOURCE_ID = @DataSourceId OR @DataSourceId = 0
END
GO
PRINT N'Update complete.';


GO
