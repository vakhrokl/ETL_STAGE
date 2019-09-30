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
/*
The column [ETL].[TASKS].[SCHEMA] on table [ETL].[TASKS] must be added, but the column has no default value and does not allow NULL values. If the table contains data, the ALTER script will not work. To avoid this issue you must either: add a default value to the column, mark it as allowing NULL values, or enable the generation of smart-defaults as a deployment option.
*/

IF EXISTS (select top 1 1 from [ETL].[TASKS])
    RAISERROR (N'Rows were detected. The schema update is terminating because data loss might occur.', 16, 127) WITH NOWAIT

GO
PRINT N'Dropping unnamed constraint on [ETL].[TASKS]...';


GO
ALTER TABLE [ETL].[TASKS] DROP CONSTRAINT [FK__TASKS__SOURCE_ID__2D27B809];


GO
PRINT N'Starting rebuilding table [ETL].[TASKS]...';


GO
BEGIN TRANSACTION;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

SET XACT_ABORT ON;

CREATE TABLE [ETL].[tmp_ms_xx_TASKS] (
    [ID]          INT             IDENTITY (1, 1) NOT NULL,
    [NAME]        NVARCHAR (50)   NOT NULL,
    [SCHEMA]      NVARCHAR (10)   NOT NULL,
    [SOURCE_ID]   INT             NOT NULL,
    [DATA_QUERY]  NVARCHAR (4000) NOT NULL,
    [TARGET_NAME] NVARCHAR (50)   NULL,
    PRIMARY KEY CLUSTERED ([ID] ASC)
);

IF EXISTS (SELECT TOP 1 1 
           FROM   [ETL].[TASKS])
    BEGIN
        SET IDENTITY_INSERT [ETL].[tmp_ms_xx_TASKS] ON;
        INSERT INTO [ETL].[tmp_ms_xx_TASKS] ([ID], [NAME], [SOURCE_ID], [DATA_QUERY], [TARGET_NAME])
        SELECT   [ID],
                 [NAME],
                 [SOURCE_ID],
                 [DATA_QUERY],
                 [TARGET_NAME]
        FROM     [ETL].[TASKS]
        ORDER BY [ID] ASC;
        SET IDENTITY_INSERT [ETL].[tmp_ms_xx_TASKS] OFF;
    END

DROP TABLE [ETL].[TASKS];

EXECUTE sp_rename N'[ETL].[tmp_ms_xx_TASKS]', N'TASKS';

COMMIT TRANSACTION;

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;


GO
PRINT N'Creating unnamed constraint on [ETL].[TASKS]...';


GO
ALTER TABLE [ETL].[TASKS] WITH NOCHECK
    ADD FOREIGN KEY ([SOURCE_ID]) REFERENCES [ETL].[DATASOURCE] ([ID]);


GO
PRINT N'Altering [ETL].[addTask]...';


GO
ALTER PROCEDURE [ETL].[addTask]
	@DataSourceId int,
	@TaskName nvarchar(50),
	@SchemaName nvarchar(10),
	@DataExtractingQuery nvarchar(4000)
AS
BEGIN
	IF(NOT EXISTS(SELECT * FROM ETL.TASKS 
								WHERE NAME = @TaskName 
								AND [SCHEMA] = @SchemaName 
								AND SOURCE_ID = @DataSourceId)
	)
	BEGIN
		INSERT INTO ETL.TASKS( SOURCE_ID, NAME, TARGET_NAME, [SCHEMA], DATA_QUERY)
		VALUES(
			@DataSourceId,
			REPLACE(REPLACE(@TaskName,'[',''),']',''),
			REPLACE(REPLACE(@SchemaName,'[',''),']',''),
			'S'+RIGHT('000'+LTRIM(STR(@DataSourceId)),3)+'_'+@TaskName,
			@DataExtractingQuery
			)
	END	
	ELSE PRINT 'THIS SOURCE IS ALREADY EXISTS';

	SELECT * FROM ETL.TASKS WHERE NAME = @TaskName AND SOURCE_ID = @DataSourceId;
END
GO
PRINT N'Refreshing [ETL].[getTasks]...';


GO
EXECUTE sp_refreshsqlmodule N'[ETL].[getTasks]';


GO
PRINT N'Checking existing data against newly created constraints';


GO
USE [$(DatabaseName)];


GO
CREATE TABLE [#__checkStatus] (
    id           INT            IDENTITY (1, 1) PRIMARY KEY CLUSTERED,
    [Schema]     NVARCHAR (256),
    [Table]      NVARCHAR (256),
    [Constraint] NVARCHAR (256)
);

SET NOCOUNT ON;

DECLARE tableconstraintnames CURSOR LOCAL FORWARD_ONLY
    FOR SELECT SCHEMA_NAME([schema_id]),
               OBJECT_NAME([parent_object_id]),
               [name],
               0
        FROM   [sys].[objects]
        WHERE  [parent_object_id] IN (OBJECT_ID(N'ETL.TASKS'))
               AND [type] IN (N'F', N'C')
                   AND [object_id] IN (SELECT [object_id]
                                       FROM   [sys].[check_constraints]
                                       WHERE  [is_not_trusted] <> 0
                                              AND [is_disabled] = 0
                                       UNION
                                       SELECT [object_id]
                                       FROM   [sys].[foreign_keys]
                                       WHERE  [is_not_trusted] <> 0
                                              AND [is_disabled] = 0);

DECLARE @schemaname AS NVARCHAR (256);

DECLARE @tablename AS NVARCHAR (256);

DECLARE @checkname AS NVARCHAR (256);

DECLARE @is_not_trusted AS INT;

DECLARE @statement AS NVARCHAR (1024);

BEGIN TRY
    OPEN tableconstraintnames;
    FETCH tableconstraintnames INTO @schemaname, @tablename, @checkname, @is_not_trusted;
    WHILE @@fetch_status = 0
        BEGIN
            PRINT N'Checking constraint: ' + @checkname + N' [' + @schemaname + N'].[' + @tablename + N']';
            SET @statement = N'ALTER TABLE [' + @schemaname + N'].[' + @tablename + N'] WITH ' + CASE @is_not_trusted WHEN 0 THEN N'CHECK' ELSE N'NOCHECK' END + N' CHECK CONSTRAINT [' + @checkname + N']';
            BEGIN TRY
                EXECUTE [sp_executesql] @statement;
            END TRY
            BEGIN CATCH
                INSERT  [#__checkStatus] ([Schema], [Table], [Constraint])
                VALUES                  (@schemaname, @tablename, @checkname);
            END CATCH
            FETCH tableconstraintnames INTO @schemaname, @tablename, @checkname, @is_not_trusted;
        END
END TRY
BEGIN CATCH
    PRINT ERROR_MESSAGE();
END CATCH

IF CURSOR_STATUS(N'LOCAL', N'tableconstraintnames') >= 0
    CLOSE tableconstraintnames;

IF CURSOR_STATUS(N'LOCAL', N'tableconstraintnames') = -1
    DEALLOCATE tableconstraintnames;

SELECT N'Constraint verification failed:' + [Schema] + N'.' + [Table] + N',' + [Constraint]
FROM   [#__checkStatus];

IF @@ROWCOUNT > 0
    BEGIN
        DROP TABLE [#__checkStatus];
        RAISERROR (N'An error occurred while verifying constraints', 16, 127);
    END

SET NOCOUNT OFF;

DROP TABLE [#__checkStatus];


GO
PRINT N'Update complete.';


GO
