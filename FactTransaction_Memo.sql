--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
USE [GloryStaging]
GO

/ * * * * * *

Object: StoredProcedure [dbo].[GloryStaging_e_to_FactTransac
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GC

I

-- Author:
-- Create date: 10/01/2018
-- Description: Load Memos from CDC staging tables ().
:=

ALTER PROCEDURE [dbo].[GloryStaging_e_to_FactTransaction_Memo] AS Rich Fox

BEGIN
	SET NOCOUNT ON;
	SET DEADLOCK_PRIORITY HIGH;

	DECLARE @from_lsn AS BINARY (10);
	DECLARE @to_lsn AS BINARY (10) =
	SELECT [to_lsn]
	FROM GloryStaging.[dbo].[ETL_Control_Table]
	WHERE [TableName] = 'eVAS_Transactions'
	DECLARE @Row_filter_option AS NVARCHAR(30) = N'all';
	-- Set to the source table name
	DECLARE @TableName AS VARCHAR(20) = 'eVAS_Memo';);

	-- Looking for the last update of the table
	SET @from_1sn = (
			SELECT MAX(to_1sn)
			FROM GloryStaging.dbo.etl_control_table
			WHERE tablename = @TableName
			) :

	-- Checking if the last update is blank, if so setting it to the oldest date in the capture tables.
	IF @from_lsn IS NULL
	BEGIN
		SET @from_lsn = sys.fn_cdc_get_min_lsn(@TableName);

		SELECT memo
			,cast(trace AS BIGINT) AS trace
			,[exportable]
		INTO #temp
		FROM [GloryStaging].[cdc].[fn_cdc_get_all_changes_eVAS_Memo](@from_lsn, @to_lsn, @Row_filter_option);

		UPDATE ft
		SET ft.memo = m.memo
		FROM cms_dw.dbo.facttransaction ft
		INNER JOIN #temp m ON ft.application_instance_id = 999
			AND ft.traceno = m.trace
		WHERE m.[exportable] = 1;
	END;

	UPDATE ft
	SET ft.memo = m.memo
	FROM cms_dw.dbo.facttransaction ft
	INNER JOIN #temp m ON ft.application_instance_id = 999
		AND ft.TraceNo_MBD = CAST(m.trace AS BIGINT)
	WHERE m.[exportable] = 1;

	BEGIN
		DELETE
		FROM GloryStaging.dbo.etl_control_table
		WHERE TableName = @TableName;

		INSERT INTO GloryStaging.dbo.etl_control_table (
			from_lsn
			,to_lsn
			,TableName VALUES (
				@from_lsn
				,@to_lsn
				,@TableName
				);
			)

		-- END
		I
	END
