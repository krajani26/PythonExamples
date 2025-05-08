USE [GloryStaging]
GO

--* * * * * * Object: StoredProcedure [dbo].[GloryStaging_e_to_FactTransaction_MEDIADB] Script Date: 3 / 25 / 2025 8 : 07 : 40 PM * * * * * *

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--========================================================
-- Author: Rich Fox
-- Create date: 10/01/2018
-- Description: Load DB Media from CDC staging tables ().
--========================================================
ALTER PROCEDURE [dbo].[GloryStaging_e_to_FactTransaction_MEDIADB]
AS
BEGIN
	SET NOCOUNT ON;
	SET DEADLOCK_PRIORITY HIGH;

	DECLARE @from_lsn AS BINARY (10);
	DECLARE @to_lsn AS BINARY (10) =
	SELECT [to_lsn]
	FROM GloryStaging.[dbo].[ETL_Control_Table]
	WHERE [TableName] = 'eVAS_Transactions' );
	DECLARE @Row_filter_option AS NVARCHAR(30) = N'all';
	-- Set to the source table name
	DECLARE @TableName AS VARCHAR(20) = 'eVAS_MEDIADB';

	-- Looking for the last update of the table
	SET @from_lsn = (
			SELECT MAX(to_lsn)
			FROM GloryStaging.dbo.etl_control_table
			WHERE tablename = @TableName
			);

	-- Checking if the last update is blank, if so setting it to the oldest date in the capture tables.
	IF @from_lsn IS NULL
	BEGIN
		SET @from_lsn = sys.fn_cdc_get_min_lsn(@TableName);
	END;

	UPDATE ft
	SET [MediaDB_Curr] = ISNULL([DBCURR] / 100, 0)
		,[MediaDB_Coin] = ISNULL([DBCOIN] / 100, 0)
		,[MediaDB_Check] = ISNULL([DBCHECK] / 100, 0)
		,[MediaDB_FS] = ISNULL([DBFS] / 100, 0)
		,[MediaDB_Misc] = ISNULL([DBMISC] / 100, 0)
	FROM cms_dw.dbo.facttransaction ft
	INNER JOIN [GloryStaging].[cdc].[fn_cdc_get_all_changes_eVAS_MEDIADB](@from_lsn, @to_lsn, @Row_filter_option) m ON ft.application_instance_id = 999
		AND ft.traceno = cast(m.traceno AS BIGINT)
	WHERE m.version = 0;

	BEGIN
		DELETE
		FROM GloryStaging.dbo.etl_control_table
		WHERE TableName = @TableName;

		INSERT INTO GloryStaging.dbo.etl_control_table (
			from_lsn
			,to_lsn
			,TableName
			)
		VALUES (
			@from_lsn
			,@to_lsn
			,@TableName
			)
	END;
