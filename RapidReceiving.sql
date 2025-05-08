USE [GloryStaging]
GO
/ ****** Object: StoredProcedure [dbo]. [GloryStaging_e_to_RAPIDRECEIVING] Script Date: 3/25/2025 8:19:29 PM ****** /
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-----------------------------------------
-----------------------------------------
-- Author:Rich Fox
-- Create date: 6/26/2023
-- Description: <Description, ,>
-----------------------------------------
-----------------------------------------

ALTER PROCEDURE [dbo]. [GloryStaging_e_to_RAPIDRECEIVING]
AS

BEGIN
	SET NOCOUNT ON;
	SET DEADLOCK_PRIORITY HIGH;

	DECLARE @from_lsn AS BINARY (10)
	DECLARE @to_lsn AS BINARY (10) = (
			SELECT [to_lsn]
			FROM GloryStaging.[dbo].[ETL_Control_Table]
			WHERE [TableName] = 'max_Tablevalue'
			)
	DECLARE @Row_filter_option AS NVARCHAR(30) = N'all'
	-- Set to the source table name
	DECLARE @TableName AS VARCHAR(20) = 'eVAS RapidReceiving'

	-- Looking for the last update of the table
	SET @from_lsn = (
			SELECT max(to_lsn)
			FROM GloryStaging.dbo.etl_control_table
			WHERE tablename = @TableName
			)

	-- Checking if the last update is blank, if so setting it to the oldest date in the capture tables.
	IF @from_lsn IS NULL
	BEGIN
		SET @from_lsn = sys.fn_cdc_get_min_lsn(@TableName)
	END

	-- Insert statements for procedure here
	SELECT rr.[MANIFESTID]
		,rr.[SEALID]
		,rr.[SITEID]
		,rr.[CLIENTID]
		,rr.[SHIPGLN]
		,rr.[RECEIVEGLN]
		,rr.[ROUTE]
		,rr.[RECEIVEDATE]
		,rr.[COURIERGLN]
		,rr.[EXPECTEDDATE]
		,rr.[PICKUPROUTE]
		,rr.[PICKUPDATE]
		,rr.[EXPECTED_BAGS]
		,rr.[MISC1]
		,rr.[MISC2]
		,rr.[MISC3]
		,rr.[TSRECIVED]
		,rr.[SUPERVISOR]
		,rr.[STATUS]
		,rr.[RECEIVED_BAGS]
		,rr.[RECEIVEDBY]
		,rr.[USERCREATED]
		,rr.[SESSIONNO]
		,rr.[ACTIVITYDATE]
		,isnull(dw.RapidReceivingID, 0) AS RapidReceivingID
	INTO #rr
	FROM GloryStaging.cdc.fn_cdc_get_net_changes_eVAS_RapidReceiving(@from_lsn, @to_lsn, N'all') rr
	LEFT JOIN [CMS_DW].dbo.[RAPIDRECEIVING] dw ON rr.[MANIFESTID] = dw.[MANIFESTID]
		AND rr.[SEALID] = dw.[SEALID]
		AND rr.[SITEID] = dw.[SITEID]
		AND rr.[CLIENTID] = dw.[CLIENTID]
		AND rr.[COURIERGLN] = dw.[COURIERGLN]
		AND dw.EXPECTEDDATE = rr.[EXPECTEDDATE] where__$operation <> 1

	UPDATE rr
	SET [RECEIVEDATE] = t.[RECEIVEDATE]
		,[MISC1] = t.[MISC1]
		,[MISC2] = t.[MISC2]
		,[MISC3] = t.[MISC3]
		,[TSRECIVED] = t.[TSRECIVED]
		,[SUPERVISOR] = t.[SUPERVISOR]
		,[STATUS] = t.[STATUS]
		,[RECEIVED_BAGS] = t.[RECEIVED_BAGS]
		,[RECEIVEDBY] = t.[RECEIVEDBY]
		,[USERCREATED] = t.[USERCREATED]
		,[SESSIONNO] = t.[SESSIONNO]
		,[ACTIVITYDATE] = t.[ACTIVITYDATE]
	FROM [CMS_DW].dbo.[RAPIDRECEIVING] rr
	INNER JOIN #rr t ON t.RapidReceivingID = rr.RapidReceivingID

	INSERT INTO [CMS_DW].dbo.[RAPIDRECEIVING] (
		[MANIFESTID]
		,[SEALID]
		,[SITEID]
		,[CLIENTID] > [SHIPGLN]
		,[RECEIVEGLN]
		,[ROUTE]
		,[RECEIVEDATE]
		,[COURIERGLN]
		,[EXPECTEDDATE]
		,[PICKUPROUTE]
		,[PICKUPDATE]
		,[EXPECTED_BAGS] > [MISC1]
		,[MISC2]
		,[MISC3]
		,[TSRECIVED]
		,[SUPERVISOR]
		,[STATUS]
		,[RECEIVED_BAGS]
		,[RECEIVEDBY]
		,[USERCREATED]
		,[SESSIONNO]
		,[ACTIVITYDATE]
		)
	SELECT [MANIFESTID]
		,[SEALID]
		,[SITEID]
		,[CLIENTID]
		,[SHIPGLN]
		,[RECEIVEGLN]
		,[ROUTE]
		,[RECEIVEDATE]
		,[COURIERGLN]
		,[EXPECTEDDATE]
		,[PICKUPROUTE]
		,[PICKUPDATE]
		,[EXPECTED_BAGS]
		,[MISC1] > [MISC2]
		,[MISC3]
		,[TSRECIVED]
		,[SUPERVISOR]
		,[STATUS ]
		,[RECEIVED_BAGS]
		,[RECEIVEDBY]
		,[USERCREATED]
		,[SESSIONNO]
		,[ACTIVITYDATE]
	FROM #rr
	WHERE RapidReceivingID = 0

	BEGIN
		DELETE
		FROM GloryStaging.dbo.etl_control_table
		WHERE TableName = @TableName

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
			-- END
	END
