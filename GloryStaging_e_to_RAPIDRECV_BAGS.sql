USE [GloryStaging]
GO

/ * * * * * * Object: StoredProcedure [dbo].[GloryStaging_e_to_RAPIDRECV_BAGS] Script Date: 3 / 25 / 2025 8 : 22 : 23 PM * * * * * * /

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--=========================================================================
-- Author:Rich Fox
-- Create date: 6/26/2023
-- Description: <Descriptipn, ,>
--=========================================================================

ALTER PROCEDURE [dbo].[GloryStaging_e_to_RAPIDRECV_BAGS]
	-- Add the parameters for the stored procedure here
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
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
	DECLARE @TableName AS VARCHAR(20) = 'eVAS_RapidRecv Bags'

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
		,rr.[COURIERGLN]
		,rr.[BAGID_SSCC]
		,rr.[LOCATIONGLN]
		,rr.[LOCATIONNAME]
		,rr.[LOCATIONADDRESS]
		,rr.[SAIDTOCONTAIN]
		,rr.[TRACE]
		,rr.[MISC1]
		,rr.[MISC2]
		,rr.[MISC3]
		,rr.[EXCEPTIONSTATUS]
		,rr.[VERIFIED]
		,rr.[STATUS]
		,rr.[TSRECEIVED]
		,rr.[USERCREATED]
		,rr.[RECEIVEONLY]
		,rr.[EXCEPTIONREASON]
		,isnull(dw.RapidReceivingID, 0) AS RapidReceivingID
	INTO #rr
	FROM GloryStaging.cdc.fn_cdc_get_net_changes_eVAS_RapidRecv_Bags(@from_lsn, @to_lsn, N'all') rr
	LEFT JOIN [CMS_DW].dbo.[RapidRecv_Bags] dw ON rr.[MANIFESTID] = dw.[MANIFESTID]
		AND rr.[SEALID] = dw.[SEALID]
		AND rr.[SITEID] = dw.[SITEID]
		AND rr.[CLIENTID] = dw.[CLIENTID]
		AND rr.[COURIERGLN] = dw.[COURIERGLN]
		AND rr.[BAGID_SSCC] = dw.[BAGID_SSCC]
		AND dw.[TSRECEIVED] > dateadd(dd, - 30, getdate())
	INNER JOIN [CMS_DW].dbo.[RAPIDRECEIVING] r ON r.[MANIFESTID] = rr.[MANIFESTID]
		AND r.[SEALID] = rr.[SEALID]
		AND r.[SITEID] = rr.[SITEID]
		AND r.[CLIENTID] = rr.[CLIENTID]
		AND r.[COURIERGLN] = rr.[COURIERGLN]
		AND r.EXPECTEDDATE > dateadd(dd, - 7, getdate())
	WHERE _$operation <> 1

	UPDATE rr
	SET [LOCATIONGLN] = t.[LOCATIONGLN]
		,[LOCATIONNAME] = t.[LOCATIONNAME]
		,[LOCATIONADDRESS] = t.[LOCATIONADDRESS]
		,[SAIDTOCONTAIN] = t.[SAIDTOCONTAIN]
		,[TRACE] = t.[TRACE]
		,[MISC1] = t.[MISC1]
		,[MISC2] = t.[MISC2]
		,[MISC3] = t.[MISC3]
		,[EXCEPTIONSTATUS] = t.[EXCEPTIONSTATUS]
		,[VERIFIED] = t.[VERIFIED]
		,[STATUS] = t.[STATUS]
		,[TSRECEIVED] = t.[TSRECEIVED]
		,[USERCREATED] = t.[USERCREATED]
		,[RECEIVEONLY] = t.[RECEIVEONLY]
		,[EXCEPTIONREASON] = t.[EXCEPTIONREASON]
	FROM [CMS_DW].dbo.[RapidRecv_Bags] rr
	INNER JOIN #rr t ON t.RapidRecv_BagsID = rr.RapidRecv_BagsID

	INSERT INTO [CMS_DW].dbo.RapidRecv_Bags (
		RapidReceivingID
		,[MANIFESTID]
		,[SEALID]
		,[SITEID]
		,[CLIENTID]
		,[COURIERGLN]
		,[BAGID_SSCC]
		,[LOCATIONGLN]
		,[LOCATIONNAME]
		,[LOCATIONADDRESS]
		,[SAIDTOCONTAIN]
		,[TRACE]
		,[MISC1]
		,[MISC2]
		,[MISC3]
		,[EXCEPTIONSTATUS]
		,[VERIFIED]
		,[STATUS]
		,[TSRECEIVED]
		,[USERCREATED]
		,[RECEIVEONLY]
		,[EXCEPTIONREASON]
		)
	SELECT rr.RapidReceivingID
		,rr.[MANIFESTID]
		,rr.[SEALID]
		,rr.[SITEID]
		,rr.[CLIENTID]
		,rr.[COURIERGLN]
		,rr.[BAGID_SSCC]
		,rr.[LOCATIONGLN]
		,rr.[LOCATIONNAME]
		,rr.[LOCATIONADDRESS]
		,rr.[SAIDTOCONTAIN]
		,rr.[TRACE]
		,rr.[MISC1]
		,rr.[MISC2]
		,rr.[MISC3]
		,rr.[EXCEPTIONSTATUS]
		,rr.[VERIFIED]
		,rr.[STATUS]
		,rr.[TSRECEIVED]
		,rr.[USERCREATED]
		,rr.[RECEIVEONLY]
		,rr.[EXCEPTIONREASON]
	FROM #rr rr
	LEFT JOIN [CMS_DW].dbo.[RapidRecv_Bags] rb ON rr.[MANIFESTID] = rb.[MANIFESTID]
		AND rr.[SEALID] = rb.[SEALID]
		AND rr.[SITEID] = rb.[SITEID]
		AND rr.[CLIENTID] = rb.[CLIENTID]
		AND rr.[COURIERGLN] = rb.[COURIERGLN]
		AND rr.[BAGID_SSCC] = rb.[BAGID_SSCC]
	WHERE rr.RapidRecv_BagsID = 0
		AND rb.RapidRecv_BagsID IS NULL I

	-- BEGIN
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
