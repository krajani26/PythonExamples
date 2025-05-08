USE [GloryStaging];
GO

-- *****************************************************************************************
-- Object: StoredProcedure [dbo].[GloryStaging_e_to_FactTransaction_Envelope] 
-- Script Date: 3/25/2025 8:10:32 PM 
-- Author: Rich Fox
-- Create date: 1/14/2015
-- Description: ETL procedure for envelopes from eVAS_Transactions to FactTransactionEnvelope.
-- *****************************************************************************************

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

ALTER PROCEDURE [dbo].[GloryStaging_e_to_FactTransaction_Envelope]
AS
BEGIN
    SET NOCOUNT ON;
    SET DEADLOCK_PRIORITY HIGH;

    DECLARE @from_lsn AS BINARY(10);
    DECLARE @to_lsn AS BINARY(10);
    DECLARE @Row_filter_option AS NVARCHAR(30) = N'all';
    DECLARE @TableName AS VARCHAR(20) = 'eVAS_Envelopes';

    -- Get the last processed LSN from the ETL control table
    SELECT @from_lsn = MAX(to_lsn)
    FROM GloryStaging.dbo.etl_control_table
    WHERE tablename = @TableName;

    -- Get the current maximum LSN for the table
    SELECT @to_lsn = [to_lsn]
    FROM GloryStaging.[dbo].[ETL_Control_Table]
    WHERE [TableName] = 'eVAS_Transactions';

    -- If no previous LSN, get the minimum LSN from the CDC table
    IF @from_lsn IS NULL
    BEGIN
        SET @from_lsn = sys.fn_cdc_get_min_lsn('eVAS_Transactions');
    END;

    -- Create a temporary table with the relevant data
    SELECT 
        tt.[CLIENTNO],
        tt.[SITENO],
        tt.[TERMINALNO],
        tt.[TRACE],
        tt.[LOGGINGUSER],
        tt.[ASSIGNEDUSER],
        tt.[PROCUSER],
        tt.[SHIFTNO],
        tt.[LEVEL1],
        tt.[leve12] AS LocationID,
        tt.[leve13],
        tt.[leve14],
        tt.[leve15],
        tt.[leve16],
        tt.[leve17] AS DepType,
        tt.[leve18] AS Agent,
        tt.[leve19],
        CAST(tt.level10 AS DATE) AS ActivityDate,
        tt.[LEVEL11],
        tt.[LEVEL12],
        tt.[LEVEL13],
        tt.[LEVEL14],
        tt.[LEVEL15],
        tt.[DECTOTAL] / 100 AS [DECTOTAL],
        tt.[DECCASH],
        tt.[CHANGEFUND],
        tt.[PUAMOUNT],
        tt.[STATUS],
        tt.[MODES],
        tt.[TYPE],
        tt.[VERIFYAMT] / 100 AS [VERIFYAMT],
        tt.[VERIFYCASH],
        tt.[DIFFERENCE] / 100 AS [DIFFERENCE],
        tt.[SMALLCOIN],
        tt.[DECBAGS],
        tt.[LOGGINGDATE],
        tt.[LOGGINGTIME],
        tt.[ASSIGNEDDATE],
        tt.[ASSIGNEDTIME],
        tt.[CLOSEDDATE],
        tt.[CLOSEDTIME],
        tt.[UPDATEDATE],
        tt.[UPDATETIME],
        tt.[CLEARFLG],
        tt.[DECENVS],
        tt.[ASSIGNFLG],
        tt.[SOLDTO],
        tt.[OVERRIDE],
        tt.[DCPROCESS],
        tt.[PROCESSTIME],
        tt.[VERAMOUNT1],
        tt.[VERAMOUNT2],
        tt.[VERAMOUNT3],
        tt.[VERAMOUNT4],
        tt.[EXTRACT_TS],
        tt.[EXTRACT_STAT],
        tt.[CWEXPORT],
        tt.[TCLOSEDATE],
        tt.[EXPLOGFLAG],
        tt.[ATMCYCLE],
        tt.[EXTREFTRACE],
        tt.[EXTREFMBDTRACE],
        tt.[EXTREFMBTRACE],
        tt.[LEVEL16],
        tt.[CWOFFSET],
        tt.[AGENTTIME],
        tt.[MICRMASK],
        tt.[PROCUSERNAME],
        rt.supertrans AS [Parrent_Trace],
        ft.FactTransactionID,
        e.[envelopeid],
        e.[shift],
        e.[initials]
    INTO #tempEnv
    FROM GloryStaging.cdc.[fn_cdc_get_net_changes_eVAS_TRANSACTIONS](@from_lsn, @to_lsn, @Row_filter_option) tt
    INNER JOIN glorystaging.[eVAS].[transrelation] rt ON rt.trace = tt.trace
    INNER JOIN CMS_DW.dbo.FactTransaction ft ON rt.supertrans = ft.traceno AND ft.Application_Instance_ID = 999
    LEFT JOIN glorystaging.[eVAS].[envelopeinfo] e ON e.trace = tt.trace
    WHERE tt.type = 32 AND tt.version = 0;

    -- Merge the temporary table into the FactTransactionEnvelope table
    MERGE CMS_DW.dbo.[FactTransactionEnvelope] AS e
    USING #tempEnv AS t
        ON (t.trace = e.[FactTransactionEnvelope_NatKey])
    WHEN NOT MATCHED BY TARGET
        THEN
            INSERT (
                [FactTransactionID],
                [FactTransactionEnvelope_NatKey],
                [BranchID],
                [MVID],
                [TraceNo],
                [Envelope_Number],
                [Shift_No],
                [Envelope_ID_Alpha],
                [DeclaredTotal],
                [VerifiedTotal],
                [RecordCreateDateTime],
                [RecordUpdateDateTime],
                [Application_Instance_ID],
                [Initials],
                [IsOffset]
            )
            VALUES (
                t.[FactTransactionID],
                t.trace,
                t.[SITENO],
                t.Clientno,
                t.parrent_trace,
                RIGHT(t.[TRACE], 3),
                t.[Shift],
                t.[ENVELOPEID],
                t.dectotal,
                t.[VERIFYAMT],
                GETDATE(),
                GETDATE(),
                999,
                t.[initials],
                CASE WHEN t.STATUS IN (5, 7, 8) THEN 'True' ELSE 'False' END
            )
    WHEN MATCHED
        THEN
            UPDATE
            SET 
                e.[Envelope_Number] = RIGHT(t.[TRACE], 3),
                e.[Shift_No] = t.[Shift],
                e.[Envelope_ID_Alpha] = t.[ENVELOPEID],
                e.[DeclaredTotal] = t.dectotal,
                e.[VerifiedTotal] = t.[VERIFYAMT],
                e.[RecordUpdateDateTime] = GETDATE(),
                e.[Initials] = t.[Initials],
                e.[IsOffset] = CASE WHEN t.STATUS IN (5, 7, 8) THEN 'True' ELSE 'False' END;

    -- Update the ETL control table
    DELETE FROM GloryStaging.dbo.etl_control_table WHERE TableName = @TableName;

    INSERT INTO GloryStaging.dbo.etl_control_table (from_lsn, to_lsn, TableName)
    VALUES (@from_lsn, @to_lsn, @TableName);

    DROP TABLE #tempEnv;