USE [GloryStaging]
GO

/*******************************************************************************
 Object: StoredProcedure [dbo].[GloryStaging_e_to_FactTransaction_Transactions]
*******************************************************************************/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:      Rich Fox
-- Create date: 10/01/2018
-- Description: Load Transactions from CDC staging tables into FactTransaction.
-- =============================================
ALTER PROCEDURE [dbo].[GloryStaging_e_to_FactTransaction_Transactions]
AS
BEGIN
    SET NOCOUNT ON; -- Changed TON to ON
    SET DEADLOCK_PRIORITY HIGH;
    -- SET XACT_ABORT ON -- Optional: Uncomment if needed for atomic transactions

    DECLARE @from_lsn BINARY(10);
    DECLARE @to_lsn BINARY(10);
    DECLARE @Row_filter_option NVARCHAR(30) = N'all';
    DECLARE @TableName VARCHAR(100) = 'eVAS_TRANSACTIONS'; -- Increased size for safety, corrected table name casing based on usage

    -- Get the maximum LSN processed so far for any table (consider if this is the correct logic)
    -- NOTE: This seems potentially wrong. Usually, you get the max LSN for the *specific* source table being processed.
    --       Using 'max_Tablevalue' might fetch an LSN unrelated to eVAS_TRANSACTIONS changes.
    --       Consider changing 'max_Tablevalue' to @TableName if you want the LSN specific to this table's CDC.
    SELECT @to_lsn = [to_lsn] -- Assuming 'to_lsn' is the correct column name
    FROM GloryStaging.[dbo].[ETL_Control_Table]
    WHERE [TableName] = 'max_Tablevalue'; -- Or potentially use @TableName here?

    -- Get the LSN from the last successful run for this specific table
    SELECT @from_lsn = MAX(to_lsn) -- Corrected typo 'to_1sn' to 'to_lsn'
    FROM GloryStaging.dbo.etl_control_table
    WHERE tablename = @TableName;

    -- If this is the first run, get the minimum LSN available for the source table
    IF @from_lsn IS NULL
    BEGIN
        -- NOTE: Ensure 'eVAS_TRANSACTIONS' is the correct capture instance name.
        --       It might be different, e.g., 'dbo_eVAS_TRANSACTIONS'. Check sys.cdc_capture_instances.
        SET @from_lsn = sys.fn_cdc_get_min_lsn(@TableName); -- Use the variable
    END

    -- Check if @to_lsn was successfully retrieved. If not, get the current max LSN.
    IF @to_lsn IS NULL
    BEGIN
         SET @to_lsn = sys.fn_cdc_get_max_lsn();
    END;

    -- Create Temporary Table to stage changes
    SELECT DISTINCT -- Added DISTINCT based on original query, review if necessary
           t.[CLIENTNO]
         , t.[SITENO]
         , t.[TERMINALNO]
         , t.[TRACE]
         , t.[LOGGINGUSER]
         , t.[ASSIGNEDUSER]
         , t.[PROCUSER]
         , t.[SHIFTNO]
         , t.[LEVEL1]
         , dal.[LocationID] -- Added LocationID from DimAccountLocation
         , t.[LEVEL3]
         , t.[LEVEL4]
         , t.[LEVEL5]
         , LEFT(t.[LEVEL6], 10) AS [LEVEL6]
         , t.[LEVEL7] AS DepType
         -- , t.[LEVEL7] AS DepType -- Duplicate column removed
         , RIGHT(t.[LEVEL8], 10) AS [Agent] -- Renamed alias to avoid conflict
         -- , RIGHT(t.[LEVEL8], 10) [Agent] -- Duplicate column removed
         , t.[LEVEL9]
         , ISNULL(mbd.LEVEL10, t.level10) AS [ActivityDate]
         -- , ISNULL(mbd.LEVEL10, t.level10) AS [ActivityDate] -- Duplicate column removed
         , CASE
               WHEN ISNUMERIC(t.level11) = 1 AND CAST(t.level11 AS BIGINT) > 1000000000 THEN 0 -- SYNTAX ERROR FIX: 'e' changed to 0 (assuming numeric)
               WHEN ISNUMERIC(t.level11) = 1 THEN CAST(t.[LEVEL11] AS MONEY) / 100 -- Added CAST for safety
               ELSE 0 -- Handle non-numeric cases
           END AS [LEVEL11]
         -- , CASE
         --       WHEN CAST(t.level11 AS BIGINT) > 1000000000 THEN O -- Duplicate column removed, SYNTAX ERROR FIX: 'O' changed to 0
         --       ELSE t.[LEVEL11] / 100
         --   END AS [LEVEL11]
         , CASE
               WHEN ISNUMERIC(t.[LEVEL12]) = 1 AND CAST(t.[LEVEL12] AS BIGINT) > 1000000000 THEN 0
               WHEN ISNUMERIC(t.[LEVEL12]) = 1 THEN ISNULL(CAST(t.[LEVEL12] AS MONEY), 0) / 100
               ELSE 0 -- Handle non-numeric cases or NULL explicitly
           END AS [LEVEL12]
         -- , CASE
         --       WHEN ISNULL(CAST(t.[LEVEL12] AS BIGINT), '0') > 1000000000 THEN 0 -- Duplicate column removed
         --       ELSE ISNULL(CAST(t.[LEVEL12] AS MONEY), '0') / 100
         --   END AS [LEVEL12]
         -- ,isnull([LEVEL12],'0') / 100 as [LEVEL12] -- Commented out duplicate logic
         , t.[LEVEL13]
         , t.[LEVEL14]
         , t.[LEVEL15]
         , ISNULL(t.[DECTOTAL], 0) / 100 AS [DECTOTAL] -- Added ISNULL for safety
         -- , t.[DECTOTAL] / 100 AS [DECTOTAL] -- Duplicate column removed
         , ISNULL(t.[DECCASH], 0) / 100 AS [DECCASH]
         -- , t.[DECCASH] / 100 AS [DECCASH] -- Duplicate column removed
         , t.[CHANGEFUND]
         , t.[PUAMOUNT]
         , t.[STATUS]
         , t.[MODES] -- Corrected typo MODes
         , t.[TYPE]
         , ISNULL(t.[VERIFYAMT], 0) / 100 AS [VERIFYAMT]
         -- , t.[VERIFYAMT] / 100 AS [VERIFYAMT] -- Duplicate column removed
         , ISNULL(t.[VERIFYCASH], 0) / 100 AS [VERIFYCASH]
         -- , t.[VERIFYCASH] / 100 AS [VERIFYCASH] -- Duplicate column removed
         , ISNULL(t.[DIFFERENCE], 0) / 100 AS [DIFFERENCE]
         -- , t.[DIFFERENCE] / 100 AS [DIFFERENCE] -- Duplicate column removed
         , t.[SMALLCOIN]
         , t.[DECBAGS]
         , t.[LOGGINGDATE]
         -- , t.[LOGGINGDATE] -- Duplicate column removed
         , t.[LOGGINGTIME]
         -- , t.[LOGGINGTIME] -- Duplicate column removed
         -- , t.[LOGGINGDATE] -- Duplicate column removed
         -- , t.[LOGGINGTIME] -- Duplicate column removed
         , t.[ASSIGNEDDATE]
         , t.[ASSIGNEDTIME]
         , t.[CLOSEDDATE]
         , t.[CLOSEDTIME]
         , t.[UPDATEDATE]
         , t.[UPDATETIME]
         , t.[CLEARFLG]
         , t.[DECENVS]
         , t.[ASSIGNFLG]
         , t.[SOLDTO]
         , t.[OVERRIDE]
         , t.[DCPROCESS] -- SYNTAX ERROR FIX: Added comma
         , t.[PROCESSTIME]
         , t.[VERAMOUNT1]
         , t.[VERAMOUNT2]
         , t.[VERAMOUNT3]
         , t.[VERAMOUNT4]
         , t.[EXTRACT_TS]
         , t.[EXTRACT_STAT]
         , t.[CWEXPORT]
         , t.[TCLOSEDATE]
         , t.[EXPLOGFLAG]
         , t.[ATMCYCLE]
         , t.[EXTREFTRACE]
         , t.[EXTREFMBDTRACE]
         , t.[EXTREFMBTRACE]
         , t.[LEVEL16]
         , t.[CWOFFSET]
         , t.[AGENTTIME]
         , t.[MICRMASK]
         -- , t.[MICRMASK] -- Duplicate column removed
         , t.[PROCUSERNAME]
         , rt.SUPERTRANS AS Parrent_Trace
         , ft.FactTransactionID
         , dal.LocationNumber
         , dal.DimAccntLocID
         , da.dimaccntid
         , dm.DimBranchid
         , dm.[DimLoomisCustMltVltID] -- Corrected typo: V1tID -> VltID
         , dar.DImActivityReportID
         , dd_locd.dimdateid AS acctloc_Dateid
         , dd_crd.dimdateid AS credit_Dateid
         , dd_lgd.dimdateid AS log_Dateid
         , dd_prd.dimdateid AS proc_Dateid
         , dt_prd.DImTimeID AS Proc_timeid
         , dt_log.DImTimeID AS Log_timeID
         , NULL AS ActLog_timeID -- Explicitly defined
         , t.[LOGACTEXPDATE]
         , del.DimEmployeeID AS log_dimid
         , des.DimEmployeeID AS sup_dimid
         , det.DimEmployeeID AS tel_dimid
         , dea.DimEmployeeID AS telass_dimid -- Added 09242020
         , dd_act.DimDateID AS ActLog_DateID
         , cr_time.dimtimeid AS cr_dimtime
         , cr_date.dimdateid AS cr_dimdate
         , CASE
               WHEN (ttp.Type = 102 OR t.Type = 106) THEN 1
               ELSE NULL -- Changed 0 to NULL as it represents a boolean flag maybe?
           END AS SmartSafeImported
         , t.[PKGFWDFLG] AS PackageForward
         , LAS.COURIERID AS LogCourier
         , COALESCE(tmo.modesdesc, CAST(t.MODES AS VARCHAR(20)), '') + ' ' + COALESCE(ttp.typedesc, CAST(t.[type] AS VARCHAR(20)), '') AS ProcessingMode
         , CASE
               WHEN ifs.SITENO IS NULL THEN 0
               ELSE 1
           END AS IFS
         , mbd.TRACE AS MBD_Child
         , mbd.STATUS AS MBD_Status
         , CAST(NULL AS DATE) AS CITServiceDate -- Explicitly defined
         , CAST(NULL AS TIME) AS CITServiceTime -- Explicitly defined
         , t.TERMINALNO AS StationNumber
         , cdc.__$operation -- Include CDC operation type
    INTO #tempTrans
    -- NOTE: Ensure 'eVAS_TRANSACTIONS' is the correct capture instance name for the CDC function.
    FROM [cdc].[fn_cdc_get_net_changes_eVAS_TRANSACTIONS](@from_lsn, @to_lsn, @Row_filter_option) t
    LEFT JOIN [GloryStaging].eVAS.[TRANSRELATION] rt
        ON rt.TRACE = t.TRACE
        AND rt.VERSION = 0 -- Added condition based on later WHERE clause
    LEFT JOIN (
        SELECT mbd_i.TRACE
             , mbd_i.STATUS
             , mbd_jn_i.SUPERTRANS
             , mbd_i.LEVEL10
        FROM [GloryStaging].eVAS.[TRANSRELATION] mbd_jn_i
        INNER JOIN GloryStaging.eVAS.TRANSACTIONS mbd_i
            ON mbd_jn_i.TRACE = mbd_i.TRACE
            AND mbd_i.LOGGINGUSER IS NOT NULL -- Moved conditions from WHERE to ON
            AND mbd_i.TYPE IN (16)             -- Moved conditions from WHERE to ON
    ) mbd
        ON t.TRACE = mbd.SUPERTRANS
    LEFT JOIN [GloryStaging].eVAS.[GROUPUSER] gg -- Ensure this join is necessary if filtering later
        ON gg.[GLORYID] = t.[PROCUSER]
    LEFT JOIN [GloryStaging].eVAS.[SMARTSAFE] sm -- Alias 'sm' is unused? Remove if not needed.
        ON sm.trace = t.trace
    LEFT JOIN GloryStaging.[eVAS].[LOGGING] lg -- SYNTAX ERROR FIX: Alias '1 g' changed to 'lg'
        ON t.trace = lg.TRACE
        -- AND t.VERSION = lg.VERSION -- This condition seems problematic with t.version=0 filter later. Review needed.
        AND lg.version = 0 -- Assuming you want version 0 from LOGGING table
    LEFT JOIN GloryStaging.[eVAS].LASESSION LAS
        ON lg.SESSIONNO = las.SESSIONNO
        AND lg.SITENO = las.SITENO
        AND lg.version = 0 -- Assuming version 0 from LASESSION related to LOGGING entry
    LEFT JOIN CMS_DW.dbo.FactTransaction ft
        ON t.[TRACE] = ft.traceno
        AND ft.Application_Instance_ID = 999 -- Added Application_Instance_ID for specificity
    LEFT JOIN CMS_DW.dbo.DimAccountLocation dal
        ON t.LEVEL2 = dal.AcctLocationID
        AND t.CLIENTNO = dal.mvid
        AND t.siteno = dal.branchid
        AND t.LEVEL1 = dal.AccountNo
        AND dal.[Application_Instance_ID] = 999
        AND dal.Active <> -1 -- Ensure only active locations are joined
    LEFT JOIN CMS_DW.dbo.DimAccount da
        ON dal.dimaccntid = da.dimaccntid
        AND da.[Application_Instance_ID] = 999 -- Added for consistency if DimAccount is instance specific
    LEFT JOIN CMS_DW.dbo.DimLoomisCustomerMultiVault dm
        ON dm.branchid = t.siteno
        AND dm.mvid = t.clientno
        AND dm.[Application_Instance_ID] = 999
    LEFT JOIN CMS_DW.dbo.DimEmployee det -- Teller (Processing User)
        ON t.[PROCUSER] = det.[EmployeeNumber]
        AND det.BranchID = t.[SITENO] -- Match on BranchID as well?
        AND det.[Application_Instance_ID] = 999
    LEFT JOIN CMS_DW.dbo.DimEmployee del -- Logger (Logging User)
        ON t.[LOGGINGUSER] = del.[EmployeeNumber]
        AND del.BranchID = t.[SITENO]
        AND del.[Application_Instance_ID] = 999
    LEFT JOIN CMS_DW.dbo.DimEmployee des -- Supervisor (Override User)
        ON t.OVERRIDE = des.[EmployeeNumber]
        AND des.BranchID = t.[SITENO]
        AND des.[Application_Instance_ID] = 999
    LEFT JOIN CMS_DW.dbo.DimEmployee dea -- Teller Assigned (Assigned User) -- Added 09242020
        ON t.ASSIGNEDUSER = dea.[EmployeeNumber]
        AND dea.BranchID = t.[SITENO]
        AND dea.[Application_Instance_ID] = 999 -- SYNTAX ERROR FIX: Removed duplicate condition
    LEFT JOIN cms_dw.dbo.DImActivityReport dar
        ON dm.[DimLoomisCustMltVltID] = dar.[DimLoomisCustMltVltID]
        AND dar.[ActRpt_Date_To] = ISNULL(mbd.LEVEL10, t.level10) -- Corrected typo: leve110 -> level10
    LEFT JOIN cms_dw.dbo.DImDate dd_locd
        ON LEFT(t.[LEVEL6], 10) = dd_locd.full_date
    LEFT JOIN cms_dw.dbo.DImDate dd_crd
        ON t.level5 = dd_crd.full_date
    LEFT JOIN cms_dw.dbo.DImDate dd_lgd
        ON t.[LOGGINGDATE] = dd_lgd.full_date
    LEFT JOIN cms_dw.dbo.DImDate dd_prd
        ON t.[CLOSEDDATE] = dd_prd.full_date
    LEFT JOIN cms_dw.dbo.DImDate dd_act
        ON CAST(t.[LOGACTEXPDATE] AS DATE) = dd_act.full_date
    LEFT JOIN cms_dw.dbo.DimTime dt_prd
        ON CAST(t.[CLOSEDTIME] AS TIME) = dt_prd.fulltime
    LEFT JOIN cms_dw.dbo.DimTime dt_log
        ON CAST(t.[LOGGINGTIME] AS TIME) = dt_log.fulltime
    LEFT JOIN cms_dw.dbo.DimTime cr_time -- Current Time Dim
        ON CAST(GETDATE() AS TIME) = cr_time.fulltime
    LEFT JOIN cms_dw.dbo.DImDate cr_date -- Current Date Dim
        ON CAST(GETDATE() AS DATE) = cr_date.full_date
    LEFT JOIN GloryStaging.dbo.TransactionModes tmo
        ON t.MODES = tmo.MODES -- Corrected typo: MODes -> MODES
    LEFT JOIN GloryStaging.dbo.TransactionTypes ttp
        ON t.type = ttp.type
    LEFT JOIN [GloryStaging].[eVAS].[IFSDATA] ifs
        ON ifs.TRACE = t.TRACE
        AND ifs.SITENO = t.SITENO
        AND ifs.CLIENTNO = t.CLIENTNO
        AND ifs.STATUS = 'IR'
    -- LEFT JOIN [GloryStaging].[eVAS].[RAPIDRECV_BAGS] rrb -- This join seems incomplete or incorrect
    --     ON rrb.TRACE = t.TRACE OR rrb.[BAGID_SSCC] = t.LEVEL14 -- Corrected typo Leve14 -> LEVEL14, Review logic (JOIN or WHERE?)
    WHERE
        t.version = 0 -- Process only version 0? Review requirement.
        -- AND rt.VERSION = 0 -- Moved to JOIN condition
        AND (
                gg.GROUPID IN ('06', '09') -- Filter based on processing user's group?
             OR t.MODES NOT IN (10, 11)    -- Or specific modes
            )
        AND t.TYPE NOT IN (32, 16)        -- Exclude specific types
        AND t.MODES NOT IN (32, 53)       -- Exclude specific modes (Corrected typo MODes)
        -- AND t.__$operation IN (2, 4)   -- Process only Inserts (4) and Updates (2) (Using standard CDC column name __$operation)
                                          -- Original _$operation might be correct if custom named. Verify CDC setup.
        AND (t.[CLOSEDDATE] > DATEADD(day, -45, GETDATE()) OR t.[CLOSEDDATE] IS NULL) -- Process recent or open transactions
        AND t.trace <> 52101234003739 -- Exclude specific trace number
    OPTION (RECOMPILE); -- Use recompile hint if parameter sniffing is an issue


    -- Update DimAccntID, DimAccntLocID, LocationNumber based on EXPLOG_DETAILS if missing
    UPDATE tt
    SET DimAccntID = dal_upd.DimAccntID,
        DimAccntLocID = dal_upd.DimAccntLocID,
        LocationNumber = dal_upd.LocationNumber
    FROM #tempTrans tt
    INNER JOIN [GloryStaging].[eVAS].[EXPLOG_DETAILS] ed
        ON tt.trace = ed.TRACENO -- SYNTAX ERROR FIX: Added ON keyword
    INNER JOIN CMS_DW.dbo.DimAccountLocation dal_upd -- Used a different alias to avoid confusion
        ON ed.LocationID = dal_upd.AcctLocationID
        AND tt.CLIENTNO = dal_upd.mvid
        AND dal_upd.branchid = 9450 -- Specific Branch ID?
        AND tt.LEVEL1 = dal_upd.AccountNo
        AND dal_upd.[Application_Instance_ID] = 999 -- SYNTAX ERROR FIX: Added condition based on context
        AND dal_upd.Active <> -1 -- Ensure only active locations are used for update
    WHERE tt.DimAccntLocID IS NULL; -- Only update if the initial join didn't find a match


    -- Removing MB Transactions for specific clients (Manual exclusion?)
    DELETE FROM #tempTrans
    WHERE MODES = 1
      AND TYPE = 4
      AND CLIENTNO IN (32, 2172);


    -- Update #tempTrans for BOA ATM Express Log specific logic
    -- LOGIC NOTE: Review this section carefully. Assignments seemed incorrect. Made assumptions.
    UPDATE T
    SET MODES = 1,
        TYPE = 103,
        LEVEL6 = CAST(dp.AtmBusDate AS VARCHAR(10)), -- SYNTAX ERROR FIX: Added '=' sign, specified length
        [DECTOTAL] = dp.BegBal,
        [DECCASH] = dp.ExpectedResidualTotal,
        Level11 = ISNULL(o.GTOTAL, 0) / 100,
        -- Level11 = ISNULL(o.GTOTAL, 0) / 100, -- SYNTAX ERROR FIX: Removed apparent duplicate/incorrect line
        Level12 = dp.Dispense,
        -- AgentTime = dp.EventTs, -- SYNTAX ERROR FIX: Assuming AgentTime should be updated from EventTs. Verify required logic.
                                  -- Original code had ', = AgentTime EventTs' which is invalid.
        ProcessingMode = 'ATM EXPRESS'
    FROM #tempTrans T
    INNER JOIN ExpressLog.Residual dp ON t.LEVEL4 = dp.BagId
    LEFT JOIN EVAS.CUOORDER O
        ON t.CLIENTNO = o.MVAREA       -- SYNTAX ERROR FIX: Changed 0. to o.
       AND dp.AtmBusDate = o.DELIVERYDATE
       AND t.LocationID = o.ACCTLOCID -- SYNTAX ERROR FIX: Changed 0. to o.
    WHERE t.CLIENTNO = 32
      AND t.MODES = 1
      AND t.TYPE = 64;

-- END OF new part FOR BOA ATM Express Log

-- Begin Transaction for applying changes to the target table
BEGIN TRAN;

    -- UPDATE existing records in FactTransaction
     
    SET [DimTransactionTypeID] =
            CASE
                -- LOGIC NOTE: Added conditions for status 5/mode 8 based on comments. Review priority.
                WHEN t.STATUS IN (0, 1, 2) THEN 2
                WHEN t.MBD_Status IN (0, 1, 2) THEN 2
                WHEN t.STATUS IN (5) AND t.MODES = 8 THEN 2 -- Based on comment
                WHEN t.MODES IN (2) THEN 5
                WHEN t.MODES IN (1) AND t.TYPE IN (103, 105) THEN 5
                WHEN t.TYPE = 4 THEN 11
                WHEN t.MODES IN (0) AND t.TYPE = 1 THEN 3
                WHEN t.MODES IN (0) THEN 4
                 -- LOGIC NOTE: WHEN t.MODES IN (1) appears twice. Assuming the specific type check (103,105) takes precedence.
                 --             The generic 'WHEN t.MODES IN (1)' might need adjustment or removal if covered.
                 --             Assuming it maps to 3 based on INSERT statement.
                WHEN t.MODES IN (1) THEN 3
                WHEN t.MODES IN (12) THEN 9 -- SYNTAX ERROR FIX: Corrected CASE logic from 'THEN 3 THEN 9'
                WHEN t.MODES IN (11) THEN 14 -- SYNTAX ERROR FIX: Added missing THEN clause (assuming 14 based on INSERT)
                WHEN t.MODES IN (10) THEN 15 -- SYNTAX ERROR FIX: Added missing THEN clause (assuming 15 based on INSERT)
                WHEN t.MODES IN (8) THEN 3
                ELSE ft.DimTransactionTypeID -- Keep existing if no match
            END,
        [DimAccntID] = ISNULL(t.DimAccntID, ft.DimAccntID),
        [DimAccntLocID] = ISNULL(t.DimAccntLocID, ft.DimAccntLocID),
        [DimEmployeeID_Teller] = ISNULL(t.tel_dimid, t.telass_dimid), -- Added 09242020: Use assigned user if processing user is null
        [DimActivityReportID] = ISNULL(t.DImActivityReportID, ft.DimActivityReportID), -- Corrected typo: DimActivity ReportID -> DImActivityReportID
        [DimBillingTransTypeID] =
            CASE
                WHEN t.STATUS IN (1, 2, 5, 7, 8) OR t.TYPE = 4 THEN NULL
                WHEN t.MBD_Status IN (1, 2, 5, 7, 8) THEN NULL -- Removed 'OR type = 4' as it's covered by first WHEN
                WHEN t.MODES IN (0, 1, 2, 3, 4, 5, 8) THEN 1
                WHEN t.MODES IN (10) THEN 4
                WHEN t.MODES IN (11) AND t.[PROCUSER] LIKE '%P' THEN NULL -- Check PROCUSER ends with P? Review logic.
                WHEN t.MODES IN (11) THEN 3
                WHEN t.MODES IN (12) THEN 5
                ELSE ft.DimBillingTransTypeID -- Keep existing if no match
            END,
        [TraceNo_MasterBag] =
            CASE
                WHEN t.modes IN (64, 128, 66) THEN CAST(t.parrent_trace AS BIGINT)
                ELSE 0
            END,
        [IsMasterBag] =
            CASE
                WHEN t.TYPE = 4 THEN 'True'
                ELSE 'False'
            END,
        [TraceNo_MBD] = CAST(t.MBD_Child AS BIGINT),
        [DepositID] = t.LEVEL14, -- Corrected typo Leve14
        [MicrData] = t.[MICRMASK],
        [SerialNo] = SUBSTRING(t.LEVEL14, 1, 10), -- Corrected typo Leve114
        [Agent] = t.[Agent], -- Using alias defined in temp table select
        [IsOffset] =
            CASE
                WHEN t.STATUS IN (5, 7, 8) THEN 'True'
                WHEN t.[MODES] = 1 AND t.type = 128 THEN 'True'
                ELSE 'False'
            END,
        [Mode] = t.Modes, -- Corrected typo Modes
        [ProcessMode] = t.TYPE,
        [TellerMinutes] = CAST(DATEADD(second, ISNULL(t.PROCESSTIME,0), 0) AS TIME), -- Assuming PROCESSTIME is seconds, handle NULL
        [DeclaredTotal] = t.[DECTOTAL],
        [DeclaredItem] = t.[DECTOTAL], -- Assuming DeclaredItem is same as DeclaredTotal
        [DeclaredCash] = t.[DECCASH], -- Corrected syntax t [DECCASH] -> t.[DECCASH]
        [VerifiedCash] = t.[VERIFYCASH],
        [VerifiedTotal] = t.[VERIFYAMT],
        [Difference] = t.[DIFFERENCE],
        [EnvelopeCnt] = t.[DECENVS],
        [LocationNumber] = ISNULL(t.LocationNumber, ft.LocationNumber),
        [EmployeeNumber] = ISNULL(t.[PROCUSER], t.[ASSIGNEDUSER]), -- Use assigned user if processing user is null
        [ProcessingDate] = ISNULL(ft.[ProcessingDate], t.[CLOSEDDATE]), -- Keep existing if already set
        [ProcessingTime] = ISNULL(ft.[ProcessingTime], CAST(t.[CLOSEDTIME] AS TIME)), -- Keep existing if already set
        [LoggedActivityRptDate] = CAST(t.[LOGACTEXPDATE] AS DATE), -- Update based on temp table
        [CreditDate] = t.LEVEL15, -- Corrected typo Leve15
        [AccntLocDate] = t.LEVEL6, -- Corrected typo Leve16 (mapping to LEVEL6 based on INSERT)
        [DimDateID_ProcessingDate] = ISNULL(ft.[DimDateID_ProcessingDate], t.proc_Dateid),
        [DimTimeID_ProcessingTime] = ISNULL(ft.[DimTimeID_ProcessingTime], t.Proc_timeid),
        [DimDateID_LoggedActivityRptDate] = t.ActLog_DateID,
        [DimDateID_CreditDate] = t.credit_Dateid,
        [DimDateID_AccntLocDate] = t.acctloc_Dateid,
        [Atm_CurrExchange] =
            CASE
                WHEN t.modes = 2 OR (t.MODES = 1 AND t.type IN (103, 105)) THEN t.Level11
                ELSE 0
            END,
        [Atm_DispenseAmt] =
            CASE
                WHEN t.modes = 2 OR (t.MODES = 1 AND t.type IN (103, 105)) THEN t.Level12
                ELSE 0
            END,
        [RecordUpdateDateTime] = GETDATE(),
        [DimEmployeeID_Supervisor] = ISNULL(t.sup_dimid, ft.DimEmployeeID_Supervisor),
        [AgentTime] = CAST(t.[AGENTTIME] AS TIME),
        [SmartSafeImported] = ISNULL(t.SmartSafeImported, ft.SmartSafeImported),
        [PackageForward] = ISNULL(t.PackageForward, ft.PackageForward),
        [LoggingCourier] = ISNULL(t.LogCourier, ft.LoggingCourier),
        [ProcessingMode] = ISNULL(t.ProcessingMode, ft.ProcessingMode),
        [IFS] = ISNULL(t.IFS, ft.IFS),
        [CITServiceDate] = ISNULL(t.CITServiceDate, ft.CITServiceDate),
        [CITServiceTime] = ISNULL(t.CITServiceTime, ft.CITServiceTime), -- Corrected typo ft.CITService TIME
        [StationNumber] = ISNULL(t.StationNumber, ft.StationNumber) -- Corrected typo t.Station Number
    FROM CMS_DW.dbo.FactTransaction ft
    INNER JOIN #tempTrans t ON ft.FactTransactionID = t.FactTransactionID
    WHERE t.__$operation = 2; -- Apply updates only for CDC update operations


    -- INSERT new records into FactTransaction
    INSERT INTO CMS_DW.dbo.[FactTransaction] (
        [FactTransaction_NatKey],
        [DimTransactionTypeID],
        [DimBranchID],
        [DimAccntID],
        -- [DimAccntID], -- SYNTAX ERROR FIX: Removed duplicate column
        [DimAccntLocID],
        [DimLoomisCustMltVltID],
        [DimEmployeeID_Teller],
        [DimCurrencyID],
        [DimActivityReportID],
        [DimBillingOrderTypeID],
        [DimBillingSNSOrdTypeID],
        [DimBillingTransTypeID],
        [BranchID],
        [MVID],
        [TraceNo],
        [TraceNo_MasterBag],
        [IsMasterBag],
        [TraceNo_MBD],
        [DepositID],
        [MicrData],
        [SerialNo],
        [Agent],
        [IsOffset],
        [Mode],
        [ProcessMode],
        [TellerMinutes],
        [DeclaredTotal],
        [DeclaredItem],
        [DeclaredCash],
        [VerifiedCash],
        [VerifiedTotal],
        [Difference],
        [EnvelopeCnt],
        [LocationNumber],
        [Country],
        [DenominationBase],
        [EmployeeNumber],
        [ProcessingDate],
        [ProcessingTime],
        [CreateDate],
        [CreateTime],
        [LogDate],
        [LogTime],
        [LoggedActivityRptDate],
        [LoggedActivityRptTime],
        [CreditDate],
        [AccntLocDate],
        [DimDateID_ProcessingDate],
        [DimTimeID_ProcessingTime],
        [DimDateID_CreateDate],
        [DimTimeID_CreateTime],
        [DimDateID_LogDate],
        [DimTimeID_LogTime],
        [DimDateID_LoggedActivityRptDate],
        [DimTimeID_LoggedActivityRptTime],
        [DimDateID_CreditDate],
        [DimDateID_AccntLocDate],
        [Atm_CurrExchange],
        [Atm_DispenseAmt],
        [RecordCreateDateTime],
        [RecordUpdateDateTime],
        [DimEmployeeID_Teller_Logged],
        [DimEmployeeID_Supervisor],
        [Application_Instance_ID],
        [AGENTTIME],
        [SmartSafeImported],
        -- [SmartSafeImported], -- SYNTAX ERROR FIX: Removed duplicate
        [PackageForward],
        [LoggingCourier],
        [ProcessingMode],
        [IFS],
        [CITServiceDate],
        [CITServiceTime],
        [StationNumber]
    )
    SELECT
        CAST(trace AS VARCHAR(50)) + '_999', -- Assuming Trace + Instance ID is the natural key
        CASE -- Logic replicated from UPDATE for consistency. Review needed.
            WHEN STATUS IN (0, 1, 2) THEN 2
            WHEN MBD_Status IN (0, 1, 2) THEN 2
            WHEN STATUS IN (5) AND MODES = 8 THEN 2 -- Added condition based on UPDATE logic.
            WHEN MODES IN (2) THEN 5
            WHEN MODES IN (0) AND type = 1 THEN 3
            WHEN MODES IN (0) THEN 4
            WHEN MODES IN (1) AND type IN (103, 105) THEN 5
            WHEN TYPE = 4 THEN 11
            WHEN MODES IN (1) THEN 3 -- General Mode 1 case
            WHEN MODES IN (12) THEN 9
            WHEN MODES IN (10) THEN 15
            WHEN MODES IN (11) THEN 14
            WHEN MODES IN (8) THEN 3
            ELSE NULL -- Default / Unknown?
        END,
        DimBranchID,
        DimAccntID,
        DimAccntLocID,
        [DimLoomisCustMltVltID],
        ISNULL(tel_dimid, telass_dimid), -- Teller Dim ID (Processing or Assigned)
        1, -- DimCurrencyID (Assuming 1 = USD or default)
        DimActivityReportID,
        0, -- DimBillingOrderTypeID (Defaulting to 0, review)
        0, -- DimBillingSNSOrdTypeID (Defaulting to 0, review)
        CASE -- Logic replicated from UPDATE
            WHEN STATUS IN (1, 2, 5, 7, 8) OR type = 4 THEN NULL
            WHEN MBD_Status IN (1, 2, 5, 7, 8) THEN NULL
            WHEN MODES IN (0, 1, 2, 3, 4, 5, 8) THEN 1
            WHEN MODES IN (10) THEN 4
            WHEN MODES IN (11) AND [PROCUSER] LIKE '%P' THEN NULL
            WHEN MODES IN (11) THEN 3
            WHEN MODES IN (12) THEN 5 -- SYNTAX ERROR FIX: Removed duplicate condition
            ELSE NULL -- Default
        END,
        [SITENO], -- BranchID
        Clientno, -- MVID
        [TRACE], -- TraceNo
        CASE WHEN modes IN (64, 128, 66) THEN CAST(parrent_trace AS BIGINT) ELSE 0 END, -- TraceNo_MasterBag
        CASE WHEN TYPE = 4 THEN 'True' ELSE 'False' END, -- IsMasterBag
        -- CASE WHEN TYPE IN (1) THEN CAST(MBD_Child AS BIGINT) ELSE 0 END, -- TraceNo_MBD (Logic differs from UPDATE? Review needed)
        CAST(MBD_Child AS BIGINT), -- Matched logic to UPDATE
        LEVEL14, -- DepositID (Corrected typo Leve14)
        [MICRMASK], -- MicrData
        SUBSTRING(LEVEL14, 1, 10), -- SerialNo (Corrected typo Leve114)
        [Agent], -- Agent
        CASE
            WHEN STATUS IN (5, 7, 8) THEN 'True'
            WHEN [MODES] = 1 AND TYPE = 128 THEN 'True' -- SYNTAX ERROR FIX: Removed 'THEN True'
            ELSE 'False'
        END, -- IsOffset
        Modes, -- Mode
        TYPE, -- ProcessMode
        CAST(DATEADD(second, ISNULL(PROCESSTIME, 0), 0) AS TIME), -- TellerMinutes
        [DECTOTAL], -- DeclaredTotal
        [DECTOTAL], -- DeclaredItem
        [DECCASH], -- DeclaredCash
        [VERIFYCASH], -- VerifiedCash
        [VERIFYAMT], -- VerifiedTotal
        [DIFFERENCE], -- Difference
        [DECENVS], -- EnvelopeCnt
        LocationNumber, -- LocationNumber
        'USA', -- Country (Hardcoded)
        'Dollar', -- DenominationBase (Hardcoded)
        ISNULL([PROCUSER], ASSIGNEDUSER), -- EmployeeNumber
        [CLOSEDDATE], -- ProcessingDate
        CAST([CLOSEDTIME] AS TIME), -- ProcessingTime
        [UPDATEDATE], -- CreateDate (Mapping UpdateDate to CreateDate? Review)
        CAST([UPDATETIME] AS TIME), -- CreateTime (Mapping UpdateTime to CreateTime? Review)
        [LOGGINGDATE], -- LogDate
        CAST([LOGGINGTIME] AS TIME), -- LogTime
        -- CASE WHEN [LOGGINGDATE] < [ActivityDate] THEN [LOGGINGDATE] ELSE NULL END, -- LoggedActivityRptDate (Logic seems specific, review)
        CAST([LOGACTEXPDATE] AS DATE), -- Using direct map based on UPDATE
        CAST([LOGACTEXPDATE] AS TIME), -- LoggedActivityRptTime (Mapping Date field to Time? Review)
        LEVEL15, -- CreditDate (Corrected typo Leve15)
        LEVEL6, -- AccntLocDate (Corrected typo Leve16, mapped to LEVEL6 based on DimDate join)
        proc_Dateid, -- DimDateID_ProcessingDate
        Proc_timeid, -- DimTimeID_ProcessingTime
        cr_dimdate, -- DimDateID_CreateDate (Using current date)
        cr_dimtime, -- DimTimeID_CreateTime (Using current time)
        log_Dateid, -- DimDateID_LogDate
        log_timeid, -- DimTimeID_LogTime
        ActLog_DateID, -- DimDateID_LoggedActivityRptDate
        NULL, -- DimTimeID_LoggedActivityRptTime (No time component derived for this)
        credit_Dateid, -- DimDateID_CreditDate
        acctloc_Dateid, -- DimDateID_AccntLocDate (Corrected typo acctloc Dateid)
        CASE WHEN modes = 2 OR (MODES = 1 AND type IN (103, 105)) THEN Level11 ELSE 0 END, -- Atm_CurrExchange (Corrected typo Leve111)
        CASE WHEN modes = 2 OR (MODES = 1 AND type IN (103, 105)) THEN Level12 ELSE 0 END, -- Atm_DispenseAmt
        GETDATE(), -- 	
        GETDATE(), -- RecordUpdateDateTime
        log_dimid, -- DimEmployeeID_Teller_Logged
        sup_dimid, -- DimEmployeeID_Supervisor
        999, -- Application_Instance_ID (Hardcoded)
        CAST([AGENTTIME] AS TIME), -- AGENTTIME
        SmartSafeImported,
        PackageForward,
        LogCourier,
        ProcessingMode,
        IFS,
        CITServiceDate,
        CITServiceTime,
        StationNumber
    FROM #tempTrans
    WHERE FactTransactionID IS NULL -- Only insert records that don't exist in the target
      AND __$operation = 4; -- Optional: Ensure only CDC inserts are processed if needed

    -- If everything succeeded, commit the transaction
    COMMIT TRAN;

    -- Clean up temporary table
    DROP TABLE #tempTrans;

	DELETE FROM GloryStaging.dbo.etl_control_table
	WHERE TableName = @TableName;
	INSERT INTO GloryStaging.dbo.etl_control_table
	(from_lsn, to_lsn, TableName
	VALUES
	(@from_lsn, @to_lsn, @TableName);
	COMMIT TRAN;
END;