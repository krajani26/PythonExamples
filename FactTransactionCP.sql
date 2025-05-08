USE [GloryStaging];
GO

/**********************************************************************
Object:  StoredProcedure [dbo].[GloryStaging_e_to_FactTransaction_CP]
Script Date: 3/25/2025 8:13:38 PM
Author: Rich Fox
Create date: 10/01/2018
Description: Load Custom Prompts from CDC staging tables (eVAS_ANALYSISACTIVITYS).
**********************************************************************/
SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

ALTER PROCEDURE [dbo].[GloryStaging_e_to_FactTransaction_CP]
AS
BEGIN
    SET NOCOUNT ON;
    SET DEADLOCK_PRIORITY HIGH;

    DECLARE @from_lsn BINARY(10);
    DECLARE @to_lsn BINARY(10);
    DECLARE @Row_filter_option NVARCHAR(30) = N'all';
    DECLARE @TableName VARCHAR(20) = 'eVAS_ANALYSISACTIVITYS';

    -- Get the last processed LSN from the control table
    SELECT @to_lsn = [to_lsn]
    FROM GloryStaging.[dbo].[ETL_Control_Table]
    WHERE [TableName] = 'eVAS_Transactions';

    -- Get the maximum processed LSN for the current table
    SELECT @from_lsn = MAX(to_lsn)
    FROM GloryStaging.dbo.etl_control_table
    WHERE tablename = @TableName;

    -- If no previous LSN, get the minimum LSN from CDC
    IF @from_lsn IS NULL
    BEGIN
        SET @from_lsn = sys.fn_cdc_get_min_lsn(@TableName);
    END;

    -- Create a temporary table to hold the pivoted data
    SELECT
        su.TRACE,
        ISNULL(SUM([1]), 0) AS [Custom_Prompt_1],
        ISNULL(SUM([2]), 0) AS [Custom_Prompt_2],
        ISNULL(SUM([3]), 0) AS [Custom_Prompt_3],
        ISNULL(SUM([4]), 0) AS [StandardStraps],
        ISNULL(SUM([5]), 0) AS [StandardCoinBags],
        ISNULL(SUM([6]), 0) AS [NonStandardCoinBags],
        ISNULL(SUM([7]), 0) AS [WrappedCoinCount],
        ISNULL(SUM([8]), 0) AS [CoinBins],
        ISNULL(SUM([9]), 0) AS [HalfCoinBags],
        ISNULL(SUM([10]), 0) AS [Custom_Prompt_10],
        ISNULL(SUM([11]), 0) AS [Custom_Prompt_11],
        ISNULL(SUM([12]), 0) AS [Custom_Prompt_12],
        ISNULL(SUM([13]), 0) AS [Custom_Prompt_13],
        ISNULL(SUM([14]), 0) AS [Custom_Prompt_14],
        ISNULL(SUM([15]), 0) AS [Custom_Prompt_15],
        ISNULL(SUM([16]), 0) AS [Custom_Prompt_16]
    INTO #CP_Codes
    FROM [GloryStaging].eVAS.ANALYSISACTIVITYS
    PIVOT (
        MAX(quantity)
        FOR [ACTIVITYNO] IN (
            [1], 
			[2], 
			[3], 
			[4], 
			[5], 
			[6], 
			[7], 
			[8], 
			[9], 
			[10],
            [11], 
			[12], 
			[13], 
			[14], 
			[15], 
			[16]
        )
    ) AS su
    INNER JOIN (
        SELECT DISTINCT TRACE
        FROM [GloryStaging].[cdc].[fn_cdc_get_net_changes_eVAS_ANALYSISACTIVITYS](@from_lsn, @to_lsn, @Row_filter_option)
    ) AS c ON su.TRACE = c.TRACE
    GROUP BY su.TRACE;

    -- Update the fact table based on application_instance_id = 999 and TraceNo
    UPDATE ft
    SET
        [Custom_Prompt_1] = ISNULL(tt.[Custom_Prompt_1], ft.[Custom_Prompt_1]),
        [Custom_Prompt_2] = ISNULL(tt.[Custom_Prompt_2], ft.[Custom_Prompt_2]),
        [Custom_Prompt_3] = ISNULL(tt.[Custom_Prompt_3], ft.[Custom_Prompt_3]),
        [StandardCoinBags] = ISNULL(tt.[StandardCoinBags], ft.[StandardCoinBags]),
        [NonStandardCoinBags] = ISNULL(tt.[NonStandardCoinBags], ft.[NonStandardCoinBags]),
        [WrappedCoinCount] = ISNULL(tt.[WrappedCoinCount], ft.[WrappedCoinCount]),
        [StandardStraps] = ISNULL(tt.[StandardStraps], ft.[StandardStraps]),
        [CoinBins] = ISNULL(tt.[CoinBins], ft.[CoinBins]),
        [HalfCoinBags] = ISNULL(tt.[HalfCoinBags], ft.[HalfCoinBags]),
        [Custom_Prompt_10] = ISNULL(tt.[Custom_Prompt_10], ft.[Custom_Prompt_10]),
        [Custom_Prompt_11] = ISNULL(tt.[Custom_Prompt_11], ft.[Custom_Prompt_11]),
        [Custom_Prompt_12] = ISNULL(tt.[Custom_Prompt_12], ft.[Custom_Prompt_12]),
        [Custom_Prompt_13] = ISNULL(tt.[Custom_Prompt_13], ft.[Custom_Prompt_13]),
        [Custom_Prompt_14] = ISNULL(tt.[Custom_Prompt_14], ft.[Custom_Prompt_14]),
        [Custom_Prompt_15] = ISNULL(tt.[Custom_Prompt_15], ft.[Custom_Prompt_15]),
        [Custom_Prompt_16] = ISNULL(tt.[Custom_Prompt_16], ft.[Custom_Prompt_16]),
        RecordUpdateDateTime = CURRENT_TIMESTAMP
    FROM cms_dw.dbo.facttransaction ft
    INNER JOIN #CP_Codes tt ON ft.application_instance_id = 999 AND ft.traceno = tt.trace
    WHERE ft.ProcessingDate > DATEADD(dd, -5, GETDATE());

    -- Update the fact table based on application_instance_id = 999 and TraceNo_MBD
    UPDATE ft
    SET
        [Custom_Prompt_1] = ISNULL(tt.[Custom_Prompt_1], ft.[Custom_Prompt_1]),
        [Custom_Prompt_2] = ISNULL(tt.[Custom_Prompt_2], ft.[Custom_Prompt_2]),
        [Custom_Prompt_3] = ISNULL(tt.[Custom_Prompt_3], ft.[Custom_Prompt_3]),
        [StandardCoinBags] = ISNULL(tt.[StandardCoinBags], ft.[StandardCoinBags]),
        [NonStandardCoinBags] = ISNULL(tt.[NonStandardCoinBags], ft.[NonStandardCoinBags]),
        [WrappedCoinCount] = ISNULL(tt.[WrappedCoinCount], ft.[WrappedCoinCount]),
        [StandardStraps] = ISNULL(tt.[StandardStraps], ft.[StandardStraps]),
        [CoinBins] = ISNULL(tt.[CoinBins], ft.[CoinBins]),
        [HalfCoinBags] = ISNULL(tt.[HalfCoinBags], ft.[HalfCoinBags]),
        [Custom_Prompt_10] = ISNULL(tt.[Custom_Prompt_10], ft.[Custom_Prompt_10]),
        [Custom_Prompt_11] = ISNULL(tt.[Custom_Prompt_11], ft.[Custom_Prompt_11]),
        [Custom_Prompt_12] = ISNULL(tt.[Custom_Prompt_12], ft.[Custom_Prompt_12]),
        [Custom_Prompt_13] = ISNULL(tt.[Custom_Prompt_13], ft.[Custom_Prompt_13]),
        [Custom_Prompt_14] = ISNULL(tt.[Custom_Prompt_14], ft.[Custom_Prompt_14]),
        [Custom_Prompt_15] = ISNULL(tt.[Custom_Prompt_15], ft.[Custom_Prompt_15]),
        [Custom_Prompt_16] = ISNULL(tt.[Custom_Prompt_16], ft.[Custom_Prompt_16]),
        RecordUpdateDateTime = CURRENT_TIMESTAMP
    FROM cms_dw.dbo.facttransaction ft
    INNER JOIN #CP_Codes tt ON ft.application_instance_id = 999 AND ft.TraceNo_MBD = tt.trace
    WHERE ft.ProcessingDate > DATEADD(dd, -5, GETDATE());
    
    -- Update the ETL Control table
    BEGIN
        DELETE FROM GloryStaging.dbo.etl_control_table WHERE TableName = @TableName;

        INSERT INTO GloryStaging.dbo.etl_control_table (from_lsn, to_lsn, TableName)
        VALUES (@from_lsn, @to_lsn, @TableName);
    END;
END;
GO