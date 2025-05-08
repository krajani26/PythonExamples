USE [GloryStaging];
GO

-- *****************************************************************************************
-- Object: StoredProcedure [dbo].[GloryStaging_e_to_FactTransaction_TRANSDETAIL] 
-- Script Date: 3/25/2025 7:59:36 PM 
-- Description: This stored procedure processes changes from eVAS_TRANSDETAIL to update FactTransaction table.
-- *****************************************************************************************

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

ALTER PROCEDURE [dbo].[GloryStaging_e_to_FactTransaction_TRANSDETAIL]
AS
BEGIN
    SET NOCOUNT ON;
    SET DEADLOCK_PRIORITY HIGH;
    SET XACT_ABORT ON;

    DECLARE @from_lsn AS BINARY(10);
    DECLARE @to_lsn AS BINARY(10);
    DECLARE @Row_filter_option AS NVARCHAR(30) = N'all';
    DECLARE @TableName AS VARCHAR(20) = 'eVAS_TRANSDETAIL';

    -- Get the last processed LSN from the ETL control table
    SET @from_lsn = (SELECT MAX(to_lsn) FROM GloryStaging.dbo.etl_control_table WHERE tablename = @TableName);

    -- Get the current maximum LSN for the table
    SET @to_lsn = (SELECT [to_lsn] FROM GloryStaging.[dbo].[ETL_Control_Table] WHERE [TableName] = 'eVAS_Transactions');

    -- If no previous LSN, get the minimum LSN from the CDC table
    IF @from_lsn IS NULL
    BEGIN
        SET @from_lsn = sys.fn_cdc_get_min_lsn(@TableName);
    END;

    -- Create a temporary table for denomination amounts
    SELECT
        [TRACE],
        [US $1],
        [US $2],
        [US $5],
        [US $10],
        [US $20],
        [US $50],
        [US $100],
        [US 1c],
        [US 5c],
        [US 10c],
        [US 25c],
        [US 50c],
        [US 100c],
        [Loose Coin],
        [Checks],
        [Miscellaneous List],
        [Misc-1],
        [Misc-2],
        [Misc-3],
        [Misc-4],
        [Misc-5],
        [Mute $1],
        [Mute $2],
        [Mute $5],
        [Mute $10],
        [Mute $20],
        [Mute $50],
        [Mute $100]
    INTO #DenomAmount
    FROM (
        SELECT
            trace,
            ISNULL(a.amount, 0) / 100 AS amount,
            m.name
        FROM [GloryStaging].[cdc].[fn_cdc_get_all_changes_eVAS_TRANSDETAIL](@from_lsn, @to_lsn, @Row_filter_option) a
        INNER JOIN GloryStaging.[eVAS].[MEDIA] m ON a.[MEDIANO] = m.[MEDIANO]
        WHERE trace <> '332010159114158' --Excluding trace value '332010159114158'
    ) d
    PIVOT(
        MAX(amount) FOR name IN (
            [US $1], [US $2], [US $5], [US $10], [US $20], [US $50], [US $100], 
            [US 1c], [US 5c], [US 10c], [US 25c], [US 50c], [US 100c],
            [Loose Coin], [Checks], [Miscellaneous List], [Misc-1], [Misc-2], [Misc-3], 
            [Misc-4], [Misc-5], [Mute $1], [Mute $2], [Mute $5], [Mute $10], 
            [Mute $20], [Mute $50], [Mute $100]
        )
    ) AS DenomAmount;

    -- Update FactTransaction with denomination amounts
    UPDATE ft
    SET 
        [Misc1] = ISNULL([Misc-1], 0),
        [Misc2] = ISNULL([Misc-2], 0),
        [Misc3] = ISNULL([Misc-3], 0),
        [Misc4] = ISNULL([Misc-4], 0),
        [Misc5] = ISNULL([Misc-5], 0),
        [OtherMisc] = ISNULL([Misc-1], 0) + ISNULL([Misc-2], 0) + ISNULL([Misc-3], 0) + ISNULL([Misc-4], 0) + ISNULL([Misc-5], 0),
        [CurrencyTotal] = ISNULL([US $1], 0) + ISNULL([US $2], 0) + ISNULL([US $5], 0) + ISNULL([US $10], 0) + ISNULL([US $20], 0) + ISNULL([US $50], 0),
        [CoinTotal] = ISNULL([US 1c], 0) + ISNULL([US 5c], 0) + ISNULL([US 10c], 0) + ISNULL([US 25c], 0) + ISNULL([US 50c], 0) + ISNULL([US 100c], 0),
        [Bill_0001_00] = ISNULL([US $1], 0),
        [Bill_0002_00] = ISNULL([US $2], 0),
        [Bill_0005_00] = ISNULL([US $5], 0),
        [Bill_0010_00] = ISNULL([US $10], 0),
        [Bill_0020_00] = ISNULL([US $20], 0),
        [Bill_0050_00] = ISNULL([US $50], 0),
        [Bill_0100_00] = ISNULL([US $100], 0),
        [Mute_0001_00] = ISNULL([Mute $1], 0),
        [Mute_0002_00] = ISNULL([Mute $2], 0),
        [Mute_0005_00] = ISNULL([Mute $5], 0),
        [Mute_0010_00] = ISNULL([Mute $10], 0),
        [Mute_0020_00] = ISNULL([Mute $20], 0),
        [Mute_0050_00] = ISNULL([Mute $50], 0),
        [Mute_0100_00] = ISNULL([Mute $100], 0),
        [Coin_Loose] = ISNULL([Loose Coin], 0),
        [Coin_0000_01] = ISNULL([US 1c], 0),
        [Coin_0000_05] = ISNULL([US 5c], 0),
        [Coin_0000_10] = ISNULL([US 10c], 0),
        [Coin_0000_25] = ISNULL([US 25c], 0),
        [Coin_0000_50] = ISNULL([US 50c], 0),
        [Coin_0001_00] = ISNULL([US 100c], 0),
        [Checks] = ISNULL(m.[Checks], 0)
    FROM cms_dw.dbo.facttransaction ft
    INNER JOIN #DenomAmount m ON ft.application_instance_id = 999 AND ft.traceno = m.trace;

    DROP TABLE #DenomAmount;

    -- Create a temporary table for denomination counts
    SELECT
        [TRACE],
        [US $1],
        [US $2],
        [US $5],
        [US $10],
        [US $20],
        [US $50],
        [US $100],
        [US 1c],
        [US 5c],
        [US 10c],
        [US 25c],
        [US 50c],
        [US 100c],
        [Loose Coin],
        [Checks],
        [Miscellaneous List],
        [Misc-1],
        [Misc-2],
        [Misc-3],
        [Misc-4],
        [Misc-5],
        [Mute $1],
        [Mute $2],
        [Mute $5],
        [Mute $10],
        [Mute $20],
        [Mute $50],
        [Mute $100]
    INTO #DenomCount
    FROM (
        SELECT
            trace,
            ISNULL(a.PIECE, 0) AS PIECE,
            m.name
        FROM [GloryStaging].[cdc].[fn_cdc_get_all_changes_eVAS_TRANSDETAIL](@from_lsn, @to_lsn, @Row_filter_option) a
        INNER JOIN GloryStaging.[eVAS].[MEDIA] m ON a.[MEDIANO] = m.[MEDIANO]
        WHERE trace <> '332010159114158' --Excluding trace value '332010159114158'
    ) d
    PIVOT(
        MAX(PIECE) FOR name IN (
            [US $1], [US $2], [US $5], [US $10], [US $20], [US $50], [US $100], 
            [US 1c], [US 5c], [US 10c], [US 25c], [US 50c], [US 100c],
            [Loose Coin], [Checks], [Miscellaneous List], [Misc-1], [Misc-2], [Misc-3], 
            [Misc-4], [Misc-5], [Mute $1], [Mute $2], [Mute $5], [Mute $10], 
            [Mute $20], [Mute $50], [Mute $100]
        )
    ) AS DenomCount;

    -- Update FactTransaction with denomination counts
    UPDATE ft
    SET
        [note_count_1] = ISNULL([US $1], 0) + ISNULL([Mute $1], 0),
        [note_count_2] = ISNULL([US $2], 0) + ISNULL([Mute $2], 0),
        [note_count_5] = ISNULL([US $5], 0) + ISNULL([Mute $5], 0),
        [note_count_10] = ISNULL([US $10], 0) + ISNULL([Mute $10], 0),
        [note_count_20] = ISNULL([US $20], 0) + ISNULL([Mute $20], 0),
        [note_count_50] = ISNULL([US $50], 0) + ISNULL([Mute $50], 0),
        [note_count_100] = ISNULL([US $100], 0) + ISNULL([Mute $100], 0),
        [CheckCnt] = CASE WHEN ft.Checks = 0 THEN 0 ELSE ISNULL(trd_Notes.Checks, 0) END,
        [LoosePiecesCntTotal] = ISNULL(trd_Notes.[US $1], 0) + ISNULL(trd_Notes.[US $2], 0) + ISNULL(trd_Notes.[US $5], 0) + ISNULL(trd_Notes.[Mute $1], 0) + ISNULL(trd_Notes.[Mute $2], 0) + ISNULL(trd_Notes.[Mute $5], 0) + ISNULL(trd_Notes.[Mute $10], 0)
    FROM cms_dw.dbo.facttransaction ft
    INNER JOIN #DenomCount trd_Notes ON ft.application_instance_id = 999 AND ft.traceno = trd_Notes.trace;

    DROP TABLE #DenomCount;

    -- Update the ETL control table
    DELETE FROM GloryStaging.dbo.etl_control_table WHERE TableName = @TableName;

    INSERT INTO GloryStaging.dbo.etl_control_table (from_lsn, to_lsn, TableName)
    VALUES (@from_lsn, @to_lsn, @TableName);