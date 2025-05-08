ALTER PROCEDURE [dbo].[GloryStaging_e_to_FactTransaction_Orders]
AS
BEGIN
	SET NOCOUNT ON;
	SET DEADLOCK_PRIORITY HIGH;
	SET XACT_ABORT ON;

	DECLARE @from_lsn AS BINARY (10);
	DECLARE @to_lsn AS BINARY (10) = (
			SELECT [to_lsn]
			FROM GloryStaging.[dbo].[ETL_Control_Table]
			WHERE [TableName] = 'max_Tablevalue'
			);
	DECLARE @Row_filter_option AS NVARCHAR(30) = N'all';
	DECLARE @TableName AS VARCHAR(20) = 'eVAS_Orders';
	SET @from_lsn = (
			SELECT MAX(to_lsn)
			FROM GloryStaging.dbo.etl_control_table
			WHERE tablename = @TableName
			);
	IF @from_lsn IS NULL
	BEGIN
		SET @from_lsn = sys.fn_cdc_get_min_lsn(@TableName);

		DECLARE @Orders TABLE (
			ORDERNO BIGINT
			,SITENO INT
			);

		INSERT INTO @Orders
		SELECT DISTINCT ORDERID
			,SITENO
		FROM GloryStaging.cdc.[fn_cdc_get_net_changes_eVAS_CUOOrder](@from_lsn, @to_lsn, @Row_filter_option)
		WHERE ORDERID NOT IN (
				SELECT ORDERNO
				FROM @Orders
				);

		INSERT INTO @Orders
		SELECT DISTINCT ORDERID
			,SITENO
		FROM GloryStaging.cdc.[fn_cdc_get_net_changes_eVAS_CUOOrderItem](@from_lsn, @to_lsn, @Row_filter_option)
		WHERE ORDERID NOT IN (
				SELECT ORDERNO
				FROM @Orders
				);

		INSERT INTO @Orders
		SELECT DISTINCT BROWSERPACKORD
			,SITENO
		FROM GloryStaging.cdc.[fn_cdc_get_net_changes_eVAS_TRANSACTIONS](@from_lsn, @to_lsn, @Row_filter_option)
		WHERE BROWSERPACKORD IS NOT NULL
			AND BROWSERPACKORD <> 0
			AND BROWSERPACKORD NOT IN (
				SELECT ORDERNO
				FROM @Orders
				);
				
		INSERT INTO @Orders
		SELECT DISTINCT [ORDERNO]
			,SITENO
		FROM GloryStaging.[cdc].[fn_cdc_get_net_changes_eVAS_ORDERDEBITSITE](@from_lsn, @to_lsn, @Row_filter_option)
		WHERE ORDERNO NOT IN (
				SELECT ORDERNO
				FROM @Orders
				);
	
		INSERT INTO @Orders
		SELECT traceno
			,Branchid
		FROM cms_dw.dbo.FactTransaction
		WHERE DimTransactionTypeID = 1
			AND IsOffset = 0
			AND DimAccntLocID IS NULL
			AND ProcessingDate > DATEADD(DD, - 1, GETDATE());
			
		SELECT DISTINCT 
			tt.[CLIENTNO]
			,tt.[SITENO]
			,tt.[ORDERNO]
			,tt.[ActivityDate]
			,tt.[PASSTHRU]
			,tt.[ORDERDATE]
			,tt.[ORDERTIME]
			,tt.[ACCOUNTNO]
			,tt.[RTNO]
			,tt.[LOCATIONID]
			,tt.[ORDERBY]
			,tt.[GTOTAL]
			,tt.[MEDIACOUNT]
			,tt.[SHIPDATE]
			,tt.[DELIVERYDATE]
			,tt.[ROUTENO]
			,tt.[COURIERID]
			,tt.[FIRSTSHIPDATE]
			,tt.[LASTSHIPDATE]
			,tt.[MANIFESTDATE]
			,tt.[CANCELDATE]
			,tt.[UPLOADDATE]
			,tt.[UPLOADTIME]
			,tt.[SEALID]
			,tt.[CANCELBY]
			,tt.[ITEMS]
			,CASE 
				WHEN tt.[ENTRYTYPE] = 5
					AND ot.[order_source] = 'ICO'
					THEN 6
				ELSE tt.[ENTRYTYPE]
				END AS [ENTRYTYPE]
			,CASE 
				WHEN tt.[SUBTYPE] > 10
					THEN 0
				ELSE tt.[SUBTYPE]
				END AS [SUBTYPE]
			,tt.[POSTDATE]
			,tt.[C4CTRACE]
			,tt.[Bill_0001_00]
			,tt.[Bill_0002_00]
			,tt.[Bill_0005_00]
			,tt.[Bill_0010_00]
			,tt.[Bill_0020_00]
			,tt.[Bill_0025_00]
			,tt.[Bill_0050_00]
			,tt.[Bill_0075_00]
			,tt.[Bill_0100_00]
			,tt.[Bill_0200_00]
			,tt.[Bill_0500_00]
			,tt.[Bill_1000_00]
			,tt.[Muta A0A1 AA1]
			,tt.[Mute_0001_00]
			,tt.[Mute_0002_00]
			,tt.[Mute_0005_00]
			,tt.[Mute_0010_00]
			,tt.[Mute_0020_00]
			,tt.[Mute_0025_00]
			,tt.[Mute_0050_00]
			,tt.[Mute_0075_00]
			,tt.[Mute_0100_00]
			,tt.[Mute_0200_00]
			,tt.[Mute_0500_00]
			,tt.[Mute_1000_00]
			,tt.[Coin_Loose]
			,tt.[Coin_0000_01]
			,tt.[Coin_0000_05]
			,tt.[Coin_0000_10]
			,tt.[Coin_0000_25]
			,tt.[Coin_0000_50]
			,tt.[Coin_0000_75]
			,tt.[Coin_0001_00]
			,tt.[Roll_0000_01]
			,tt.[Roll_0000_05]
			,tt.[Roll_0000_10]
			,tt.[Roll_0000_25]
			,tt.[Roll_0000_50]
			,tt.[Roll_0000_75]
			,tt.[Roll_0001_00]
			,tt.[FS_0001_00]
			,tt.[FS_0005_00]
			,tt.[FS_0010_00]
			,tt.[Checks]
			,tt.[FS_Loose]
			,tt.[Checks]
			,tt.[FS_Loose]
			,tt.[Misc-1] AS misc1
			,tt.[Misc-2] AS misc2
			,tt.[Misc-3] AS misc3
			,tt.[Misc-4] AS misc4
			,tt.[Misc-5] AS misc5
			,CashAdd
			,CAST(tt.[note_count_1] AS INT) AS [note_count_1]
			,CAST(tt.[note_count_2] AS INT) AS [note_count_2]
			,CAST(tt.[note_count_5] AS INT) AS [note_count_5]
			,CAST(tt.[note_count_10] AS INT) AS [note_count_10]
			,CAST(tt.[note_count_20] AS INT) AS [note_count_20]
			,CAST(tt.[note_count_50] AS INT) AS [note_count_50]
			,CAST(tt.[note_count_100] AS INT) AS [note_count_100]
			,ft.FactTransactionID
			,dar.dimactivityreportid
			,dm.[DimBranchID]
			,da.[DimAccntID]
			,dal.[DimAccntLocID]
			,dm.[DimLoomisCustMltVltID]
			,dd_prd.dimdateid AS DimDateID_Proc
			,dd_locd.dimdateid AS DimDateID_Del
			,dd_crd.DimDateID AS DimDateID_Ord
			,dt_cr.DimTimeID AS DimTimeID_Ord
			,dd_ctd.DimDateID AS DimDateID_Ctd
			,tt.[StandardCoinBags]
			,tt.[HalfCoinBags]
			,da.MiscID2
			,dal.locationnumber
			,tt.[ATMTRACE]
			,tt.Rolled_Penny
			,tt.Rolled_Nickel
			,tt.Rolled_Dime
			,tt.Rolled_Quarter
			,tt.Rolled_Half
			,tt.Rolled_Dollar
			,tt.Boxed_Penny
			,tt.Boxed_Nickel
			,tt.Boxed_Dime
			,tt.Boxed_Quarter
			,tt.Boxed_Half
			,tt.Boxed_Dollar
			,tt.Bag_Penny
			,tt.Bag_Nickel
			,tt.Bag_Dime
			,tt.Bag_Quarter
			,tt.Bag_Half
			,tt.Bag_Dollar
			,tt.Half_Bag_Penny
			,tt.Half_Bag_Nickel
			,tt.Half_Bag_Dime
			,tt.Half_Bag_Quarter
			,tt.Half_Bag_Half
			,tt.Half_Bag_Dollar
			,tt.PENDING
			,'ORDER' AS ProcessingMode
			,CASE 
				WHEN CANCELDATE IS NOT NULL
					THEN 9
				WHEN pending = 1
					THEN - 1
				WHEN tt.ActivityDate IS NOT NULL
					THEN 5
				WHEN tt.[MANIFESTDATE] IS NOT NULL
					THEN 3
				WHEN tt.firstshipdate IS NOT NULL
					THEN 2
				ELSE 0
				END AS Mode
		INTO #tempOrders
		FROM GloryStaging.[dbo].vwOrderDetails_1 tt
		INNER JOIN @Orders t ON tt.ORDERNO = t.ORDERNO
			AND tt.SITENO = t.SITENO
		LEFT JOIN CMS_DW.dbo.FactTransaction ft ON tt.ORDERNO = ft.traceno
			AND ft.Application_Instance_ID = 999
			AND ft.DimTransactionTypeID = 1
		LEFT JOIN CMS_DW.dbo.Dimaccountlocation dal ON tt.[LocationNumber] = dal.LocationNumber
			AND tt.CLIENTNO = dal.mvid
			AND tt.siteno = dal.branchid
			AND dal.[Application_Instance_ID] = 999
		LEFT JOIN CMS_DW.dbo.DimAccount da ON dal.dimaccntid = da.dimaccntid
		LEFT JOIN CMS_DW.dbo.DimLoomisCustomerMultiVault dm ON dm.branchid = tt.siteno
			AND dm.mvid = tt.clientno
			AND dm.[Application_Instance_ID] = 999
		LEFT JOIN cms_dw.dbo.DImActivityReport dar ON dm.[DimLoomisCustMltVltID] = dar.[DimLoomisCustMltVltID]
			AND dar.[ActRpt_Date_To] = [ActivityDate]
		LEFT JOIN cms_dw.dbo.DImDate dd_prd ON tt.[SHIPDATE] = dd_prd.Full_Date
		LEFT JOIN cms_dw.dbo.DImDate dd_locd ON tt.[DELIVERYDATE] = dd_locd.Full_Date
		LEFT JOIN cms_dw.dbo.DImDate dd_crd ON tt.[ORDERDATE] = dd_crd.Full_Date
		LEFT JOIN cms_dw.dbo.DimTime dt_cr ON CAST(tt.[ORDERTIME] AS TIME) = dt_cr.FullTime
		LEFT JOIN cms_dw.dbo.DImDate dd_ctd ON tt.[POSTDATE] = dd_ctd.Full_Date
		LEFT JOIN [GloryStaging].[dbo].[order_trans] ot ON tt.ORDERNO = ot.source_order_no
		OPTION (RECOMPILE);
	
		DELETE
		FROM #tempOrders
		WHERE [CANCELDATE] < DATEADD(dd, - 30, GETDATE())
			OR [DELIVERYDATE] < DATEADD(dd, - 60, GETDATE());
		
		BEGIN TRAN;
		UPDATE f
		SET [DimEmployeeID_Teller] = NULL
			,[DimAccntID] = t.[DimAccntID]
			,[DimActivityReportID] = t.[DimActivityReportID]
			,[DimAccntLocID] = t.[DimAccntLocID]
			,[DimBillingOrderTypeID] = ISNULL([SUBTYPE], 1)
			,[DimBillingTransTypeID] = CASE 
				WHEN MiscID2 LIKE 'FED%'
					AND ([CLIENTNO] <> 1)
					AND [CANCELDATE] IS NULL
					THEN 3
				ELSE 2
				END
			,[DepositID] = NULL
			,[IsOffset] = CASE 
				WHEN [CANCELDATE] IS NULL
					OR ActivityDate IS NOT NULL
					THEN 0
				ELSE 1
				END
			,[OrderCashAdd] = ISNULL(CashAdd, 0)
			,[OrderCashAdd] = ISNULL(CashAdd, 0)
			,[OrderExchange] = CASE 
				WHEN CashAdd = 1
					THEN 0
				ELSE 1
				END
			,[EmployeeNumber] = 0
			,[OrdTrace] = [ATMTRACE]
			,[OrdPassThruLong] = [PASSTHRU]
			,[OrdC4CTrace] = [C4CTRACE]
			,[RecordUpdateDateTime] = GETDATE()
			,[Memo] = NULL
			,Order_Penging = PENDING
			,CRVDate = CASE 
				WHEN ActivityDate IS NOT NULL
					THEN [CANCELDATE]
				ELSE NULL
				END
			,Mode = t.Mode
			,OrderType = t.ENTRYTYPE
		FROM cms_dw.dbo.FactTransaction AS f
		INNER JOIN #tempOrders t ON f.FactTransactionID = t.FactTransactionID;

		INSERT INTO cms_dw.dbo.FactTransaction (
			[FactTransaction_NatKey]
			,[DimTransactionTypeID]
			,[DimBranchID]
			,[DimAccntID]
			,[DimAccntLocID]
			,[DimLoomisCustMltVltID]
			,[DimEmployeeID_Teller]
			,[DimCurrencyID]
			,[DimActivityReportID]
			,DimBillingOrderTypeID
			,Half_Bag_Dollar
			,Order_Penging
			,ProcessingMode
			,Mode
			)
		SELECT [ORDERNO]
			,1 
			,[DimBranchID]
			,[DimAccntID] 
			,[DimAccntLocID]
			,[DimLoomisCustMltVltID]
			,NULL
			,1
			,[DimActivityReportID]
			,ISNULL([SUBTYPE], 1)
			,CASE 
				WHEN (
						note_count_1 / 100 + note_count_2 / 100 + note_count_5 / 100 + note_count_10 / 100 + note_count_20 / 100 + note_count_50
						AND Coin_0000_01 % 25 = 0
						AND Coin_0000_05 % 100 = 0
						AND Coin_0000_10 % 250 = 0
						AND Coin_0000_25 % 500 = 0
						AND Coin_0000_50 % 500 = 0
						AND Coin_0001_00 % 1000 = 0 ) THEN 1 ELSE 2 END
						,CASE 
							WHEN MiscID2 LIKE 'FED%'
								AND ([CLIENTNO] <> 1)
								AND [CANCELDATE] IS NULL
								THEN 3
							ELSE 2
							END
						,CASE 
							WHEN MiscID2 LIKE 'FED%'
								AND ([CLIENTNO] <> 1)
								AND [CANCELDATE] IS NULL
								THEN 3
							ELSE 2
							END
						,[SITENO]
						,[CLIENTNO]
						,[ORDERNO]
						,NULL
						,CASE 
							WHEN [CANCELDATE] IS NULL
								THEN 0
							ELSE 1 [ ENTRYTYPE]
								,0. ISNULL(CashAdd, 0)
								,CASE 
									WHEN CashAdd = 1
										THEN 0
									ELSE 1
									END;
								,NULL
								,[GTOTAL] / 100
								,[GTOTAL] / 100
								,[Bill_0001_00] + [Bill_0002_00] + [Bill_0005_00] + [Bill_0010_00] + [Bill_0020_00] + [Bill_0050_00] + [Bill_0100_00]
								,NULL
								,[GTOTAL] / 100
								,[GTOTAL] / 100
								,[Bill_0001_00] + [Bill_0002_00] + [Bill_0005_00] + [Bill_0010_00] + [Bill_0020_00] + [Bill_0050_00] + [Bill_0100_00]
								,[Bill_0001_00] + [Bill_0002_00] + [Bill_0005_00] + [Bill_0010_00] + [Bill_0020_00] + [Bill_0050_00] + [Bill_0100_00]
								,[Coin_0000_01] + [Coin_0000_05] + [Coin_0000_10] + [Coin_0000_25] + [Coin_0001_00] + [Coin_0000_50] + [Coin_Loose] + 
								[Roll_0000_01] + [Bill_0001_00] + [Bill_0002_00] + [Bill_0005_00] + [Bill_0010_00] + [Bill_0020_00] + 
								[Bill_0050_00] + [Bill_0100_00] + [Coin_0000_01] + [Misc1] + [Misc2] + [Misc3] + [Misc4] + [Misc5]
								,0
								,0
								,[GTOTAL] / 100
								,0
								,0
								,0
								,0
								,0
								,[Coin_0000_01] * 10 / 5 + [Coin_0000_05] / 2 + [Coin_0000_10] / 5 + [Coin_0000_25] / 10 + [Coin_0000_50] / 10 + [Coin_0001_00] / 25
								,LocationNumber
								,'USA'
								,'Dollar'
								,0
								,[SHIPDATE]
								,[ORDERDATE]
								,[ORDERTIME]
								,[POSTDATE]
								,[DELIVERYDATE]
								,[ORDERDATE]
								,[ORDERTIME]
								,[SHIPDATE]
								,[DELIVERYDATE]
								,DimDateID_Proc
								,DimDateID_Ord
								,[DELIVERYDATE]
								,[ORDERDATE]
								,[ORDERTIME]
								,[SHIPDATE]
								,[DELIVERYDATE]
								,DimDateID_Proc
								,DimDateID_Ord
								,DimTimeID_Ord
								,DimDateID_Ctd
								,DimDateID_Del
								,DimDateID_Ord
								,DimTimeID_Ord
								,DimDateID_Proc
								,DimDateID_Del
								,ISNULL([Bill_0001_00], 0)
								,ISNULL([Bill_0002_00], 0)
								,ISNULL([Bill_0005_00], 0)
								,ISNULL([Bill_0010_00], 0)
								,ISNULL([Bill_0020_00], 0)
								,ISNULL([Bill_0025_00], 0)
								,ISNULL([Bill_0050_00], 0)
								,ISNULL([Bill_0075_00], 0)
								,ISNULL([Bill_0100_00], 0)
								,ISNULL([Bill_0200_00], 0)
								,ISNULL([Bill_0500_00], 0)
								,ISNULL([Bill_1000_00], 0)
								,ISNULL([Mute_0001_00], 0)
								,ISNULL([Mute_0002_00], 0)
								,ISNULL([Mute_0005_00], 0)
								,ISNULL([Mute_0010_00], 0)
								,ISNULL([Mute_0020_00], 0)
								,ISNULL([Mute_0025_00], 0)
								,ISNULL([Mute_0050_00], 0)
								,Half_Bag_Penny
								,Half_Bag_Nickel
								,Half_Bag_Dime
								,Half_Bag_Quarter
								,Half_Bag_Half
								,Half_Bag_Dollar
								,PENDING
								,ProcessingMode
								,Mode 
								FROM #tempOrders 
								WHERE FactTransactionID IS NULL
								AND [Bill_0001_00] IS NOT NULL;
								
								DROP TABLE #tempOrders;
								
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
									) :

								-- END
								COMMIT TRAN;
								END;