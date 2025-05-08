USE [Glorystaging]
GO
/ ****** object: Storedprocedure [dbo]. [Glorystaging_e_to_FactActivityReportStartEndBalance] Script Date: 4/22/2025 8:09:55 PM ****** /
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--==============================================================================================
-- Author: Rich Fox
--Create date: 9/8/2014
--Description: <Description, ,>
--==============================================================================================

ALTER PROCEDURE [dbo]. [GloryStaging_e_to_FactActevityReportStartEndBalance]
-- Add the parameters for the stored procedure here
AS

BEGIN
-- SET NOCOUNT ON added to prevent extra result sets from
-- interfering with SELECT statements.
SET NOCOUNT ON;
SET DEADLOCK_PRIORITY HIGH;

DECLARE @from_lsn AS BINARY(10)
DECLARE @to_lsn AS BINARY(10) = (select [to_lsn] from GloryStaging. [dbo].[ETL_Control_Table] where [TableName] = 'max_Tablevalue' )
DECLARE @Row_filter_option AS nvarchar(30) = N'all'

-- Set to the source table name
DECLARE @TableName AS varchar(20) = 'eVAS_ACTSTINVDTL'

-- Looking for the last update of the table
set @from_lsn = (select max(to_lsn) from GloryStaging.dbo.etl_control_table where tablename = @TableName)

-- Checking if the last update is blank, if so setting it to the oldest date in the capture tables.
if @from 1sn is null

if @from_lsn is null
begin
set @from_lsn = sys.fn_cdc_get_min_lsn(@TableName)
End

-- Start of New Part 9/25/17 R Fox

DECLARE @from_lsn_1 AS BINARY(10)
DECLARE @to_lsn_1 AS BINARY(10) = (select [to_lsn] from GloryStaging. [dbo].[ETL_Control_Table] where [TableName] = 'max_Tablevalue' )


-- Set to the source table name
DECLARE @TableName_1 AS varchar(20) = 'eVAS_ACTIVITYDATES'

-- Looking for the last update of the table
set @from_lsn_1 = (select max(to_lsn) from Glorystaging.dbo.etl_control_table where tablename = @TableName_1)

-- Checking if the last update is blank, if so setting it to the oldest date in the capture tables.
if @from_lsn_1 is null
begin
set @from_lsn_1 = sys.fn_cdc_get_min_lsn(@TableName_1)
End

Declare @ClosedDates Table
(CLIENTNO Int, SITENO int, CLOSEDDATE date)

Insert into @closedDates
Select distinct CLIENTNO, SITENO, CLOSEDDATE
From Glorystaging.cdc. [fn_cdc_get_net_changes_eVAS_ACTSTINVDTL](@from_lsn, @to_lsn, N'all')

-- insert into @ClosedDates values
-- (2809, 1570, '2019-05-10')

-- Insert into @closedDates
-- Select 4687, 1120, '2018-08-06'

Insert into @closedDates
Select a.CLIENTNO, a.SITENO, GloryStaging.dbo.GetLastActivityDate(a.[CLIENTNO], a.[SITENO],a.[CLOSEDDATE])
from Glorystaging.cdc.fn_cdc_get_net_changes_eVAS_ACTIVITYDATES (@from_lsn_1, @to_lsn_1, @Row_filter_option) a
left join @ClosedDates cd
on a. [CLIENTNO] = cd.[CLIENTNO]
and a. [SITENO] = cd.[SITENO]
and GloryStaging.dbo.GetLastActivityDate(a.[CLIENTNO], a.[SITENO],a.[CLOSEDDATE]) = cd.[CLOSEDDATE]
where cd.CLIENTNO is null

-- End of New Part 9/25/17 R FOX
-- select * from @ClosedDates

Select distinct
v.ClientNO as MVID,
case when v. [SITENO] = 1 then 9999 else v.[SITENO] end as BranchID,
V.CLOSEDDATE,
v.BalancestartEnd,
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
[checks],
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
[Mute $100],
[US $1]+[US $2]+[US $5]+[US $10]+[US $20]+[US $50]+[US $100]+[US 1c]+[US 5c]+[US 10c]+[US 25c]+[US 50c]+[US 100c]+[Loose Coin]+[Checks]+[Miscellaneous List]+[Misc-1] as TotalAmt,
[US 1c]+[US 5c]+[US 10c]+[US 25c]+[ys 50c]+[US 100c]+[Loose Coin] as Totalcoin,
[US $1]+[US $2]+[US $5]+[US $10]+[Us $20]+[US $50]+[US $100] as TotalCash,
999 as Application_instance_ID,
dar.DimActivityReportID,
dar.DimLoomisCustMltVltID,
dm.DimBranchID,
fas.FactActivityReportID
Into #tempSEB
FROM GloryStaging.dbo.vwStartEndBalance v
inner join @closedDates c
on v. [CLIENTNO]= c.[CLIENTNO]
and v. [SITENO] = c.[SITENO]
and v. [CLOSEDDATE] = c.[CLOSEDDATE]
inner join CMS_DW.dbo.DimActivityReport dar
on v.clientno = dar.MVID
and v.siteno = dar.BranchID
and dar.Application_Instance_ID = 999
and v.CLOSEDDATE = dar.ActRpt_Date_To
Inner join CMS_DW.dbo.DimLoomisCustomerMultiVault dm
on dar. DimLoomisCustmltvltID = dm.DimLoomisCustMltvltID
Left Join CMS_DW.dbo. FactActivityReportStartEndBalance fas
on fas.DimActivityReportID = dar.DimActivityReportID
and fas.Balance_Starting_Ending = v.BalanceStartEnd
where not C.ClientNO is null

UPDATE f
SET [TotalAmt] = t.TotalAmt
, [CurrencyTotal] = t.TotalCash
, [CoinTotal] = t.TotalCoin
, [checkAmt] = t.[checks]
,[Bill_0001_00] = t.[US $1]
,[Bill_0002_00] = t.[US $2]
,[Bill_0005_00] = t.[US $5]
,[Bill_0010_00] = t.[US $10]
,[Bill_0020_00] = t.[US $20]
,[Bill_0050_00] = t.[US $50]
,[Bill_0100_00] = t.[US $100]
,[Mute_0001_00] = t.[Mute $1]
, [Mute_0002_00] = t.[Mute $2]
,[Mute_0005_00] = t.[Mute $5]
,[Mute_0010_00] = t.[Mute $10]
,[Mute_0020_00] = t.[Mute $20]
,[Mute_0050_00] = t.[Mute $50]
,[Mute_0100_00] = t.[Mute $100]
,[Coin_Loose] = [Loose Coin]
,[Coin_0000_01] = [US 1c]
,[Coin_0000_05] = [US 5c]
,[Coin_0000_10] = [US 10c]
,[Coin_0000_25] = [US 25c]
,[Coin_0000_50] = [US 50c]
,[Coin_0001_00] = [US 100c]
, [RecordUpdateDateTime] = getdate()
from [CMS_DW].dbo. [FactActivityReportStartEndBalance] f
inner join #tempseb t on f.FactActivityReportID = t.FactActivityReportID

INSERT INTO [CMS_DW].dbo. [FactActivityReportstartEndBalance]
([DimActivityReportID]
,[DimBranchID]
,[DimLoomisCustMltv1tID]
,[Balance_Starting_Ending]
,[BranchID]
,[MVID]
,[TotalAmt]
,[CurrencyTotal]
,[CoinTotal]
, [CheckAmt]
Select
,[CheckAmt]
,[Bill_0001_00]
,[Bill_0002_00]
,[Bill_0005_00]
,[Bill_0010_00]
,[Bill_0020_00]
,[Bill_0050_00]
,[Bill_0100_00]
,[Mute_0001_00]
,[Mute_0002_00]
,[Mute_0005_00]
,[Mute_0010_00]
,[Mute_0020_00]
,[Mute_0050_00]
,[Mute_0100_00]
,[Coin_Loose]
,[Coin_0000_01]
,[Coin_0000_05]
,[Coin_0000_10]
,[Coin_0000_25]
,[Coin_0000_50]
,[Coin_0001_00]
,[RecordcreateDateTime]
,[RecordUpdateDateTime]
,[Application_Instance_ID])
,DimActivityReportID
,DimBranchID
,DimLoomisCustMltvltID
,BalanceStartEnd
,BranchID
,MVID
,TotalAmt
,TotalCash
,TotalAmt
,Totalcash
,TotalCoin
,Checks
,[US $1],
[US $2],
[uS $5]
[US $10],
[US $20],
[US $50],
[US $100],
[Mute $1],
[Mute $2],
[Mute $5],
[Mute $10],
[Mute $20],
[Mute $50],
[Mute $100]
,[Loose Coin]
,[US 1c]
,[US 5c]
,[US 10c]
,[US 25c]
,[US 50c]
,[US 100c]
>getdate()
,getdate()
,Application_Instance_ID
from #tempSEB
where FactActivityReportID is null


BEGIN

DELETE from GloryStaging.dbo.etl_control_table where TableName = @TableName
Insert into Glorystaging.dbo.etl_control_table (from_lsn, to_lsn, TableName) values (@from_lsn, @to_lsn,@TableName)
-- END

end