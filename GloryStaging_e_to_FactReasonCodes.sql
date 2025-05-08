USE [Glorystaging]
GO
/ ****** object: StoredProcedure [dbo].[GloryStaging_e_to_FactReasonCodes] Script Date: 4/22/2025 8:19:06 PM ****** /
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

======================================================================================================================
-- Author: Rich Fox
-- Create date: 4/29/2015
-- Description: <Description,,>
======================================================================================================================
ALTER PROCEDURE [dbo]. [Glorystaging_e_to_FactReasonCodes] -- Add the parameters for the stored procedure here
AS
BEGIN
-- SET NOCOUNT ON added to prevent extra result sets from
-- interfering with SELECT statements.
SET NOCOUNT ON;
SET DEADLOCK_PRIORITY HIGH;

DECLARE @from_1sn AS BINARY(10)
DECLARE @to_lsn AS BINARY(10) = (select [to_lsn] from GloryStaging. [dbo].[ETL_Control_Table] where [TableName] = 'eVAS_Transactions')
DECLARE @Row_filter_option AS nvarchar(30) = N'all'

-- Set to the source table name
DECLARE @TableName AS varchar(20) = 'eVAS_ERRORDTL'

-- Looking for the last update of the table
set @from_lsn = (select max(to_lsn) from GloryStaging.dbo.etl_control_table where tablename = @TableName)

-- Checking if the last update is blank, if so setting it to the oldest date in the capture tables.
if @from_lsn is null
begin
set @from_lsn = sys.fn_cdc_get_min_lsn(@TableName)
End

SELECT ed. [TRACE] as TraceNo
,ed. [TYPE]
,ed.[ERRORID]
,CASE _$operation
WHEN 1 then 0
else ed. [COUNT]
end as [COUNT]
,t.CLIENTNO as MVID
,case when t. [SITENO] = 1 then 9999 else t.[SITENO] end as BranchID
,ef.LABEL as Description
,dc.DimReasonCodeID
,999 as Application_Instance_ID
,FactReasonCodeID
,dc.ReasonCodeID
into #TempReasonsDep
FROM Glorystaging. [eVAS]. [ERRORDTL] ed
inner join GloryStaging. [eVAS]. [TRANSACTIONS] t on ed.TRACE = t.TRACE
inner join Glorystaging. [eVAS]. [ERRORDEF] ef on ed. ERRORID = ef.[ERRORNO] and t.clientno = ef.Clientno and ed. TYPE = ef. TYPE
inner join CMS_DW. dbo. DimReasonCode dc on ed. TYPE = dc. Type and dc.Application_Instance_ID = 999 and t.CLIENTNO = dc.` and ed.ERRORID = dc.ReasonCodeID
inner join GloryStaging. [cdc]. [fn_cdc_get_net_changes_eVAS_ERRORDTL](@from_lsn, @to_lsn, N'all') ZZZ on zzz. trace = ed.trace and zzz.ERRORID = ed.ERRORID and zzz.type = ed.type
left join [CMS_DW]. [dbo]. [FactReasonCode] fr on fr.TraceNo = ed. TRACE and fr.DimReasonCodeID = dc.DimReasonCodeID
where ed. [TYPE] = 'P'


DELETE FROM [CMS_DW]. [dbo].[FactReasonCode]
where [FactReasonCodeID] in (select FactReasonCodeID from #TempReasonsDep where COUNT = 0)

insert into [CMS_DW]. [dbo]. [FactReasonCode]
([FactTransactionID]
, [DimReasonCodeID]
, [BranchID]
,[MVID]
,[TraceNo]
,[ReasonCodeID]
,[Type]
, [Name]
, [RecordCreateDateTime]
, [RecordUpdateDateTime]
, [RecordCreateAuditPackageExecutionId]
, [RecordUpdateAuditPackageExecutionId]
,[Application_Instance_ID])
SELECT isnull(ft.FactTransactionID, ft1.FactTransactionID)
,t. [DimReasonCodeID]
,t.BranchID
,t.MVID
,case when ft. FactTransactionID is not null then t. TraceNo else ft1. TraceNo end
,t.ReasonCodeID
,t. TYPE
,t.Description
,Getdate()
,Getdate()
.0
,0
,999
from #TempReasonsDep t
left join CMS_DW.dbo. FactTransaction ft on t. TraceNo = ft.traceno and t.Application_Instance_ID = ft.Application_Instance_ID
left join CMS_DW.dbo. FactTransaction ft1 on t. TraceNo = ft1. TraceNo_MBD and t.Application_Instance_ID = ft1.Application_Instance_ID
where TYPE = 'P' and COUNT <> 0 and FactReasonCodeID is null

DROP table #TempReasonsDep

SELECT fte. TraceNo as TraceNo
,ed. [TYPE]
,ed. [ERRORID]
,CASE $operation
WHEN 1 then 0
else ed. [COUNT]
end as [COUNT]
,t.CLIENTNO as MVID
,case when t. [SITENO] = 1 then 9999 else t.[SITENO] end as BranchID
,ef.LABEL as Description
,dc.DimReasonCodeID
,999 as Application_Instance_ID
, FactEnvReasonCodeID
,dc.ReasonCodeID
,fte. FactTransactionID
,fte. FactTransactionEnvelopeID
,fte.Envelope_Number
into #TempReasonsEnv
FROM GloryStaging. [eVAS].[ERRORDTL] ed
inner join Glorystaging. [eVAS].[TRANSACTIONS] t on ed. TRACE = t.TRACE
inner join GloryStaging. [eVAS]. [ERRORDEF] ef on ed.ERRORID = ef. [ERRORNO] and t.clientno = ef.Clientno and ed. TYPE = ef.TYPE
inner join CMS_DW. dbo. DimReasonCode dc on ed. TYPE = dc. Type and dc.Application_Instance_ID = 999 and t.CLIENTNO = dc.MVID and ed. ERRORID = dc.ReasonCodeID
inner join GloryStaging. [cdc].[fn_cdc_get_net_changes_eVAS_ERRORDTL](@from_lsn, @to_lsn, N'all') ZZZ on zzz.trace = ed.trace and zzz. ERRORID = ed.ERRORID 
inner join [CMS_DW]. [dbo]. [FactTransactionEnvelope] fte on fte.Application_Instance_ID = 999 and fte. FactTransactionEnvelope_NatKey = ed. TRACE
left join [CMS_DW]. [dbo].[FactEnvReasonCode] fr on fr.TraceNo = ed. TRACE and fr.DimReasonCodeID = dc.DimReasonCodeID and fte. Envelope_Number = fr.Envelope_Number
where ed. [TYPE] = 'N'

insert into [CMS_DW] . [dbo] . [FactEnvReasonCode]
([FactTransactionEnvelopeID]
, [FactTransactionID]
,[DimReasonCodeID]
,[BranchID]
,[MVID]
,[TraceNo]
,Envelope_Number
,[ReasonCodeID]
,[Type]
[Name]
, [RecordCreateDateTime]
,[RecordUpdateDateTime]
,[RecordCreateAuditPackageExecutionId]
,[RecordUpdateAuditPackageExecutionId]
,[Application_Instance_ID])
SELECT FactTransactionEnvelopeID
,FactTransactionID
,[DimReasonCodeID]
,BranchID
,MVID
,TraceNo
,Envelope_Number
,ReasonCodeID
, TYPE
,Description
,Getdate()
,Getdate()
,0
,0
,999
from #TempReasonsEnv
-- left join CMS_DW. dbo. FactTransactionEnvelope ft on t.Application_Instance_ID = ft.Application_Instance_ID and ft. FactTransactionEnvelope_NatKey = t.TraceNo
where TYPE = 'N' and COUNT <> 0 and FactEnvReasonCodeID is null

DROP TABLE #TempReasonsEnv

BEGIN

DELETE from GloryStaging.dbo.etl_control_table where TableName = @TableName
Insert into GloryStaging.dbo.etl_control_table (from_lsn, to_lsn, TableName) values (@from_lsms @to_lsn,@TableName)
-- END

end