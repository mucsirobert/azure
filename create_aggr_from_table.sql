

create procedure aggr_external_insurance_table
@table_name VARCHAR(100)
as
BEGIN

    declare @sql nvarchar(max) = 
    
    'CREATE EXTERNAL TABLE aggr_'+@table_name+'
    WITH (
        LOCATION = ''/aggr_'+@table_name+''',
        DATA_SOURCE = [insur_csv],  
        FILE_FORMAT = [DELIMITEDTEXT]
    )  
    AS
    SELECT aggregated_sheet.*
    , string_agg(cast(original_sheet.policyID as nvarchar(max)), '''+','+''') as highest_eq_site_limit_policyID
    FROM '+@table_name+' as original_sheet
        inner join 
        (SELECT county, count(policyID) as num_of_policyIDs, max(eq_site_limit) as highest_eq_site_limit
        FROM '+@table_name+'
        group by county) as aggregated_sheet on original_sheet.county = aggregated_sheet.county 
        and aggregated_sheet.highest_eq_site_limit = original_sheet.eq_site_limit
    group by aggregated_sheet.county, aggregated_sheet.num_of_policyIDs, aggregated_sheet.highest_eq_site_limit'

    print @sql

    
    EXEC SP_executesql @tsql = @sql

END