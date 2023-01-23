
create PROCEDURE create_external_insur_table
    @table_name VARCHAR(100) 
AS
BEGIN

    IF NOT EXISTS (select * from sys.external_data_sources where name = 'insur_csv')
    create EXTERNAL data source [insur_csv]
    WITH (
        LOCATION = 'abfss://insu1324rance@ins1324urance.dfs.core.windows.net'
    )

    IF NOT EXISTS (select * from sys.external_file_formats where name = 'DELIMITEDTEXT')
        CREATE EXTERNAL FILE FORMAT DELIMITEDTEXT  
    WITH (  
        FORMAT_TYPE = DELIMITEDTEXT  
        , FORMAT_OPTIONS (STRING_DELIMITER = '', FIELD_TERMINATOR = ',', FIRST_ROW = 2, ENCODING = 'UTF8') 
        )


    declare @sql nvarchar(max) = 
    'IF EXISTS (SELECT * FROM sys.external_tables where name = '''+@table_name+''')
        DROP EXTERNAL TABLE '+@table_name+'


    create EXTERNAL TABLE '+@table_name+'(
        policyID int ,
        statecode varchar(2) ,
        county varchar(45) ,
        eq_site_limit float ,
        hu_site_limit float ,
        fl_site_limit float ,
        fr_site_limit float ,
        tiv_2011 float ,
        tiv_2012 float ,
        eq_site_deductible float ,
        hu_site_deductible float ,
        fl_site_deductible float ,
        fr_site_deductible float ,
        point_latitude float ,
        point_longitude float ,
        line varchar(30) ,
        construction varchar(30) ,
        point_granularity SMALLINT 
    ) with (LOCATION = ''/'+@table_name+'.csv'+'''
    , DATA_SOURCE = [insur_csv]
    , FILE_FORMAT = [DELIMITEDTEXT]
    )'

    print @sql

    EXEC SP_executesql @tsql = @sql
END
