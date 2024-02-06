/****** Script for SelectTopNRows command from SSMS  ******/


CREATE MASTER KEY;

CREATE DATABASE SCOPED CREDENTIAL ADLSCreds
WITH
	IDENTITY = 'Storage Account Key' ,
    SECRET = 'QHc7JRG/sH+0w/Drx19fARz+Pp4kxX5JRJFBg/I2gz5vbK4XExuLwd47NF/8DqfrpYRuLwHxfGlN+ASt4//6SQ=='
;

-- https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
CREATE EXTERNAL DATA SOURCE ADLGSource
WITH
  ( LOCATION = 'abfss://pplsource@dp203datalakeor.dfs.core.windows.net/people.csv' ,
   CREDENTIAL = ADLSCreds,
    TYPE = HADOOP
 );


CREATE EXTERNAL FILE FORMAT csvFile
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
      FIELD_TERMINATOR = ',',
      STRING_DELIMITER = '"',
      FIRST_ROW = 2,
      USE_TYPE_DEFAULT = FALSE,
      ENCODING = 'UTF8')
);


CREATE EXTERNAL TABLE dbo.People (
	   [firstname] NVARCHAR(256) NULL
      ,[lastname] NVARCHAR(256) NULL
      ,[gender] NVARCHAR(256) NULL
	  ,[location] NVARCHAR(256) NULL
      ,[subscription_type] NVARCHAR(256) NULL
)
WITH (
    LOCATION='../',
    DATA_SOURCE=ADLGSource,
    FILE_FORMAT=csvFile,
	REJECT_TYPE = value,
	REJECT_VALUE = 2
);


select top 10 * from dbo.People

