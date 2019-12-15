CREATE EXTERNAL TABLE IF NOT EXISTS pld_naics_csv (
    naics_code STRING,
    naics_label STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = '\t',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE location '/pld/data/naics';
