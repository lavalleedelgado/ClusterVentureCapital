CREATE EXTERNAL TABLE IF NOT EXISTS pld_msa_csv (
    msa_label STRING,
    msa_code INTEGER
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = '\t',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE location '/pld/data/msa';
CREATE TABLE IF NOT EXISTS pld_msa (
    census_year SMALLINT,
    msa_code INTEGER,
    msa_label STRING
)
STORED AS ORC;
INSERT OVERWRITE TABLE pld_msa
SELECT 2010, msa_code, msa_label
FROM pld_msa_csv;
