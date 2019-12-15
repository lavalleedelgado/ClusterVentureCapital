CREATE EXTERNAL TABLE IF NOT EXISTS pld_zip_cbsa_csv (
    zcta5 STRING,
    cbsa INTEGER,
    metro TINYINT,
    rel_population INTEGER,
    rel_housing_units INTEGER,
    rel_total_area INTEGER,
    rel_land_area INTEGER,
    zip_population INTEGER,
    zip_housing_units INTEGER,
    zip_total_area INTEGER,
    zip_land_area INTEGER,
    cbsa_population INTEGER,
    cbsa_housing_units INTEGER,
    cbsa_total_area INTEGER,
    cbsa_land_area INTEGER,
    rel_zip_pct_population FLOAT,
    rel_zip_pct_housing_units FLOAT,
    rel_zip_pct_total_area FLOAT,
    rel_zip_pct_land_area FLOAT,
    rel_cbsa_pct_population FLOAT,
    rel_cbsa_pct_housing_units FLOAT,
    rel_cbsa_pct_total_area FLOAT,
    rel_cbsa_pct_land_area FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE location '/pld/data/zip_msa';
CREATE TABLE IF NOT EXISTS pld_zip_msa (
    census_year SMALLINT,
    zip_code STRING,
    msa_code INTEGER
)
STORED AS ORC;
INSERT OVERWRITE TABLE pld_zip_msa
SELECT DISTINCT 2010, zcta5, cbsa
FROM pld_zip_cbsa_csv
WHERE metro = 1;
