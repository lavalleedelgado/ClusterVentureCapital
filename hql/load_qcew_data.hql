CREATE EXTERNAL TABLE IF NOT EXISTS pld_qcew (
    area_fips STRING,
    own_code STRING,
    industry_code STRING,
    agglvl_code STRING,
    size_code STRING,
    year STRING,
    qtr STRING,
    disclosure_code STRING,
    area_title STRING,
    own_title STRING,
    industry_title STRING,
    agglvl_title STRING,
    size_title STRING,
    qtrly_estabs INTEGER,
    month1_emplvl INTEGER,
    month2_emplvl INTEGER,
    month3_emplvl INTEGER,
    total_qtrly_wages INTEGER,
    taxable_qtrly_wages INTEGER,
    qtrly_contributions INTEGER,
    avg_wkly_wage INTEGER,
    lq_disclosure_code STRING,
    lq_qtrly_estabs FLOAT,
    lq_month1_emplvl FLOAT,
    lq_month2_emplvl FLOAT,
    lq_month3_emplvl FLOAT,
    lq_total_qtrly_wages FLOAT,
    lq_taxable_qtrly_wages FLOAT,
    lq_qtrly_contributions FLOAT,
    lq_avg_wkly_wage FLOAT,
    oty_disclosure_code STRING,
    oty_qtrly_estabs_chg INTEGER,
    oty_qtrly_estabs_pct_chg FLOAT,
    oty_month1_emplvl_chg INTEGER,
    oty_month1_emplvl_pct_chg FLOAT,
    oty_month2_emplvl_chg INTEGER,
    oty_month2_emplvl_pct_chg FLOAT,
    oty_month3_emplvl_chg INTEGER,
    oty_month3_emplvl_pct_chg FLOAT,
    oty_total_qtrly_wages_chg INTEGER,
    oty_total_qtrly_wages_pct_chg FLOAT,
    oty_taxable_qtrly_wages_chg INTEGER,
    oty_taxable_qtrly_wages_pct_chg FLOAT,
    oty_qtrly_contributions_chg INTEGER,
    oty_qtrly_contributions_pct_chg FLOAT,
    oty_avg_wkly_wage_chg INTEGER,
    oty_avg_wkly_wage_pct_chg FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = '\,',
    'quoteChar' = '\"'
)
STORED AS TEXTFILE location '/pld/data/qcew';
CREATE TABLE IF NOT EXISTS pld_emp (
    year SMALLINT,
    quarter STRING,
    msa_code INTEGER,
    naics_code INTEGER,
    employment INTEGER
)
STORED AS ORC;
INSERT OVERWRITE TABLE pld_emp
SELECT
    year,
    qtr,
    SUBSTR(area_fips, 2, 4) || '0',
    industry_code,
    (month1_emplvl + month2_emplvl + month3_emplvl / 3)
FROM pld_qcew
WHERE disclosure_code = '';
