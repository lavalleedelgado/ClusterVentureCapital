CREATE TABLE pld_vc_hbase (
    vc_id INTEGER,
    cluster_label STRING,
    cluster_emp INTEGER,
    msa_emp DOUBLE,
    cluster_amt DOUBLE,
    msa_amt DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,vc:cluster_label,vc:cluster_emp,vc:msa_emp,vc:cluster_amt,vc:msa_amt'
)
TBLPROPERTIES ('hbase.table.name' = 'pld_vc_hbase');
INSERT OVERWRITE TABLE pld_vc_hbase
SELECT
    CAST(year || quarter || msa_code AS INTEGER),
    cluster_label,
    cluster_emp,
    msa_emp,
    cluster_amt,
    msa_amt
FROM pld_vc;
CREATE TABLE pld_msa_hbase (
    msa_id INTEGER,
    msa_label STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,msa:msa_label'
)
TBLPROPERTIES ('hbase.table.name' = 'pld_msa_hbase');
INSERT OVERWRITE TABLE pld_msa_hbase
SELECT
    CAST(census_year || msa_code AS INTEGER),
    msa_label
FROM pld_msa;
