CREATE TABLE pld_cluster_venture_capital_hbase (
    hbase_id INTEGER,
    msa_label STRING,
    cluster_label STRING
    cluster_emp INTEGER,
    msa_emp DOUBLE,
    cluster_amt DOUBLE,
    msa_amt DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,cvc:msa_label,cvc:cluster_label,cvc:cluster_emp,cvc:msa_emp,cvc:cluster_amt,cvc:msa_amt'
)
TBLPROPERTIES ('hbase.table.name' = 'pld_cluster_venture_capital');
INSERT OVERWRITE TABLE pld_cluster_venture_capital_hbase
SELECT
    CAST(year || quarter || msa_code AS INTEGER),
    msa_label,
    cluster_label,
    cluster_emp,
    msa_emp,
    cluster_amt,
    msa_amt
FROM pld_cluster_venture_capital;
