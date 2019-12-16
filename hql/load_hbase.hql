CREATE TABLE pld_vc_wide_hbase (
    vc_id INTEGER,
    msa_emp INTEGER,
    msa_amt INTEGER,
    computers_emp INTEGER,
    computers_amt INTEGER,
    other_real_estate_emp INTEGER,
    other_real_estate_amt INTEGER,
    oil_and_gas_emp INTEGER,
    oil_and_gas_amt INTEGER,
    electric_utilities_emp INTEGER,
    electric_utilities_amt INTEGER,
    other_emp INTEGER,
    other_amt INTEGER,
    coal_mining_emp INTEGER,
    coal_mining_amt INTEGER,
    retailing_emp INTEGER,
    retailing_amt INTEGER,
    construction_emp INTEGER,
    construction_amt INTEGER,
    other_energy_emp INTEGER,
    other_energy_amt INTEGER,
    telecommunications_emp INTEGER,
    telecommunications_amt INTEGER,
    lodging_and_conventions_emp INTEGER,
    lodging_and_conventions_amt INTEGER,
    restaurants_emp INTEGER,
    restaurants_amt INTEGER,
    agriculture_emp INTEGER,
    agriculture_amt INTEGER,
    airlines_and_airports_emp INTEGER,
    airlines_and_airports_amt INTEGER,
    other_banking_and_financial_services_emp INTEGER,
    other_banking_and_financial_services_amt INTEGER,
    other_health_care_emp INTEGER,
    other_health_care_amt INTEGER,
    tourism_and_travel_services_emp INTEGER,
    tourism_and_travel_services_amt INTEGER,
    business_services_emp INTEGER,
    business_services_amt INTEGER,
    manufacturing_emp INTEGER,
    manufacturing_amt INTEGER
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,vc:msa_emp,vc:msa_amt,vc:computers_emp,vc:computers_amt,vc:other_real_estate_emp,vc:other_real_estate_amt,vc:oil_and_gas_emp,vc:oil_and_gas_amt,vc:electric_utilities_emp,vc:electric_utilities_amt,vc:other_emp,vc:other_amt,vc:coal_mining_emp,vc:coal_mining_amt,vc:retailing_emp,vc:retailing_amt,vc:construction_emp,vc:construction_amt,vc:other_energy_emp,vc:other_energy_amt,vc:telecommunications_emp,vc:telecommunications_amt,vc:lodging_and_conventions_emp,vc:lodging_and_conventions_amt,vc:restaurants_emp,vc:restaurants_amt,vc:agriculture_emp,vc:agriculture_amt,vc:airlines_and_airports_emp,vc:airlines_and_airports_amt,vc:other_banking_and_financial_services_emp,vc:other_banking_and_financial_services_amt,vc:other_health_care_emp,vc:other_health_care_amt,vc:tourism_and_travel_services_emp,vc:tourism_and_travel_services_amt,vc:business_services_emp,vc:business_services_amt,vc:manufacturing_emp,vc:manufacturing_amt,'
)
TBLPROPERTIES ('hbase.table.name' = 'pld_vc_wide_hbase');
INSERT OVERWRITE TABLE pld_vc_wide_hbase
SELECT
    CAST(year || quarter || msa_code AS INTEGER),
    msa_emp,
    msa_amt,
    computers_emp,
    computers_amt,
    other_real_estate_emp,
    other_real_estate_amt,
    oil_and_gas_emp,
    oil_and_gas_amt,
    electric_utilities_emp,
    electric_utilities_amt,
    other_emp,
    other_amt,
    coal_mining_emp,
    coal_mining_amt,
    retailing_emp,
    retailing_amt,
    construction_emp,
    construction_amt,
    other_energy_emp,
    other_energy_amt,
    telecommunications_emp,
    telecommunications_amt,
    lodging_and_conventions_emp,
    lodging_and_conventions_amt,
    restaurants_emp,
    restaurants_amt,
    agriculture_emp,
    agriculture_amt,
    airlines_and_airports_emp,
    airlines_and_airports_amt,
    other_banking_and_financial_services_emp,
    other_banking_and_financial_services_amt,
    other_health_care_emp,
    other_health_care_amt,
    tourism_and_travel_services_emp,
    tourism_and_travel_services_amt,
    business_services_emp,
    business_services_amt,
    manufacturing_emp,
    manufacturing_amt
FROM pld_vc_wide;
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
