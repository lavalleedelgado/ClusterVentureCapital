ADD JAR hdfs:///pld/ClusterVentureCapital-1.0-SNAPSHOT.jar;
CREATE TABLE IF NOT EXISTS pld_vc_tmp (
    year SMALLINT,
    quarter TINYINT,
    msa_code INTEGER,
    cluster_label STRING,
    cluster_emp INTEGER,
    cluster_amt INTEGER
)
STORED AS ORC;
WITH
    form_d_quarter AS (
        SELECT 
            pld_form_d.year,
            (FLOOR(pld_form_d.month / 4) + 1) AS quarter,
            pld_form_d.zip_code,
            pld_form_d.cluster_label,
            pld_form_d.amount
        FROM pld_form_d
    ),
    sec_zip_on_msa AS (
        SELECT
            form_d_quarter.year,
            form_d_quarter.quarter,
            pld_zip_msa.msa_code,
            form_d_quarter.cluster_label,
            SUM(form_d_quarter.amount) AS cluster_amt
        FROM form_d_quarter
        JOIN pld_zip_msa
            ON pld_zip_msa.zip_code = form_d_quarter.zip_code
            AND pld_zip_msa.census_year = (FLOOR(form_d_quarter.year / 10) * 10)
        GROUP BY year, quarter, msa_code, cluster_label
    ),
    bls_naics_on_cluster AS (
        SELECT
            pld_emp.year,
            pld_emp.quarter,
            pld_emp.msa_code,
            pld_naics.cluster_label,
            SUM(pld_emp.employment) AS cluster_emp
        FROM pld_emp
        JOIN pld_naics
            ON pld_naics.naics_code = pld_emp.naics_code
        GROUP BY year, quarter, msa_code, cluster_label
    )
INSERT OVERWRITE TABLE pld_vc_tmp
SELECT
    sec_zip_on_msa.year,
    sec_zip_on_msa.quarter,
    sec_zip_on_msa.msa_code,
    sec_zip_on_msa.cluster_label,
    bls_naics_on_cluster.cluster_emp,
    sec_zip_on_msa.cluster_amt
FROM sec_zip_on_msa
JOIN bls_naics_on_cluster
    ON bls_naics_on_cluster.year = sec_zip_on_msa.year
    AND bls_naics_on_cluster.quarter = sec_zip_on_msa.quarter
    AND bls_naics_on_cluster.msa_code = sec_zip_on_msa.msa_code
    AND bls_naics_on_cluster.cluster_label = sec_zip_on_msa.cluster_label;
    