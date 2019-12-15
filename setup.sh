#!/bin/bash

# Patrick Lavallee Delgado
# Department of Computer Science
# University of Chicago
# December 2019

################################################################################
# Set up ClusterVentureCapital.
################################################################################

# On the cluster, navigate to the working directory.
cd ~/ClusterVentureCapital

# # Build the project from the Maven model.
# mvn clean -f 'pom.xml'
# mvn install -f 'pom.xml'

################################################################################
# Get exempt filings data.
################################################################################

touch sec.out

# Download SEC data to the cluster.
sh utils/get_sec_data.sh >> sec.out
echo 'Downloaded exempt filings data.'

# Serialize SEC data into HDFS.
JAR='target/ClusterVentureCapital-1.0-SNAPSHOT.jar'
yarn jar $JAR pld.ClusterVentureCapital.SerializeExemptOfferings data/form_d/
echo 'Serialized exempt filings data.'

# Load SEC data into Hive.
hdfs dfs -put $JAR /pld
hive -f hql/load_form_d_data.hql >> sec.out
echo 'Finished with exempt filings data.'

################################################################################
# Get employment data.
################################################################################

touch bls.out

# Download BLS data to HDFS.
sh utils/get_bls_data.sh >> bls.out
echo 'Downloaded quarterly employment data.'

# Load BLS data into Hive.
hive -f hql/load_qcew_data.hql >> bls.out
echo 'Finished with quarterly employment data.'

################################################################################
# Get census data.
################################################################################

touch acs.out

# Download ACS data to HDFS.
sh utils/get_acs_data.sh >> acs.out
echo 'Downloaded census data.'

# Load ACS data into Hive.
hive -f hql/load_zip_msa_data.hql >> acs.out
hive -f hql/load_msa_data.hql >> acs.out
hive -f hql/load_naics_data.hql >> acs.out
echo 'Finished with census data.'

################################################################################
# Create the views.
################################################################################

touch view.out

# Create the correspondence of NAICS codes to cluster codes.
spark-shell \
    --conf spark.hadoop.metastore.catalog.default=hive \
    < hql/view_naics_clusters.scala
    >> view.out
echo 'Finished drawing NAICS-cluster correspondence.'

# Create an intermediate view to avoid reconciling Spark with Hive.
hive -f hql/view_vc_tmp.hql >> view.out

# Create the final batch layer view and load into HBase.
spark-shell --conf spark.hadoop.metastore.catalog.default=hive \
    < hql/view_vc.scala
    >> view.out
hbase shell hql/init_hbase.txt >> view.out
hive -f hql/load_hbase.hql >> view.out
echo 'Finished batch layer view.'

# # Create the speed layer view and load into HBase.
# echo 'Finished speed layer view.'
