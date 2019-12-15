#!/bin/bash

# Patrick Lavallee Delgado
# Department of Computer Science
# University of Chicago
# December 2019

################################################################################
# Collect data from the US Bureau of Labor Statistics. Get the Quarterly County
# Employment and Wages aggregations for the metropolitan statistical area level.
################################################################################

# Create the pld directory in HDFS.
HDFS='/pld/data'
hdfs dfs -mkdir -p $HDFS

# Create the local directory in which to save the data.
DIR='data/qcew'
mkdir -p $DIR

# Specify the QCEW endpoint.
QCEW='https://data.bls.gov/cew/data/files/$year/csv/$year_qtrly_by_area.zip'

# Download average annual employment data since 2010 through the current year.
YEAR_LBOUND=2010
YEAR_UBOUND=$(date +'%Y')
for year in $(seq $YEAR_LBOUND 1 $YEAR_UBOUND)
do
    url=$(echo $QCEW | sed "s/\$year/$year/g")
    wget $url -P $DIR
done

# Unzip MSA average annual employment data.
for archive in "$DIR"/*.zip
do
    unzip -j -d $DIR $archive '*MSA.csv'
done

# Cut the header row of each CSV file and rename with the MSA code.
for csv in "$DIR"/*.csv
do
    new_filename=$(echo $csv | sed 's/\.[a-z|0-9|-]* /_/' | sed 's/ .*/.csv/')
    cat "$csv" | tail -n +2 > $new_filename
    rm "$csv"
done

# Clean up ZIP files.
UR='data/ur'
mkdir -p $UR
mv "$DIR"/*.zip $UR

# Put data in HDFS.
hdfs dfs -put $DIR $HDFS
