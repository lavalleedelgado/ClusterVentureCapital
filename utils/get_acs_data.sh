#!/bin/bash

# Patrick Lavallee Delgado
# Department of Computer Science
# University of Chicago
# December 2019

################################################################################
# Collect data from the US Census Bureau. Get geographic correspondence of ZIP
# and MSA codes from 2010, as well as NAICS codes from 2007.
################################################################################

# Relationship files: https://www.census.gov/geographies/reference-files.2010.html
# ACS API: https://www.census.gov/data/developers/data-sets/acs-1year.html
# NAICS: https://www.census.gov/eos/www/naics/

# Create the pld directory in HDFS.
HDFS='/pld/data'
hdfs dfs -mkdir -p $HDFS

# Specify service endpoints.
REF='http://www2.census.gov/geo/docs/maps-data/data/rel'
ACS='https://api.census.gov/data/2010/acs/acs1/profile?'

# Download the ZIP-CBSA relationship file.
dir_zip_msa='data/zip_msa'
mkdir -p $dir_zip_msa
wget -qO- $REF'/zcta_cbsa_rel_10.txt'   |   # Request the file.
sed "s/,/$(printf '\t')/g"              |   # Replace delimiter with tab.
tail -n +2                              |   # Remove header row.
cat > $dir_zip_msa'/zip_msa_2010.txt'       # Save.
hdfs dfs -put $dir_zip_msa $HDFS            # Put in HDFS.

# Download the MSA labels and reformat as tab-delimited text.
dir_msa='data/msa'
mkdir -p $dir_msa
wget -qO- $ACS'get=NAME&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*' |
sed 's/\[//g'                   |   # Remove all open brackets.
sed 's/\]//g'                   |   # Remove all closed brackets.
sed 's/,$//g'                   |   # Remove all commas at end of lines.
sed "s/\",\"/$(printf '\t')/"   |   # Replace delimiter with tab.
sed 's/"//g'                    |   # Remove all remaining double quotes.
tail -n +2                      |   # Remove header row.
cat > $dir_msa'/msa_2010.txt'       # Save.
hdfs dfs -put $dir_msa $HDFS        # Put in HDFS.

# Download the NAICS labels and reformat as tab-delimited text.
dir_naics='data/naics'
mkdir -p $dir_naics
wget -qO- 'https://www.census.gov/eos/www/naics/reference_files_tools/2007/naics07.txt' |
awk '{ print substr($0, 9, 6) "\t" substr($0, 16, 121) }'   |   # Cut by position.
sed 's/"//g'                                                |   # Remove double quotes.                           
tail -n +3                                                  |   # Remove header rows.
tail -r | sed '1,7d' | tail -r                              |   # Remove trailing rows.
cat > $dir_naics'/naics_2007.txt'                               # Save.
hdfs dfs -put $dir_naics $HDFS                                  # Put in HDFS.

# Clean up ZIP files.
UR='data/ur'
mkdir -p $UR
mv $path_msa* $UR

# # Download the MSA cartographic boundary files with 1:20M resolution.
# SHP='https://www2.census.gov/geo/tiger/GENZ2010'
# dir_shp='data/shp'
# mkdir -p $dir_shp
# path_msa=$dir_shp'/msa_shapes_2010'
# wget -O $path_msa'.zip' $SHP'/gz_2010_us_310_m1_20m.zip'
# unzip $path_msa'.zip' -d $dir_shp
# pip3 install -q geopandas
# python3 << EOF
# import geopandas as gpd
# shapefile_dir = '$path_msa'
# shapefile_csv = shapefile_dir + '.csv'
# gpd.read_file(shapefile_dir).to_csv(shapefile_csv, index=False, header=False)
# EOF
# hdfs dfs -put $dir_shp $HDFS
