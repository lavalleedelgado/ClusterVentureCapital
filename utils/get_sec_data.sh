#!/bin/bash

# Patrick Lavallee Delgado
# Department of Computer Science
# University of Chicago
# December 2019

################################################################################
# Collect data from the US Securities and Exchange Commission. Get the daily
# index, then the individual filings on form D at the URLs identified.
################################################################################

# Create the local directories for the SEC data.
DIR_INDEX='data/edgar'
DIR_FILES='data/form_d'
mkdir -p $DIR_INDEX $DIR_FILES

# Specify the EDGAR index and filing endpoints.
EDGAR_INDEX='https://www.sec.gov/Archives/edgar/daily-index/$year/$quarter/'
EDGAR_FILES='https://www.sec.gov/Archives/edgar/data/$file/primary_doc.xml'

# Download daily indices since 2010 for each quarter through the current year.
YEAR_LBOUND=2010
YEAR_UBOUND=$(date +'%Y')
for year in $(seq $YEAR_LBOUND 1 $YEAR_UBOUND)
do 
    for quarter in QTR1 QTR2 QTR3 QTR4
    do
        url=$(echo $EDGAR_INDEX | sed "s/\$year/$year/" | sed "s/\$quarter/$quarter/")
        wget -r -nd --accept-regex 'form\..*\.idx' --reject-regex '.*\.html|.*\.txt' -P $DIR_INDEX $url
    done
done

# Download form D filings identified in daily indices.
for index in "$DIR_INDEX"/*.idx
do
    forms_d=$(
        cat $index                          |   # Read the index file.
        grep '^D '                          |   # Identify filings on form D.
        sed 's/^.*data\/\(.*\).txt/\1/g'    |   # Extract the CIK and filing ID.
        sed 's/-//g'                        |   # Remove the hyphen.
        sed 's/\//\\\//g'                       # Escape reserved characters for sed, l. 46.
    )
    for file in $forms_d
    do            
        url=$(echo $EDGAR_FILES | sed "s/\$file/$file/")            # Construct the URL.
        filename=$(echo $file | sed 's/^.*\/\([0-9]*$\)/\1.xml/g')  # Construct the new filename.
        wget -qO- $url |                                            # Request the filing.
        sed -E 's/^<.?\?xml.*$//g' |                                # Remove errant XML tags.
        cat > $DIR_FILES'/'$filename                                # Save.
    done
done
