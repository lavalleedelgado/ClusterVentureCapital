# Venture Capital Finance Mapper

Patrick Lavallee Delgado \\
Department of Computer Science \\
University of Chicago \\
December 2019

Financial investments in start-ups and emerging firms offer compelling indicators of the economic development in an industry or region, particularly with respect to advances in knowledge and technology. Since small companies have fewer reporting obligations to the US Securities and Exchange Commission, one often has little evidence beyond anecdote with which to identify sectors on the brink of growth.

Still, even small companies must disclose the stocks, bonds, and other financial instruments they sell investors to raise capital. One can approximate venture capital activity from the receipts of these "exempt offerings". Companies report these on SEC form D, filings for which are publically available the next business day. Of course, this premise assumes widespread compliance, [which some argue is weak](https://techcrunch.com/2018/11/07/the-disappearing-form-d/).

This application offers preliminary understanding the geographic and industrial distribution of venture capital from SEC data with contextual support from US Bureau of Labor Statistics and US Census Bureau data. Existing services either require expensive subscriptions or are static visualizations.

The scripts `setup.sh` and `run.sh` replicate the work I describe below.

## Data

### US Security and Exchance Commission, EDGAR system

The daily index lists all filings received in the previous business day by form type and with links to XML file representations of each. I look to the form D template to identify the relevant tags in the document. It seems that all EDGAR files start with an unended XML tag, which confuses the document intepreter. I remove this line upon download.

| `pld_form_d`  | type     |
| ------------- | -------- |
| cik           | INTGER   |
| entity        | STRING   |
| census_year   | SMALLINT |
| year          | SMALLINT |
| month         | TINYINT  |
| day           | TINYINT  |
| zip_code      | STRING   |
| cluster_label | STRING   |
| amount        | FLOAT    |

My implementation uses a Java appliation to read each XML file, serializes the data into HDFS, and loads into Hive with the Thrift deserializer.

```
touch sec.out

# Download SEC data to the cluster.
sh utils/get_sec_data.sh >> sec.out

# Serialize SEC data into HDFS.
JAR='target/ClusterVentureCapital-1.0-SNAPSHOT.jar'
yarn jar $JAR pld.ClusterVentureCapital.SerializeExemptOfferings data/form_d/

# Load SEC data into Hive.
hdfs dfs -put $JAR /pld
hive -f hql/load_form_d_data.hql >> sec.out
```

### US Bureau of Labor Statistics, QCEW survey

The quarterly survey collects county employment and wage data by industry. The data also offer aggregation at the metropolitan statistical area (MSA) level, which more naturally describes the lived economic climate. Each publication of the QCEW is a ZIP directory of CSV files for each geopgrahy. I only keep those that correspond to a MSA and cut the header row.

| `pld_emp`  | type     |
| ---------- | -------- |
| year       | SMALLINT |
| quarter    | STRING   |
| msa_code   | INTEGER  |
| naics_code | STRING   |
| employment | INTEGER  |

My implementation puts all the CSV files into HDFS, loads the data into Hive with the CSV deserializer, and puts the necessary fields into a new ORC table. 

```
touch bls.out

# Download BLS data to HDFS.
sh utils/get_bls_data.sh >> bls.out

# Load BLS data into Hive.
hive -f hql/load_qcew_data.hql >> bls.out
```

### US Census Bureau

The geographic correspondence data allow me to compare the data I collect from the SEC at the ZIP code level to that I collect from the BLS at the MSA level. The MSA and industry code (NAICS) labels make the data easier to read. These are static data: the census updates geographic correspondence every ten years, and industry definitions in response just as infrequently.

| `pld_msa`   | type     |
| ----------- | -------- |
| census_year | SMALLINT |
| msa_code    | INTEGER  |
| msa_label   | STRING   |

My implementation reinterprets the original comma-delimited files as tab-delimited files for easy reference from the command line. it puts the TSV files into HDFS and loads the data into Hive with the CSV deserializer using the tab delimiter. 

```
touch acs.out

# Download ACS data to HDFS.
sh utils/get_acs_data.sh >> acs.out

# Load ACS data into Hive.
hive -f hql/load_zip_msa_data.hql >> acs.out
hive -f hql/load_msa_data.hql >> acs.out
hive -f hql/load_naics_data.hql >> acs.out
```

## Views

### Industry clusters

The economic development and policy literature offers no consensus on the definition of a "cluster". The US Cluster Mapping Project at Harvard Business School clusters NAICS codes by their geographic specialization and comanifestation. Future efforts will replicate those clusters, but in the meantime, I follow a rather rudimentary approach based on semantic proximity.

| `pld_naics`   | type   |
| ------------- | ------ |
| naics_code    | STRING |
| naics_label   | STRING |
| cluster_label | STRING |

My implementation uses a Scala script to map NAICS codes to the umbrella industries that companies report on SEC form D. The NAICS classification system reads from left to right, which requires I process these values as strings.

```
touch view.out

# Create the correspondence of NAICS codes to cluster codes.
spark-shell \
    --conf spark.hadoop.metastore.catalog.default=hive \
    < hql/view_clusters_naics.scala
    >> view.out
```

### Batch view

Lastly, I use Scala and HQL to join these tables into the view our application serves in response to a query. I aggregate the SEC data from months to quarters and from ZIP codes to MSA codes, and then join the result with the BLS and NAICS data. I also aggregate that same view to the MSA level to calculate the share of employment and financing that a cluster represents for the MSA. The unique identifier for HBase is the concatenation of the year, quarter, and MSA code fields.

| `pld_venture_capital` | type     |
| --------------------- | -------- |
| year                  | SMALLINT |
| quarter               | STRING   |
| msa_code              | INTEGER  |
| cluster_label         | STRING   |
| cluster_emp           | INTEGER  |
| msa_emp               | FLOAT    |
| cluster_amt           | INTEGER  |
| msa_amt               | FLOAT    |

```
# Create an intermediate view to avoid reconciling Spark with Hive.
hive -f hql/view_venture_capital_tmp.hql >> view.out

# Create the final batch layer view and load into HBase.
spark-shell --conf spark.hadoop.metastore.catalog.default=hive \
    < hql/view_venture_capital.scala
    >> view.out
hbase shell hql/init_hbase.txt >> view.out
hive -f hql/load_hbase.hql >> view.out
```

### Realtime view

I regret not being able to build a speed layer to update this application with new data from the SEC and BLS. My approach would have repurposed the shell scripts that downloaded the available data to accept a different lower time bound as a parameter. These new data would go into HDFS for inclusion with the compution of the next batch view, and into a Kafka topic for immediate incorporation with the current batch view.

Other hopes and dreams include:
* the clustering exercise to represent the lived economic climate that the NAICS codes do not describe
* a map of MSAs to identify venture capital hotspots and movement of clusters and financing over time
* a correspondence of geographic definitions from 2010 to previous and future decennial censuses

## Web app.

The web app allows accepts and delivers requests of the serving layer. I create a simple Node.js application and launch it on the webserver.

```
# Install any missing dependencies.
npm install express
npm install mustache
npm install hbase-rpc-client

# Run the application.
node web_app.js
```

The webserver is on `34.66.189.234` and the applicaton lists on port `3886`. So, navigating to `http://34.66.189.234:3886/cvc.html` reaches the web app.



Thanks for reading!
