ADD JAR hdfs:///pld/ClusterVentureCapital-1.0-SNAPSHOT.jar;
CREATE EXTERNAL TABLE IF NOT EXISTS pld_form_d
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
WITH SERDEPROPERTIES (
    'serialization.class' = 'pld.IngestExemptOfferings.ExemptOffering',
    'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol'
)
STORED AS SEQUENCEFILE LOCATION '/pld/data/form_d';
