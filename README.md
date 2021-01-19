# Ubisoft
## Part one: 
### Assuming that you write this table to HDFS, how much disk space would it occupy?

Using the formula

H = C*R*S/(1-i) * 120%

Where:
C = Compression ratio. It depends on the type of compression used (Snappy, LZOP, …) and size of the data. When no compression is used, C=1.

R = Replication factor. It is usually 3 in a production cluster.

S = Initial size of data need to be moved to Hadoop. This could be a combination of historical data and incremental data. (In this, we need to consider the growth rate of Initial Data as well, at least for next 3-6 months period, Like we have 500 TB data now, and it is expected that 50 TB will be ingested in next three months, and Output files from MR Jobs may create at least 10 % of the initial data, then we need to consider 600 TB as the initial data size).

i = intermediate data factor. It is usually 1/3 or 1/4. It is Hadoop’s Intermediate working space dedicated to storing intermediate results of Map Tasks are any temporary storage used in Pig or Hive. This is a common guidlines for many production applications. Even Cloudera has recommended 25% for intermediate results.

120 % – or 1.2 times the above total size, this is because, We have to allow room for the file system underlying the HDFS. For HDFS, this is ext3 or ext4 usually which gets very, very unhappy at much above 80% fill. I.e. For example, if you have your cluster total size as 1200 TB, but it is recommended to use only up to 1000 TB.


H = 1*3*S/(1-1/4) = 3*S/(3/4) = 4*S

To estimate it create the table from the query:

_CREATE TABLE schematransactionzhensize row format delimited fields TERMINATED BY '|' stored AS rcfile AS
SELECT TRANSACTION.*, 
       result.nb_address_ip_ltd, 
       Sum(TRANSACTION.amount_eur) 
         OVER( 
           partition BY TRANSACTION.customerid 
           ORDER BY TRANSACTION.order_datetime) amount_eur_utd 
FROM   (SELECT t.customerid, 
               Count(DISTINCT t.ip_address) AS nb_address_ip_ltd 
        -- Sum(t.amount_eur)            AS amount_eur_utd  
        FROM   schematransaction t  
        GROUP  BY t.customerid) result 
       JOIN schematransaction TRANSACTION 
         ON result.customerid = TRANSACTION.customerid 
				
describe extended schemaTransactionSize 

Table(tableName:schematransactionsize, dbName:default, owner:root, createTime:1610895410, lastAccessTime:0, retention:0, 
sd:StorageDescriptor(cols:[FieldSchema(name:customerid, type:int, comment:null), 
FieldSchema(name:order_date, type:date, comment:null), FieldSchema(name:order_datetime, type:timestamp, comment:null), FieldSchema(name:address, type:string, comment:null), FieldSchema(name:ip_address, type:string, comment:null), 
FieldSchema(name:amount_eur, type:bigint, comment:null), FieldSchema(name:nb_address_ip_ltd, type:bigint, comment:null), FieldSchema(name:amount_eur_utd, type:bigint, comment:null)], 
location:hdfs://namenode:8020/user/hive/warehouse/schematransactionsize, inputFormat:org.apache.hadoop.hive.ql.io.RCFileInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.RCFileOutputFormat, 
compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe, 
parameters:{serialization.format=|, field.delim=|}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], 
parameters:{totalSize=2160, numRows=46, rawDataSize=1900, COLUMN_STATS_ACCURATE={"BASIC_STATS":"true"}, numFiles=1, transient_lastDdlTime=1610895410}, viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE, rewriteEnabled:false)	

The totalSize returned in Hive is only the actual size of the table itself, which is only 1 copy, so taking the raw data size 4*1900=7600Kb into account the replication, compression and intermediate data factor parameters.



### In the Hadoop ecosystem, which mechanism(s) ensure(s) fault tolerance as far as Data Storage is concerned?

HDFS is highly fault-tolerant.  It handles faults by the process of replica creation. It creates a replica of users’ data on different machines in the HDFS cluster. So whenever if any machine in the cluster goes down, then data is accessible from other machines in which the same copy of data was created.

HDFS also maintains the replication factor by creating a replica of data on other available machines in the cluster if suddenly one machine fails.


## Part two: 
### What is the abstraction type in Spark that doesn’t benefit from the engine’s optimization capabilities?

 RDDs. they suffer from both Garbage Collection and serialization issues, which are expensive operations when the data grows. Unluckily, Spark does not offer any built-in optimization to speed up this kind of processes. Because of this, DataFrames were introduced in the library.

## Part three:
### Furthermore, imagine that the schemaTransaction table contains 100 million transactions for a 10k distinct users over a year of data. How would you write this table to disk in order to optimize subsequent queries on the data?

df.persist() with memory_and_disk mode


## Part Four – Optimization
### In the join function, what is the broadcasting operation used for?
It performs a join on two relations by first broadcasting the smaller one to all Spark executors, then evaluating the join criteria with each executor's partitions of the other relation.

### You will find a script below which creates some features. How would you optimize it? Please describe a few potential solutions.

1.persis store transaction table in mode memory_only or memory_and_disk
2. code in java with Kryo serializer 
3. use dataframe (catalyst optimizer, low garbage collection)

extra points: Rdd uses reducebykey instead of groupbykey; 
use parquet with snappy compression ;
repartition vs coalesce
