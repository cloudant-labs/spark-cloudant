Spark SQL Cloudant External Datasource
================

Cloudant integration with Spark as Spark SQL external datasource. 

## Contents:

### RelationProvider implementations

All implementions can be found [here] (cloudant-spark-sql/src/main/scala/com/cloudant/spark):

Relation Provider Name | Table Option | Scan Type | Column Pruning | Predicates Push Down | Parallel Loading | Insertable | 
--- | --- | --- | --- | --- | --- | --- | 
[DefaultSource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/DefaultSource.scala)|database or path, index|PrunedFilteredScan| Yes |_id or first predicate | Yes, except with index | Yes |
[CloudantDatasource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/CloudantDatasource.scala)|database|TableScan| No | No | No | No | 
[CloudantPrunedFilteredDatasource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/)|database|PrunedFilteredScan| Yes |_id or first predicate | No | No | CloudantPrunedFilteredDatasource.scala)
[CloudantPartitionedPrunedFilteredDatasource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/CloudantPartitionedPrunedFilteredDatasource.scala)|database, index|PrunedFilteredScan| Yes |_id or first predicate |  Yes, except with index  | No |


### Binary download:

Spark Version | Release # | Binary Location
--- | --- | ---
1.3.0 | v0.1 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v0.1/cloudant-spark.jar)
1.3.1 | v1.3.1.2 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v1.3.1.2/cloudant-spark.jar)
1.4.0 | v1.4.0.0 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/1.4.0.0/cloudant-spark.jar)
1.4.1 | v1.4.1.3 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v1.4.1.3/cloudant-spark.jar)
1.4.1 | v1.4.1.4 | [Location] (https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.4.1.4/cloudant-spark-1.4.1.jar)
1.4.1 | v1.4.1.5 | [Location] (https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.4.1.5/cloudant-spark.jar)
1.6.1 | v1.6.1.0 | [Location] (https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.6.1/cloudant-spark-v1.6.1-35.jar)


### Build from source:

[Instructions](README_build.md)
	


## Sample application 

### Using SQL In Python 
	
[python code](examples/python/CloudantApp.py)

```python
conf = SparkConf().setAppName("Cloudant Spark SQL External Datasource in Python")
	
# define cloudant related configuration
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password","PASSWORD")
	
# create Spark context and SQL context
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
	
# create temp table
sqlContext.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark.CloudantRP OPTIONS ( database 'airportcodemapping')")
      
# create Schema RDD
data = sqlContext.sql("SELECT airportCode, airportName FROM airportTable WHERE airportCode >= 'CAA' ORDER BY airportCode")
	
# print schema
data.printSchema()
	
# print data
for code in data.collect():
	print code.airportCode
		
```	

### Using SQL In Scala 

[Scala code](examples/scala/src/main/scala/mytest/spark/CloudantApp.scala)

```scala
val conf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource in Scala")
	
// define cloudant related configuration	
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password","PASSWORD")
    
// create Spark context and SQL context
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext._
    
// Create a temp table 
sqlContext.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark.CloudantRP OPTIONS ( database 'airportcodemapping'")
  
// create Schema RDD
val data = sqlContext.sql("SELECT airportCode, airportName FROM airportTable WHERE airportCode >= 'CAA' ORDER BY airportCode"")
    
// print schema
data.printSchema()

// print data
data.map(t => "airportCode: " + t(0) +"airportName: " + t(1)).collect().foreach(println) 
	
```	


### Using DataFrame In Python 

[python code](examples/python/CloudantDF.py). 

```python	    
conf = SparkConf().setAppName("Cloudant Spark SQL External Datasource in Python")
# define coudant related configuration
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password","PASSWORD")
	
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
	
df = sqlContext.load("airportcodemapping", "com.cloudant.spark")

# cache RDD in memory
df.cache()
# to cache RDD on disk:
# df.persist(storageLevel = StorageLevel(True, True, False, True, 1))

df.printSchema()
	
df.filter(df.airportCode >= 'CAA').select("airportCode",'airportName').save("airportcodemapping_df", "com.cloudant.spark")	
	    
```
	
In case of doing multiple operations on a dataframe (select, filter etc.),
you should persist a dataframe. Othewise, every operation on a dataframe will load the same data from Cloudant again.
Persisting will also speed up computation. This statement will persist an RDD in memory: `df.cache()`.  Alternatively for large dbs to persist in memory & disk, use: 

```python
from pyspark import StorageLevel
df.persist(storageLevel = StorageLevel(True, True, False, True, 1))
```	
	
[Sample code on using DataFrame option to define cloudant configuration](examples/python/CloudantDFOption.py)
	
### Using DataFrame In Scala 

[Scala code](examples/scala/src/main/scala/mytest/spark/CloudantDF.scala)

```	scala
val conf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource in Scala")
	
// define cloudant related configuration	
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password","PASSWORD")
    
// create Spark context and SQL context
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext._
    
val df = sqlContext.read.format("com.cloudant.spark").load("airportcodemapping")

// cache RDD in memory
df.cache()
// to cache RDD on disk:
// df.persist(StorageLevel.MEMORY_AND_DISK) 

df.printSchema()
df.filter(df("airportCode") >= "CAA").select("airportCode","airportName").show()
df.filter(df("airportCode") >= "CAA").select("airportCode","airportName").write.format("com.cloudant.spark").save("airportcodemapping_df")

```	
    
 [Sample code on using DataFrame option to define cloudant configuration](examples/scala/src/main/scala/mytest/spark/CloudantDFOption.scala)

	
## Job Submission

[Details](README_submit.md)


### For Python
	
	spark-submit  --master local[4] --jars <path to cloudant-spark.jar>  <path to python script> 


### For Scala

	spark-submit --class "<your class>" --master local[4] --jars <path to cloudant-spark.jar> <path to your app jar>
		
		

## Configuration Overview		

### Configuration on SparkConf

Configuration can also be passed on DataFrame using option, which overrides what is defined in SparkConf

Name | Default | Meaning
--- |:---:| ---
cloudant.host||cloudant host url
cloudant.username||cloudant userid
cloudant.password||cloudant password
jsonstore.rdd.partitions|5|the number of partitions intent used to drive JsonStoreRDD loading query result in parallel. The actual number is calculated based on total rows returned and satisfying maxInPartition and minInPartition
jsonstore.rdd.maxInPartition|-1|the max rows in a partition. -1 means unlimited
jsonstore.rdd.minInPartition|10|the min rows in a partition.
jsonstore.rdd.requestTimeout|100000| the request timeout in milli-second
jsonstore.rdd.concurrentSave|-1| the parallel saving size. -1 means unlimited
jsonstore.rdd.bulkSize|1| the bulk save size
jsonstore.rdd.schemaSampleSize|1| the sample size for RDD schema discovery. -1 means unlimited

Default values are defined in [here](cloudant-spark-sql/src/main/resources/application.conf)

### Configuration on Spark SQL temp table

Configuration can also be passed on DataFrame using option.

Name | Default | Meaning
--- |:---:| ---
database||cloudant database name
index||cloudant search index w/o the database name.only used for load.
path||cloudant: as database name if database is not present
schemaSampleSize|1| the sample size used to discover the schema for this temp table. -1 scans all documents

## Troubleshooting

### Schema variance

If your database contains documents that don't all match exactly one JSON schema, it is possible that Spark functions break with a 
stack trace similar to this:

````
   df.show()
	org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 8.0 failed 1 times, most recent failure: Lost task 0.0 in stage 8.0 (TID 28, localhost): java.lang.ArrayIndexOutOfBoundsException: 14
	at org.apache.spark.sql.catalyst.CatalystTypeConverters$.convertRowWithConverters(CatalystTypeConverters.scala:348)
````

This error indicates that a field has been found in a document but it is not present in the RDD. Given that the RDD is by default constructed based on the data of the first document only, this error is going to happen in situations where:
 - the first document was missing an attribute
 - the first document was using an attribute but with a NULL value
 - the first document was using an attribute but with a value of a different type


 To resolve this situation we introduced the **schemaSampleSize** option listed above. That option can be used in one of two places:
 
 1) as a global setting for the Spark Context (applies to all RDDs created within that context)

 2) as a local setting for the specific RDD. (A local setting precedes a global setting)

 To add the global settting directly to your Spark Context use:

 ```
conf = SparkConf().setAppName("Multiple schema test")

conf.set("cloudant.host","<ACCOUNT>.cloudant.com")
conf.set("cloudant.username", "<USERNAME>")
conf.set("cloudant.password","<PASSWORD>")
conf.set("jsonstore.rdd.schemaSampleSize", -1)

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
```

For a local setting applied to a single RDD only, use:

```
sqlContext.sql("CREATE TEMPORARY TABLE schema-test USING com.cloudant.spark.CloudantRP OPTIONS ( schemaSampleSize '10',database 'schema-test')")
schemaTestTable = sqlContext.sql("SELECT * FROM schema-test")
```

Acceptable values for either setting are:

-1 - scan all documents in the database (be careful! This can cause the Spark job to become very expensive!)

1 - scan only the first document in the database (the default)

N - scan an arbitrary number of documents in the database (if N is greater than the number of documents in the database, we will apply -1)

0 or any non-integer values are not permitted and will result in an error.

### Unicode support

Having non-ascii characters in your Cloudant documents requires the Python interpreter to be set to support UTF-8. Failure to set the right encoding results in errors like the one shown for a `df.show()` action:

````
   df.show()
  File "/Users/holger/dev/spark-1.5.2/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 256, in show
UnicodeEncodeError: 'ascii' codec can't encode character u'\xdf' in position 1198: ordinal not in range(128)
````

There are a number of ways to force the Python interpreter to use UTF-8 encoding. A simple method is to add these lines to your script:

````
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
````

See [https://issues.apache.org/jira/browse/SPARK-11772](https://issues.apache.org/jira/browse/SPARK-11772) for details.

## Known limitations and improvement areas

* Chunked response is not supported. For parallel partitioned loading, issue can be workarouded by setting jsonstore.rdd.maxInPartition.  The following is the exception when query result is over limit.
	HttpClientConnection: Aggregated response entity greater than configured limit of 1048576 bytes,closing connection
	java.lang.RuntimeException: sendReceive doesn't support chunked response

* TableScan in cases hits IndexOutOfRangeException

* Cloudant search index query does not support "paging" through skip and limit.
		
* Need to improve how number of partitions is determined for parallel loading

