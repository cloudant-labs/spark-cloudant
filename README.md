Spark Cloudant Connector
================

Cloudant integration with Spark as Spark SQL external datasource, and Spark Streaming as a custom receiver. 

##  Contents:
1. [Implementation of RelationProvider](#implementation-of-relationProvider)
2. [Implementation of Receiver](#implementation-of-Receiver)
3. [Binary download](#Binary-download)
4. [Build from source](#Build-from-source)
5. [Sample applications](#Sample-application)
    1. [Using SQL In Python](#Using-SQL-In-Python)
    2. [Using SQL In Scala](#Using-SQL-In-Scala)
    3. [Using DataFrame In Python](#Using-DataFrame-In-Python)
    4. [Using DataFrame In Scala](#Using-DataFrame-In-Scala)
    5. [Using Streams In Scala](#Using-Streams-In-Scala)
6. [Job Submission](#Job-Submission)
7. [Configuration Overview](#Configuration-Overview)
8. [Troubleshooting](#Troubleshooting)
9. [Known limitations and areas for improvement] (#Known-limitations)

### Implementation of RelationProvider

[DefaultSource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/DefaultSource.scala) is a RelationProvider for loading data from Cloudant to Spark, and saving it back from Cloudant to Spark.  It has the following functionalities:

Functionality | Value 
--- | --- | ---
Table Option | database or path, search index, view 
Scan Type | PrunedFilteredScan | 
Column Pruning | yes
Predicates Push Down | _id or first predicate 
Parallel Loading | yes, except with search index
 Insertable | yes
 
### Implementation of Receiver

Spark Cloudant connector creates a discretized stream in Spark (Spark input DStream) out of Cloudant data sources. [CloudantReceiver.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/CloudantReceiver.scala) is a custom Receiver that converts `_changes` feed from a Cloudant database to DStream in Spark. This allows all sorts of processing on this streamed data including [using DataFrames and SQL operations on it](examples/scala/src/main/scala/mytest/spark/CloudantStreaming.scala).

**NOTE:** Since CloudantReceiver for Spark Streaming is based on `_changes` API, there are some limitations that application developers should be aware of. Firstly, results returned from `_changes` are partially ordered, and may not be presented in order in which documents were updated. Secondly, in case of shards' unavailability, you may see duplicates, changes that have been seen already. Thus, it is up to applications using Spark Streaming with CloudantReceiver to keep track of _changes they have processed and detect duplicates. 

### Binary downloads:
The current release is 2.0.0 for Spark 2.0 and Scala 11.

The latest release for Spark 1.6 is 1.6.4 is available [here]
(https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.6.4/cloudant-spark-v1.6.4-167.jar). It is tested to work with Spark 1.6.


### Build from source:

[Instructions](README_build.md)

## Sample applications

### Using SQL In Python 
	
[CloudantApp.py](examples/python/CloudantApp.py)

```python
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using temp tables")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .getOrCreate()


# Loading temp table from Cloudant db
spark.sql(" CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping')")
airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
for code in airportData.collect():
    print code._id
```

### Using SQL In Scala 


[CloudantApp.scala](examples/scala/src/main/scala/mytest/spark/CloudantApp.scala)

```scala
val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example")
      .config("cloudant.host","ACCOUNT.cloudant.com")
      .config("cloudant.username", "USERNAME")
      .config("cloudant.password","PASSWORD")
      .getOrCreate()

// For implicit conversions of Dataframe to RDDs
import spark.implicits._
    
// create a temp table from Cloudant db and query it using sql syntax
spark.sql(
    s"""
    |CREATE TEMPORARY TABLE airportTable
    |USING com.cloudant.spark
    |OPTIONS ( database 'n_airportcodemapping')
    """.stripMargin)
// create a dataframe
val airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
println(s"Total # of rows in airportData: " + airportData.count())
// convert dataframe to array of Rows, and process each row
airportData.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println)
```

### Using DataFrame In Python 

[CloudantDF.py](examples/python/CloudantDF.py). 

```python	    
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .config("jsonstore.rdd.partitions", 8)\
    .getOrCreate()

# ***1. Loading dataframe from Cloudant db
df = spark.read.load("n_airportcodemapping", "com.cloudant.spark")
df.cache() 
df.printSchema()
df.filter(df.airportName >= 'Moscow').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').show()	    
```
	
In case of doing multiple operations on a dataframe (select, filter etc.),
you should persist a dataframe. Otherwise, every operation on a dataframe will load the same data from Cloudant again.
Persisting will also speed up computation. This statement will persist an RDD in memory: `df.cache()`.  Alternatively for large dbs to persist in memory & disk, use: 

```python
from pyspark import StorageLevel
df.persist(storageLevel = StorageLevel(True, True, False, True, 1))
```

[Sample code on using DataFrame option to define cloudant configuration](examples/python/CloudantDFOption.py)

### Using DataFrame In Scala 

[CloudantDF.scala](examples/scala/src/main/scala/mytest/spark/CloudantDF.scala)

```	scala
val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example with Dataframe")
      .config("cloudant.host","ACCOUNT.cloudant.com")
      .config("cloudant.username", "USERNAME")
      .config("cloudant.password","PASSWORD")
      .config("createDBOnSave","true") // to create a db on save
      .config("jsonstore.rdd.partitions", "20") // using 20 partitions
      .getOrCreate()
          
// 1. Loading data from Cloudant db
val df = spark.read.format("com.cloudant.spark").load("n_flight")
// Caching df in memory to speed computations
// and not to retrieve data from cloudant again
df.cache() 
df.printSchema()

// 2. Saving dataframe to Cloudant db
val df2 = df.filter(df("flightSegmentId") === "AA106")
    .select("flightSegmentId","economyClassBaseCost")
df2.show()
df2.write.format("com.cloudant.spark").save("n_flight2")
```
    
[Sample code on using DataFrame option to define cloudant configuration](examples/scala/src/main/scala/mytest/spark/CloudantDFOption.scala)
 
 
### Using Streams In Scala 
[CloudantStreaming.scala](examples/scala/src/main/scala/mytest/spark/CloudantStreaming.scala)

```scala
val ssc = new StreamingContext(sparkConf, Seconds(10))
val changes = ssc.receiverStream(new CloudantReceiver(Map(
  "cloudant.host" -> "ACCOUNT.cloudant.com",
  "cloudant.username" -> "USERNAME",
  "cloudant.password" -> "PASSWORD",
  "database" -> "n_airportcodemapping")))

changes.foreachRDD((rdd: RDD[String], time: Time) => {
  // Get the singleton instance of SparkSession
  val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

  println(s"========= $time =========")
  // Convert RDD[String] to DataFrame
  val changesDataFrame = spark.read.json(rdd)
  if (!changesDataFrame.schema.isEmpty) {
    changesDataFrame.printSchema()
    changesDataFrame.select("*").show()
    ....
  }
})
ssc.start()
// run streaming for 120 secs
Thread.sleep(120000L)
ssc.stop(true)
	
```

By default, Spark Streaming will load all documents from a database. If you want to limit the loading to specific documents, use `selector` option of `CloudantReceiver` and specify your conditions ([CloudantStreamingSelector.scala](examples/scala/src/main/scala/mytest/spark/CloudantStreamingSelector.scala)):

```scala
val changes = ssc.receiverStream(new CloudantReceiver(Map(
  "cloudant.host" -> "ACCOUNT.cloudant.com",
  "cloudant.username" -> "USERNAME",
  "cloudant.password" -> "PASSWORD",
  "database" -> "sales",
  "selector" -> "{\"month\":\"May\", \"rep\":\"John\"}")))
```
	
## Job Submission

[Details](README_submit.md)


### For Python
	
	spark-submit  --master local[4] --jars <path to cloudant-spark.jar>  <path to python script> 


### For Scala

	spark-submit --class "<your class>" --master local[4] --jars <path to cloudant-spark.jar> <path to your app jar>
		
		

## Configuration Overview	
The configuration is obtained in the following sequence:

1. default in the Config, which is set in the application.conf
2. key in the SparkConf, which is set in SparkConf
3. key in the parameters, which is set in a dataframe or temporaty table options
4. "spark."+key in the SparkConf (as they are treated as the one passed in through spark-submit using --conf option)

Here each subsequent configuration overrides the previous one. Thus, configuration set using DataFrame option overrides what has beens set in SparkConf. And configuration passed in spark-submit using --conf takes precedence over any setting in the code.


### Cofiguration in application.conf
Default values are defined in [here](cloudant-spark-sql/src/main/resources/application.conf).

### Configuration on SparkConf

Name | Default | Meaning
--- |:---:| ---
cloudant.protocol|https|protocol to use to transfer data: http or https
cloudant.host||cloudant host url
cloudant.username||cloudant userid
cloudant.password||cloudant password
jsonstore.rdd.partitions|10|the number of partitions intent used to drive JsonStoreRDD loading query result in parallel. The actual number is calculated based on total rows returned and satisfying maxInPartition and minInPartition
jsonstore.rdd.maxInPartition|-1|the max rows in a partition. -1 means unlimited
jsonstore.rdd.minInPartition|10|the min rows in a partition.
jsonstore.rdd.requestTimeout|900000| the request timeout in milliseconds
bulkSize|200| the bulk save size
schemaSampleSize| "-1" | the sample size for RDD schema discovery. 1 means we are using only first document for schema discovery; -1 means all documents; 0 will be treated as 1; any number N means min(N, total) docs 
createDBOnSave|"false"| whether to create a new database during save operation. If false, a database should already exist. If true, a new database will be created. If true, and a database with a provided name already exists, an error will be raised. 



###  Configuration on Spark SQL Temporary Table or DataFrame

Besides all the configurations passed to a temporary table or dataframe through SparkConf, it is also possible to set the following configurations in temporary table or dataframe using OPTIONS: 

Name | Default | Meaning
--- |:---:| ---
database||cloudant database name
view||cloudant view w/o the database name. only used for load.
index||cloudant search index w/o the database name. only used for load data with less than or equal to 200 results.
path||cloudant: as database name if database is not present
schemaSampleSize|"-1"| the sample size used to discover the schema for this temp table. -1 scans all documents
bulkSize|200| the bulk save size
createDBOnSave|"false"| whether to create a new database during save operation. If false, a database should already exist. If true, a new database will be created. If true, and a database with a provided name already exists, an error will be raised. 



For fast loading, views are loaded without include_docs. Thus, a derived schema will always be: `{id, key, value}`, where `value `can be a compount field. An example of loading data from a view: 

```python
spark.sql(" CREATE TEMPORARY TABLE flightTable1 USING com.cloudant.spark OPTIONS ( database 'n_flight', view '_design/view/_view/AA0')")

```

###  Configuration on Cloudant Receiver for Spark Streaming

Name | Default | Meaning
--- |:---:| ---
cloudant.host||cloudant host url
cloudant.username||cloudant userid
cloudant.password||cloudant passwor
database||cloudant database name
selector| all documents| a selector written in Cloudant Query syntax, specifying conditions for selecting documents. Only documents satisfying the selector's conditions will be retrieved from Cloudant and loaded into Spark.




###  Configuration in spark-submit using --conf option

The above stated configuration keys can also be set using `spark-submit --conf` option. When passing configuration in spark-submit, make sure adding "spark." as prefix to the keys.


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
 
```python
spark = SparkSession\
    .builder\
    .appName("Multiple schema test")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .config("jsonstore.rdd.schemaSampleSize", -1)\
    .getOrCreate()
```
For a local setting applied to a single RDD only, use:

``` python
spark.sql("CREATE TEMPORARY TABLE schema-test USING com.cloudant.spark OPTIONS ( schemaSampleSize '10',database 'schema-test')")
schemaTestTable = spark.sql("SELECT * FROM schema-test")
```

Acceptable values for either setting are:

-1 - scan all documents in the database (be careful! This can cause the Spark job to become very expensive!)

1 - scan only the first document in the database (the default)

N - scan an arbitrary number of documents in the database (if N is greater than the number of documents in the database, we will apply -1)

0 or any non-integer values are not permitted and will result in an error.

### Unicode support

Having non-ascii characters in your Cloudant documents requires the Python interpreter to be set to support UTF-8. Failure to set the right encoding results in errors like the one shown for a `df.show()` action:

```
df.show()
File "/Users/holger/dev/spark-1.5.2/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 256, in show
UnicodeEncodeError: 'ascii' codec can't encode character u'\xdf' in position 1198: ordinal not in range(128)
```

There are a number of ways to force the Python interpreter to use UTF-8 encoding. A simple method is to add these lines to your script:

```python
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
```

See [https://issues.apache.org/jira/browse/SPARK-11772](https://issues.apache.org/jira/browse/SPARK-11772) for details.


## Known limitations and areas for improvement

* Loading data from Cloudant search index will work only for up to 200 results.
		
* Need to improve how number of partitions is determined for parallel loading
