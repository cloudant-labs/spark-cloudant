Spark SQL Cloudant External Datasource
================

Cloudant integration with Spark as Spark SQL external datasource. 

##  Contents:
- [Implementation of RelationProvider](#id-section1)
- [Binary download](#id-section2)
- [Build from source](#id-section3)
- [Sample application ](#id-section4)
- [Job Submission](#id-section5)
- [Configuration Overview](#id-section6)
- [Troubleshooting](#id-section7)
- [Known limitations and areas for improvement] (#id-section8)

<div id='id-section1'/>
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
 

<div id='id-section2'/>
### Binary download:

Spark Version | Release # | Binary Location
--- | --- | ---
1.3.0 | v0.1 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v0.1/cloudant-spark.jar)
1.3.1 | v1.3.1.2 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v1.3.1.2/cloudant-spark.jar)
1.4.0 | v1.4.0.0 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/1.4.0.0/cloudant-spark.jar)
1.4.1 | v1.4.1.3 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v1.4.1.3/cloudant-spark.jar)
1.4.1 | v1.4.1.4 | [Location] (https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.4.1.4/cloudant-spark-1.4.1.jar)
1.4.1 | v1.4.1.5 | [Location] (https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.4.1.5/cloudant-spark.jar)
1.6.0 | v1.6.1.0 | [Location] (https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.6.1/cloudant-spark-v1.6.1-43.jar)


<div id='id-section3'/>
### Build from source:

[Instructions](README_build.md)
	

<div id='id-section4'/>
## Sample application 

### Using SQL In Python 
	
[python code](examples/python/CloudantApp.py)

```python
conf = SparkConf().setAppName("Cloudant Spark SQL External Datasource in Python")
	
# define cloudant related configuration:
# set protocol to http if needed, default value=https
# conf.set("cloudant.protocol","http")
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password","PASSWORD")
	
# create Spark context and SQL context
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
	
# create temp table
sqlContext.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark OPTIONS ( database 'airportcodemapping')")
      
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
// set protocol to http if needed, default value=https
// conf.set("cloudant.protocol","http")	
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password","PASSWORD")
    
// create Spark context and SQL context
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext._
    
// Create a temp table 
sqlContext.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark OPTIONS ( database 'airportcodemapping'")
  
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


<div id='id-section5'/>
	
## Job Submission

[Details](README_submit.md)


### For Python
	
	spark-submit  --master local[4] --jars <path to cloudant-spark.jar>  <path to python script> 


### For Scala

	spark-submit --class "<your class>" --master local[4] --jars <path to cloudant-spark.jar> <path to your app jar>
		
		
<div id='id-section6'/>

## Configuration Overview	
The configuration is obtained in the following sequence:

1. default in the Config, which is set in the application.conf
2. key in the SparkConf, which is set in SparkConf
3. key in the parameters, which is set in a dataframe or temporaty table options
4. "spark."+key in the SparkConf (as they are treated as the one passed in through spark-submit using --conf option)

Here each subsequent configuration overrides the previous one. Thus, configuration set using DataFrame option overrides what has beens set in SparkConf. And configuration passed in spark-submit using --conf takes precedence over any setting in the code.



### Cofiguration in application.conf
Default values are defined in [here](cloudant-spark-sql/src/main/resources/application.conf)

### Configuration on SparkConf

Name | Default | Meaning
--- |:---:| ---
cloudant.protocol|https|protocol to use to transfer data: http or https
cloudant.host||cloudant host url
cloudant.username||cloudant userid
cloudant.password||cloudant password
jsonstore.rdd.partitions|5|the number of partitions intent used to drive JsonStoreRDD loading query result in parallel. The actual number is calculated based on total rows returned and satisfying maxInPartition and minInPartition
jsonstore.rdd.maxInPartition|-1|the max rows in a partition. -1 means unlimited
jsonstore.rdd.minInPartition|10|the min rows in a partition.
jsonstore.rdd.requestTimeout|100000| the request timeout in milli-second
jsonstore.rdd.bulkSize|20| the bulk save size
jsonstore.rdd.schemaSampleSize|1| the sample size for RDD schema discovery. 1 means first document; -1 means all document; 0 will be treated as 1; N means min(n, total) document 



###  Configuration on Spark SQL Temporary Table or DataFrame

Besides all the configurations passed to a temporary table or dataframe through SparkConf, it is also possible to set the following configurations in temporary table or dataframe using OPTIONS: 

Name | Default | Meaning
--- |:---:| ---
database||cloudant database name
view||cloudant view w/o the database name. only used for load.
index||cloudant search index w/o the database name. only used for load data with less than or equal to 200 results.
path||cloudant: as database name if database is not present

For fast loading, views are loaded without include_docs. Thus, a derived schema will always be: `{id, key, value}`, where `value `can be a compount field. An example of loading data from a view: 

```python
sqlContext.sql(" CREATE TEMPORARY TABLE flightTable1 USING com.cloudant.spark OPTIONS ( database 'n_flight', view '_design/view/_view/AA0')")

```

###  Configuration in spark-submit using --conf option

The above stated configuration keys can also be set using `spark-submit --conf` option. When passing configuration in spark-submit, make sure adding "spark." as prefix to the keys.

<div id='id-section7'/>
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
conf = SparkConf().setAppName("Multiple schema test")

conf.set("cloudant.host","<ACCOUNT>.cloudant.com")
conf.set("cloudant.username", "<USERNAME>")
conf.set("cloudant.password","<PASSWORD>")
conf.set("jsonstore.rdd.schemaSampleSize", -1)

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
```
For a local setting applied to a single RDD only, use:

``` python
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


<div id='id-section8'/>

## Known limitations and areas for improvement

* Loading data from Cloudant search index will work only for up to 200 results.
		
* Need to improve how number of partitions is determined for parallel loading

