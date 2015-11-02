Spark SQL Cloudant External Datasource
================

Cloudant integration with Spark as Spark SQL external datasource. Most of the implementation can be re-used by other JsonStore, e.g. Riak. 


## Contents:

### RelationProvider implementations

All implementions are under [here] (cloudant-spark-sql/src/main/scala/com/cloudant/spark)

#### Cloudant

Relation Provider Name | Table Option | Scan Type | Column Pruning | Predicates Push Down | Parallel Loading | Insertable | Source Code Location
--- | --- | --- | --- | --- | --- | --- | --- 
com.cloudant.spark.DefaultSource|database or path, index|PrunedFilteredScan| Yes |_id or first predicate | Yes, except with index | Yes | [DefaultSource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/DefaultSource.scala)
com.cloudant.spark.CloudantRP|database|TableScan| No | No | No | No | [CloudantDatasource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/CloudantDatasource.scala)
com.cloudant.spark.CloudantPrunedFilteredRP|database|PrunedFilteredScan| Yes |_id or first predicate | No | No | [CloudantPrunedFilteredDatasource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/CloudantPrunedFilteredDatasource.scala)
com.cloudant.spark.CloudantPartitionedPrunedFilteredRP|database, index|PrunedFilteredScan| Yes |_id or first predicate |  Yes, except with index  | No |[CloudantPartitionedPrunedFilteredDatasource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/CloudantPartitionedPrunedFilteredDatasource.scala)


#### Riak

Relation Provider Name | Table Option | Scan Type | Column Pruning | Predicates Push Down | Parallel Loading | Insertable | Source Code Location
--- | --- | --- | --- | --- | --- | --- | ---
com.cloudant.spark.CloudantPartitionedPrunedFilteredRP|path|PrunedFilteredScan| Yes | first predicate | Yes | No |[RiakPartitionedPrunedFilteredDatasource.scala](cloudant-spark-sql/src/main/scala/com/cloudant/spark/riak/RiakPartitionedPrunedFilteredDatasource.scala)



### Binary download:

Spark Version | Release # | Binary Location
--- | --- | ---
1.3.0 | v0.1 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v0.1/cloudant-spark.jar)
1.3.1 | v1.3.1.2 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v1.3.1.2/cloudant-spark.jar)
1.4.0 | v1.4.0.0 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/1.4.0.0/cloudant-spark.jar)
1.4.1 | v1.4.1.3 | [Location] (https://github.com/cloudant/spark-cloudant/releases/download/v1.4.1.3/cloudant-spark.jar)
1.4.1 | v1.4.1.4 | [Location] (https://github.com/cloudant-labs/spark-cloudant/releases/download/v1.4.1.4/cloudant-spark-1.4.1.jar)
1.5.1 | v1.5.1.0 | [Location] (https://github.com/yanglei99/spark-cloudant/releases/download/v1.5.1.0/cloudant-spark.jar)


### Build from source:

[Instructions](README_build.md)
	


## Sample application 

### Using SQL In Python 
	
[python code](python/CloudantApp.py)
	
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


### Using SQL In Scala 

[Scala code](spark-test/src/main/scala/mytest/spark/CloudantApp.scala)
	
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


### Using DataFrame In Python 

[python code](python/CloudantDF.py). 
	    
	conf = SparkConf().setAppName("Cloudant Spark SQL External Datasource in Python")
	# define coudant related configuration
	conf.set("cloudant.host","ACCOUNT.cloudant.com")
	conf.set("cloudant.username", "USERNAME")
	conf.set("cloudant.password","PASSWORD")
	
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	
	df = sqlContext.load("airportcodemapping", "com.cloudant.spark")
	df.printSchema()
	
	df.filter(df.airportCode >= 'CAA').select("airportCode",'airportName').save("airportcodemapping_df", "com.cloudant.spark")	    
	
[Sample code on using DataFrame option to define cloudant configuration](python/CloudantDFOption.py)
	
### Using DataFrame In Scala 

[Scala code](spark-test/src/main/scala/mytest/spark/CloudantDF.scala)
	
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
     df.printSchema()

     df.filter(df("airportCode") >= "CAA").select("airportCode","airportName").show()
     df.filter(df("airportCode") >= "CAA").select("airportCode","airportName").write.format("com.cloudant.spark").save("airportcodemapping_df")
     

[Sample code on using DataFrame option to define cloudant configuration](spark-test/src/main/scala/mytest/spark/CloudantDFOption.scala)

### Note: 
	
In the above table creation, you can replace "USING com.cloudant.spark.CloudantRP" with other RelationProvider implementations

	
	
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
riak.host|| riak ip address
riak.port|| riak port
jsonstore.rdd.partitions|5|the number of partitions intent used to drive JsonStoreRDD loading query result in parallel. The actual number is calculated based on total rows returned and satisfying maxInPartition and minInPartition
jsonstore.rdd.maxInPartition|-1|the max rows in a partition. -1 means unlimited
jsonstore.rdd.minInPartition|10|the min rows in a partition.
jsonstore.rdd.requestTimeout|100000| the request timeout in milli-second
jsonstore.rdd.concurrentSave|-1| the parallel saving size. -1 means unlimited
jsonstore.rdd.bulkSize|1| the bulk save size. 

Default values are defined in [here](cloudant-spark-sql/src/main/resources/application.conf)

### Configuration on Spark SQL temp table

Configuration can also be passed on DataFrame using option.

Name | Default | Meaning
--- |:---:| ---
database||cloudant database name
index||cloudant search index w/o the database name.only used for load.
path||riak: search index name; cloudant: as database name if database does not present


## Known limitations and improvement areas

* Chunked response is not supported. For parallel partitioned loading, issue can be workarouded by setting jsonstore.rdd.maxInPartition.  The following is the exception when query result is over limit.
	HttpClientConnection: Aggregated response entity greater than configured limit of 1048576 bytes,closing connection
	java.lang.RuntimeException: sendReceive doesn't support chunked response

* TableScan in cases hits IndexOutOfRangeException 

* Schema is calculated on the first document w/o any predicate push down. Need a better approach

* Cloudant search index query does not support "paging" through skip and limit.
		
* Need to improve how number of partitions is determined for parallel loading

