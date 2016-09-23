## Project for Spark SQL Cloudant External Datasource -- Build

## The folder structure:

### Scala projects:

	./cloudant-spark-sql folder

contains the integration of clouding as spark-sql external datasource
   				

	./examples/scala folder
		
contains various test cases for exploring spark api:

* mytest.spark.CloudantApp: cloudant as spark-sql external datastore application
* mytest.spark.CloudantDF: cloudant as spark-sql external datastore application using DataFrame
* mytest.spark.CloudantDFOption: cloudant as spark-sql external datastore application using DataFrame with option
* mytest.spark.CloudantStreaming: cloudant as spark-sql external datastore application using StreamingContext


### Python project:

	./python folder
		
Contains various test cases for exploring spark api:

* CloudantApp.py : Cloudant as spark-sql external datastore application , similar to test class in Scala
* CloudantDF.py : Cloudant as spark-sql external datastore DataFrame application using DataFrame
* CloudantDFOption.py : Cloudant as spark-sql external datastore DataFrame application using DataFrame with option
		

## To work with Scala projects

#### Things enabled

* Spark 2.0.0
* Scala 11.x
* sbt 0.13


#### To import into eclipse:

assume at root folder

	sbt eclipse 
	eclipse -> import "Existing project into workspace" of project directory



## To make a build 

### Project cloudant-spark-sql

assume at cloudant-spark-sql folder
	
	sbt assembly

create one all-inclusive cloudant-spark.jar under target/scala-2.11 to be used for job submission. All-inclusive jar will contain all dependencies except spark and hadoop that should be installed separately.
	
	
### Project /examples/scala
	
assume at root folder
		
	sbt package
			
will create application jar: `/examples/scala/target/scala-2.11/spark_test_2.11-2.0.0.jar` needed for job submission together with cloudant-spark.jar 
			

