## Project for Spark SQL Cloudant External Datasource -- Build

### The folder structure:

#### Scala projects:

	./cloudant-spark-sql folder

contains the integration of clouding as spark-sql external datasource
   				

	./examples/scala folder
		
contains various test cases for exploring spark api:

* mytest.spark.CloudantApp: cloudant as spark-sql external datastore application
* mytest.spark.CloudantDF: cloudant as spark-sql external datastore application using DataFrame
* mytest.spark.CloudantDFOption: cloudant as spark-sql external datastore application using DataFrame with option


#### Python project:

	./python folder
		
Contains various test cases for exploring spark api:

* CloudantApp.py : Cloudant as spark-sql external datastore application , similar to test class in Scala
* CloudantDF.py : Cloudant as spark-sql external datastore DataFrame application using DataFrame
* CloudantDFOption.py : Cloudant as spark-sql external datastore DataFrame application using DataFrame with option
		

### To work with Scala projects

#### Things enabled

* spark, hadoop, spray, play-json under build.sbt
* plugin for eclipse and assembly under project/plugins.sbt


#### To import into eclipse:

assume at each project folder

	sbt eclipse 
	eclipse -> import "Existing project into workspace" of project directory



### To build 

#### Project cloudant-spark-sql

assume at cloudant-spark-sql folder
	
	sbt assembly

create all-inclusive cloudant-spark.jar (except spark and hadoop) under target/scala-2.10 and be used for job submission
	
	
#### Project /examples/scala
	
assume at /examples/scala folder
		
	sbt package
			
create application jar: target/scala-2.10/spark_test_2.10-0.1-SNAPSHOT.jar and be used for job submission together with cloudant-spark.jar
			

