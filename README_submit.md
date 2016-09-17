## Project for Spark SQL Cloudant External Datasource -- Submission

### Submit the sample applications

assume at root folder and cloudant-spark.jar is copied here

#### For Python:

submission with flavors

##### with local slave
		
	spark-submit  --master local[4]  --jars cloudant-spark.jar examples/python/CloudantApp.py
	spark-submit  --master local[4]  --jars cloudant-spark.jar examples/python/CloudantDF.py
	spark-submit  --master local[4]  --jars cloudant-spark.jar examples/python/CloudantDFOption.py

##### with mesos master
		
	replace --master with your mesos cluster URL. e.g. mesos://127.0.0.1:5050  

	
##### with spark master
		
	replace --master with spark cluster URL. e.g. spark://<spark host>:7077 
		
##### by loading a spark-cloudant package from http://spark-packages.org/package/cloudant-labs/spark-cloudant

	spark-submit  --master local[4]  --packages cloudant-labs:spark-cloudant:VERSION examples/python/CloudantApp.py


#### For Scala

submission with flavors

##### with local slave
		
	spark-submit --class "mytest.spark.CloudantApp" --master local[4] --jars cloudant-spark.jar examples/scala/target/scala-2.11/spark_test_2.11-2.0.0.jar
	spark-submit --class "mytest.spark.CloudantDF" --master local[4] --jars cloudant-spark.jar examples/scala/target/scala-2.11/spark_test_2.11-2.0.0.jar
	spark-submit --class "mytest.spark.CloudantDFOption" --master local[4] --jars cloudant-spark.jar examples/scala/target/scala-2.11/spark_test_2.11-2.0.0.jar
	spark-submit --class "mytest.spark.CloudantStreaming" --master local[4] --jars cloudant-spark.jar examples/scala/target/scala-2.11/spark_test_2.11-2.0.0.jar
	
	
##### with mesos master
		
	replace --master with your mesos cluster URL. e.g. mesos://127.0.0.1:5050  
		
		
##### with spark master
		
	replace --master with spark cluster URL. e.g. spark://<spark host>:7077 
	
	
		
### Tips on creating Spark Cluster Master

#### start master

	start-master.sh 

* get the spark master host name from the process (ps) --ip and be used by worker
* get the ip address and listening port using netstat -at | grep 7077. Note: this ip can not be used for worker


#### start worker (multiple times)
				 		 
	spark-class org.apache.spark.deploy.worker.Worker spark://<spark master host name above>:7077  


#### Dashboard

* Spark cluster dashboard at: http://<spark master host ip>:8080
* Spark worker dashboard at: http://`worker_host_ip`:`worker_returned_port`
		    
