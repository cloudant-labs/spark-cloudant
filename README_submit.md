## Project for Spark SQL Cloudant External Datasource -- Submission

### Submit the sample applications

assume at root folder and cloudant-spark.jar is copied here

#### For Python:

submission with flavors

##### with local slave
		
	spark-submit  --master local[4]  --jars cloudant-spark.jar python/CloudantApp.py
	spark-submit  --master local[4]  --jars cloudant-spark.jar python/RiakApp.py
	spark-submit  --master local[4]  --jars cloudant-spark.jar python/CloudantDF.py


##### with mesos master
		
	replace --master with your mesos cluster URL. e.g. mesos://127.0.0.1:5050  

	
##### with spark master
		
	replace --master with spark cluster URL. e.g. spark://<spark host>:7077 
		


#### For Scala

submission with flavors

##### with local slave
		
	spark-submit --class "mytest.spark.CloudantApp" --master local[4] --jars cloudant-spark.jar spark-test/target/scala-2.10/spark_test_2.10-0.1-SNAPSHOT.jar
	
	
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
* Spark worker dashboard at: http://<worker host ip>:<worker returned port>
		    
