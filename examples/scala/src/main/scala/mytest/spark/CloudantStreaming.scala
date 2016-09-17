package mytest.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf 
import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.rdd.RDD
import com.cloudant.spark.CloudantReceiver
import org.apache.spark.streaming.scheduler.{ StreamingListener, StreamingListenerReceiverError}

object CloudantStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource in Scala")
    // Create the context with a 10 seconds batch size
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

        var hasDelRecord = false
        var hasAirportNameField = false
        for (field <- changesDataFrame.schema.fieldNames) {
          if ("_deleted".equals(field)) {
            hasDelRecord = true
          }
          if ("airportName".equals(field)) {
            hasAirportNameField = true
          }
        }
        if (hasDelRecord) {
          changesDataFrame.filter(changesDataFrame("_deleted")).select("*").show()
        }

        if (hasAirportNameField) {
          changesDataFrame.filter(changesDataFrame("airportName") >= "Paris").select("*").show()
          changesDataFrame.registerTempTable("airportcodemapping")
          val airportCountsDataFrame =
            spark.sql("select airportName, count(*) as total from airportcodemapping group by airportName")
          airportCountsDataFrame.show()
        }
      }

    })
    ssc.start()
    // run streaming for 120 secs
    Thread.sleep(120000L)
    ssc.stop(true)
  }
}

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {
  @transient  private var instance: SparkSession = _
  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
