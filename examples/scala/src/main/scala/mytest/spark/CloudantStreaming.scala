package mytest.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming.{ Duration, StreamingContext, Time }
import org.apache.spark.rdd.RDD
import com.cloudant.spark.CloudantReceiver

object CloudantStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource in Scala")

    // Create the context with a 10 seconds batch size
    val duration = new Duration(10000)
    val ssc = new StreamingContext(sparkConf, duration)

    val changes = ssc.receiverStream(new CloudantReceiver(Map(
      "cloudant.host" -> "ACCOUNT.cloudant.com",
      "cloudant.username" -> "USERNAME",
      "cloudant.password" -> "PASSWORD",
      "database" -> "n_airportcodemapping"), duration.milliseconds / 2))

    changes.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      println(s"========= $time =========")
      // Convert RDD[String] to DataFrame
      val changesDataFrame = sqlContext.read.json(rdd)

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
            sqlContext.sql("select airportName, count(*) as total from airportcodemapping group by airportName")

          airportCountsDataFrame.show()
        }

      }

    })

    ssc.start()
    Thread.sleep(120000L)
    ssc.stop(true)
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}