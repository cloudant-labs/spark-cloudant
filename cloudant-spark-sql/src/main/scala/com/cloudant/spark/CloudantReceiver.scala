package com.cloudant.spark

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import com.cloudant.spark.common._
import play.api.libs.json.{ Json, JsString, JsValue, JsArray }

class CloudantReceiver(cloudantParams: Map[String, String], interval: Long = 5000L)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {

  lazy val config: CloudantConfig = {
    JsonStoreConfigManager.getConfig(cloudantParams: Map[String, String]).asInstanceOf[CloudantConfig]
  }

  lazy val dataAccess = {
    new JsonStoreDataAccess(config)
  }

  private var stopped = true

  private var lastSeq: String = "0"

  private def getRows(result: JsValue): JsArray = {
    new JsArray(((result \ "results").asInstanceOf[JsArray]).value.map { x => x \ "doc" })
  }

  private def processResults(jsonString: String): String = {
    if (jsonString != null && !jsonString.isEmpty()) {
      val jsonResult = Json.parse(jsonString)
      lastSeq = (jsonResult \ "last_seq").asInstanceOf[JsString].value
      var rows = getRows(jsonResult)
      Json.stringify(rows)
    } else {
      null
    }
  }

  def onStart() {
    new Thread("Cloudant Receiver") {
      override def run() {
        stopped = false
        while (!stopped) {
          val result = dataAccess.getChanges(config.getChangesUrl() + "&since=" + lastSeq, processResults)
          if (result != null && !stopped) {
            store(result)
          }
          Thread.sleep(interval)
        }
      }
    }.start()
  }

  def onStop() {
    stopped = true
    config.shutdown()
  }

}