package com.cloudant.spark

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import com.cloudant.spark.common._
import play.api.libs.json.Json
import scalaj.http._



class CloudantReceiver(cloudantParams: Map[String, String])
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
  lazy val config: CloudantConfig = {
    JsonStoreConfigManager.getConfig(cloudantParams: Map[String, String]).asInstanceOf[CloudantConfig]
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Cloudant Receiver") {
      override def run() { receive() }
    }.start()
  }

  private def receive(): Unit = {
    val url = config.getContinuousChangesUrl()
    val selector:String = if (config.getSelector() != null) {
      "{\"selector\":" + config.getSelector() + "}"
    } else {
      "{}"
    }

    val clRequest: HttpRequest = config.username match {
      case null =>
        Http(url)
          .postData(selector)
          .timeout(connTimeoutMs = 1000, readTimeoutMs = 0)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
      case _ =>
        Http(url)
          .postData(selector)
          .timeout(connTimeoutMs = 1000, readTimeoutMs = 0)
          .header("Content-Type", "application/json")
          .header("User-Agent", "spark-cloudant")
          .auth(config.username, config.password)
    }

    clRequest.exec((code, headers, is) => {
      if (code == 200) {
        scala.io.Source.fromInputStream(is, "utf-8").getLines().foreach(line => {
          if (line.length() > 0) {
            val json = Json.parse(line)
            val jsonDoc = (json \ "doc").get
            val doc = Json.stringify(jsonDoc)
            store(doc)
          }
        })
      } else {
        val status = headers.getOrElse("Status", IndexedSeq.empty)
        val errorMsg = "Error retrieving _changes feed for a database " + config.getDbname() + ": " + status(0)
        reportError(errorMsg, new RuntimeException(errorMsg))
      }
    })
  }

  def onStop() = {
  }
}
