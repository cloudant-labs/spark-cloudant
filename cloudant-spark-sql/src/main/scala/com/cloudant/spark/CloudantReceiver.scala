package com.cloudant.spark

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import com.cloudant.spark.common._
import java.io.{BufferedReader, InputStreamReader}

import play.api.libs.json.Json
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClients}
import org.apache.http.HttpEntity
import org.apache.http.client.CredentialsProvider
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.entity.StringEntity


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
    var url = config.getContinuousChangesUrl()
    val httpclient:CloseableHttpClient =  if (config.username == null) {
      HttpClients.createDefault()
    }else {
      val credsProvider:CredentialsProvider  = new BasicCredentialsProvider()
      credsProvider.setCredentials(
        new AuthScope(null, -1),
        new UsernamePasswordCredentials(config.username, config.password))
      HttpClients.custom()
        .setDefaultCredentialsProvider(credsProvider)
        .build()
    }

    val req: HttpPost = new HttpPost(url)
    req.setHeader("Content-Type", "application/json")
    req.setHeader("User-Agent", "spark-cloudant")
    val selector = config.getSelector()
    if (selector != null) {
      val strEntity = "{\"selector\":" + selector + "}"
      val entity: StringEntity = new StringEntity(strEntity)
      req.setEntity(entity)
    }

    val response: CloseableHttpResponse = httpclient.execute(req)
    try {
      val sl = response.getStatusLine()
      if (sl.getStatusCode != 200){
        val errorMsg = "Error retrieving _changes: " + sl.getStatusCode + " " + sl.getReasonPhrase()
        stop(errorMsg)
      } else {
        val entity: HttpEntity = response.getEntity()
        val reader: BufferedReader = new BufferedReader(new InputStreamReader(
          entity.getContent(), "UTF-8"))
        var line: String = reader.readLine()
        while (!isStopped() && line != null) {
          if (line.length() > 0) {
            val json = Json.parse(line)
            val jsonDoc = json \ "doc"
            val doc = Json.stringify(jsonDoc)
            store(doc)
          }
          line = reader.readLine()
        }
      }
    } finally {
      response.close()
    }
  }


  def onStop() = {
    config.shutdown()
  }
}