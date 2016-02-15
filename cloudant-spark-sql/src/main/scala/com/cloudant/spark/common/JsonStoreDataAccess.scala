/*******************************************************************************
* Copyright (c) 2015 IBM Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************************/
package com.cloudant.spark.common


import com.cloudant.spark.{JsonUtil, CloudantConfig}
import spray.client.pipelining._
import scala.concurrent._
import akka.event.Logging
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import scala.collection.mutable.HashMap
import play.api.libs.json.JsNull
import org.apache.spark.sql.sources._
import org.apache.spark.SparkEnv
import spray.httpx.marshalling.Marshaller
import play.api.libs.json.JsString
import com.typesafe.config.ConfigFactory
import scala.util.Random
import scala.concurrent.Future

import akka.actor.ActorSystem
import akka.util.Timeout
import akka.pattern.ask
import akka.io.IO

import spray.can.Http
import spray.http._
import HttpMethods._

/**
 * @author yanglei
 */
class JsonStoreDataAccess (config: CloudantConfig)  {
  implicit lazy val timeout = {Timeout(config.requestTimeout)}
  lazy val envSystem = {SparkEnv.get.actorSystem}
  lazy val logger = {Logging(envSystem, getClass)}

  private lazy val validCredentials: BasicHttpCredentials = {
    if (config.username !=null) BasicHttpCredentials(config.username, 
      config.password)
    else null
  }

  def getOne()( implicit columns: Array[String] = null) = {
    var r = this.getQueryResult[Seq[String]](config.getOneUrlExcludeDDoc1(), processAll)
    if (r.size == 0 ){
      r = this.getQueryResult[Seq[String]](config.getOneUrlExcludeDDoc2(), processAll)
    }
    if (r.size == 0) {
      throw new RuntimeException("Database " + config.getDbname() +
        " doesn't have any non-design documents!")
    }else {
      r
    }
  }

  def getMany(limit: Int)(implicit columns: Array[String] = null) = {
    if(limit == 0){
      throw new RuntimeException("Database " + config.getDbname() +
        " schema sample size is 0!")
    }
    if(limit < -1){
      throw new RuntimeException("Database " + config.getDbname() +
        " schema sample size is " + limit + "!")
    }
    var r = this.getQueryResult[Seq[String]](config.getAllDocsUrl(limit), processAll)
    if (r.size == 0) {
      r = this.getQueryResult[Seq[String]](config.getAllDocsUrlExcludeDDoc(limit), processAll)
    }
    if (r.size == 0) {
      throw new RuntimeException("Database " + config.getDbname() +
        " doesn't have any non-design documents!")
    }else {
      r
    }
  }

  def getAll[T](url: String)
      (implicit columns: Array[String] = null,
      attrToFilters: Map[String, Array[Filter]] =null) = {
    this.getQueryResult[Seq[String]](url,processAll)
  }

  def getIterator(skip: Int, limit: Int, url: String)
      (implicit columns: Array[String] = null,
      attrToFilters: Map[String, Array[Filter]] = null) = {
    implicit def convertSkip(skip: Int): String  = {
      val url = config.getLastUrl(skip)
      if (url == null) 
        skip.toString()
      else  
        this.getQueryResult[String](url,
          {result => config.getLastNum(Json.parse(result)).as[JsString].value})
    }
    val newUrl = config.getSubSetUrl(url,skip, limit)
    this.getQueryResult[Iterator[String]](newUrl, processIterator)
  }

  def getTotalRows(url: String) : Int ={
    val totalUrl = config.getTotalUrl(url)
    this.getQueryResult[Int](totalUrl,
        {result => config.getTotalRows(Json.parse(result))})
  }

  private def processAll (result: String)
      (implicit columns: Array[String], 
      attrToFilters: Map[String, Array[Filter]] = null)= {
    logger.debug(s"processAll columns:$columns, attrToFilters:$attrToFilters")
    val jsonResult = Json.parse(result)
    var rows = config.getRows(jsonResult)
    if (config.viewName == null) {
      //filter design docs
      rows = rows.filter(r => FilterDDocs.filter(r))
    }
    rows.map(r => convert(r))
  }
      
  private def processIterator (result: String)
    (implicit columns: Array[String], 
    attrToFilters: Map[String, Array[Filter]] =null)= {
    processAll(result).iterator
  }

  private def convert(rec:JsValue)(implicit columns: Array[String]): String = {
    if (columns == null) return Json.stringify(Json.toJson(rec))
    val m = new HashMap[String, JsValue]()
    for ( x <-columns){
        val field = JsonUtil.getField(rec, x).getOrElse(JsNull)
        m.put(x,  field)
    }
    val result = Json.stringify(Json.toJson(m.toMap))
    logger.debug(s"converted: $result")
    result
  }

  private def getQueryResult[T](url: String, postProcessor:(String) => T)
      (implicit columns: Array[String] = null, 
      attrToFilters: Map[String, Array[Filter]] =null) : T={
    logger.warning("Loading data from Cloudant using query: "+ url)
    implicit val system = config.getSystem()

    val request: HttpRequest = if (validCredentials != null) {
      Get(url) ~> addCredentials(validCredentials)
    } else {
      HttpRequest(GET, Uri(url))
    }

    val response: Future[HttpResponse] =
      (IO(Http) ? request).mapTo[HttpResponse]
    val result = Await.result(response, timeout.duration)
    if (result.status.isFailure){
      throw new RuntimeException("Database " + config.getDbname() +
        " request error: " + result.entity.asString)
    }
    val data = postProcessor(result.entity.asString)
    logger.debug(s"got result:$data")
    data
  }

  def saveAll(rows: List[String]) {
    implicit val system = config.getSystem()
    import system.dispatcher 
    
    val useBulk = (config.getBulkPostUrl() != null && config.bulkSize>1)
    val bulkSize = if (useBulk) config.bulkSize else 1
    val bulks = rows.grouped(bulkSize).toList
    val totalBulks = bulks.size
    logger.info(s"total records:${rows.size}=bulkSize:$bulkSize * totalBulks:$totalBulks, execute in parallel $totalBulks")
    
    val url = if (bulkSize>1) config.getBulkPostUrl() else config.getPostUrl()
    logger.info(s"Post:$url")
    
    if (url == null) return
    import ContentTypes._
    implicit val stringMarshaller = Marshaller.of[String](`application/json`) {
      (value, ct, ctx) => ctx.marshalTo(HttpEntity(ct, value))
    }
    // Better parallelism 
    val allFutures : List[Future[HttpResponse]] =  { 
        for (bulk <- bulks) yield {
          val  x: String = if (bulkSize >1) config.getBulkRows(bulk) else bulk(0)
          logger.debug(s"content:$x")
          var pipeline: HttpRequest => Future[HttpResponse] = null
          if (validCredentials!=null)
          {
            pipeline = ( 
            addCredentials(validCredentials) 
            ~> sendReceive
            )
          }else
          {
            pipeline = sendReceive
          }
          val request = Post(url,x)
          val response: Future[HttpResponse] = pipeline(request)
          response
        }
    }
    val f = Future.sequence(allFutures.toList)
    val result = Await.result(f, timeout.duration)
    var reason : String = null
    val isSuccessful= result.forall { x => if( !x.status.isSuccess ) reason= x.status.reason;x.status.isSuccess }
    logger.info(s"Save total ${totalBulks}=${result.length} successful=$isSuccessful")
    if (!isSuccessful)
    {
         throw new RuntimeException(s"Database ${config.getDbname()} faield with $reason")
    }
  }
}

