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

import spray.http._
import spray.client.pipelining._
import scala.concurrent._
import akka.actor.ActorSystem
import akka.event.Logging
import scala.util.{Success, Failure}
import akka.actor.Actor
import akka.util.Timeout
import play.api.libs.json.Json
import play.api.libs.json.JsArray
import play.api.libs.json.JsValue
import scala.collection.mutable.HashMap
import play.api.libs.json.JsNull
import play.api.libs.json.JsUndefined
import play.api.libs.json.JsNumber
import org.apache.spark.sql.sources._
import org.apache.spark.SparkEnv
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import spray.httpx.marshalling.Marshaller
import spray.http.StatusCodes.Success


/**
 * @author yanglei
 */
class JsonStoreDataAccess (config: JsonStoreConfig)  {

      implicit val timeout = Timeout(JsonStoreConfigManager.timeoutInMillis)
      implicit val system = SparkEnv.get.actorSystem
      import system.dispatcher 

      val logger = Logging(system, getClass)
      
      private lazy val validCredentials: BasicHttpCredentials = {
            if (config.username !=null) BasicHttpCredentials(config.username, config.password)
            else null
      }


      def getOne()( implicit columns: Array[String] = null) = {
          this.getQueryResult[Seq[String]](config.getOneUrl,processAll)
      }
      
      def getAll[T](url: String )(implicit columns: Array[String] =null,attrToFilters: Map[String, Array[Filter]] =null) ={
          this.getQueryResult[Seq[String]](url,processAll)
      }

      def getIterator(skip: Int, limit: Int, url: String)(implicit columns: Array[String] =null,attrToFilters: Map[String, Array[Filter]] =null) ={
        val newUrl = config.getSubSetUrl(url,skip, limit)
        this.getQueryResult[Iterator[String]](newUrl,processIterator)
      }

      def getTotalRows(url: String) : Int ={
          this.getQueryResult[Int](url,{result => config.getTotalRows(Json.parse(result)).as[JsNumber].value.intValue()})
      }
      private def processAll (result: String)( implicit columns: Array[String], attrToFilters: Map[String, Array[Filter]] =null)= {
          val json = Json.parse(result)
          var rows = config.getRows(json )
          if (attrToFilters != null)
          {
            val util = new FilterUtil(attrToFilters)
            rows = rows.filter(r => util.apply(r))
          }
          rows.map(r =>  convert(r))
      }
      
      private def processIterator (result: String)( implicit columns: Array[String], attrToFilters: Map[String, Array[Filter]] =null)= {
        processAll(result).iterator
      }


      private def convert(rec:JsValue)(implicit columns: Array[String]): String = {
        if (columns ==null) return Json.stringify(Json.toJson(rec))
        val m = new HashMap[String, JsValue]()
        for ( x <-columns)
        {
            val field = JsonUtil.getField(rec, x).getOrElse(JsNull)
            m.put(x,  field)
        }
        val result = Json.stringify(Json.toJson(m.toMap))
        logger.debug(s"converted: $result")
        result
      }
      
      private def  getQueryResult[T](url: String, postProcessor:(String) => T)(implicit columns: Array[String] =null, attrToFilters: Map[String, Array[Filter]] =null) : T={
          logger.info("Cloudant query: "+ url)

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
          val response: Future[HttpResponse] = pipeline(Get(url))
          val result = Await.result(response, timeout.duration)
          val data = postProcessor(result.entity.asString)
          logger.debug(s"got result:$data")
          data
      }
      
      def saveAll(data: Array[String]) {
          val url =config.getPostUrl()
          logger.info(s"Post:$url")
            import ContentTypes._
            implicit val stringMarshaller = Marshaller.of[String](`application/json`) {
              (value, ct, ctx) => ctx.marshalTo(HttpEntity(ct, value))
            }
          val allFutures = data.map { x => 
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
          val f= Future.sequence(allFutures.toList)
          val result = Await.result(f, timeout.duration)
           logger.info(s"Save result:$result")
      }
}

