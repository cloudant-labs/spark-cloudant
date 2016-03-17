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

import org.apache.spark.sql.SQLContext
import com.typesafe.config.ConfigFactory
import com.cloudant.spark.CloudantConfig
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import akka.actor.ActorSystem
import scala.concurrent.duration.Duration
import org.apache.spark.SparkConf

 object JsonStoreConfigManager
{
  private val CLOUDANT_CONNECTOR_VERSION = "1.6.2"
  
  val SCHEMA_FOR_ALL_DOCS_NUM = -1

  private val CLOUDANT_HOST_CONFIG = "cloudant.host"
  private val CLOUDANT_USERNAME_CONFIG = "cloudant.username"
  private val CLOUDANT_PASSWORD_CONFIG = "cloudant.password"
  private val CLOUDANT_PROTOCOL_CONFIG = "cloudant.protocol"

  private val PARTITION_CONFIG = "jsonstore.rdd.partitions"
  private val MAX_IN_PARTITION_CONFIG = "jsonstore.rdd.maxInPartition"
  private val MIN_IN_PARTITION_CONFIG = "jsonstore.rdd.minInPartition"
  private val REQUEST_TIMEOUT_CONFIG = "jsonstore.rdd.requestTimeout"
  private val BULK_SIZE_CONFIG = "bulkSize"
  private val SCHEMA_SAMPLE_SIZE_CONFIG = "schemaSampleSize"

  private val configFactory = ConfigFactory.load()

  private val ROOT_CONFIG_NAME = "spark-sql"
  private val rootConfig = configFactory.getConfig(ROOT_CONFIG_NAME) 
     
  private lazy val actorSystem: ActorSystem  = {
      val classLoader = this.getClass.getClassLoader
      val myconfig = ConfigFactory.load(classLoader)// force config from my classloader
      val uuid = java.util.UUID.randomUUID.toString
      ActorSystem("CloudantSpark-"+uuid, myconfig,classLoader)
  }
  
  def getActorSystem(): ActorSystem = {
    actorSystem
  }
  
  private var  alreadyShutdown = false
  
  def shutdown() =  {
    
   if (!alreadyShutdown )
    {
      this.synchronized {
        alreadyShutdown = true
        actorSystem.shutdown
        actorSystem.awaitTermination(Duration(10000, "millis"))
      }
    }
  }

  
  /**
   * The sequence of getting configuration
   * 1. "spark."+key in the SparkConf (as they are treated as the one passed in through spark-submit)
   * 2. key in the parameters, which is set in DF option
   * 3. key in the SparkConf, which is set in SparkConf
   * 4. default in the Config, which is set in the application.conf
   */

  private def getInt(sparkConf: SparkConf, parameters: Map[String, String],
      key: String) : Int = {
    val valueS = parameters.getOrElse(key, null)
    if (sparkConf != null) {
      val default = if (valueS == null)
          sparkConf.getInt(key, rootConfig.getInt(key))
        else
          valueS.toInt
      sparkConf.getInt(s"spark.$key", default)
    }else
      if (valueS == null) rootConfig.getInt(key) else valueS.toInt
  }

  private def getLong(sparkConf: SparkConf, parameters: Map[String, String],
      key: String) : Long = {
    val valueS = parameters.getOrElse(key, null)
    if (sparkConf != null) {
      val default = if (valueS == null)
          sparkConf.getLong(key, rootConfig.getLong(key))
        else
          valueS.toLong
      sparkConf.getLong(s"spark.$key", default)
    }else
      if (valueS == null) rootConfig.getLong(key) else valueS.toLong
  }

  private def getString(sparkConf: SparkConf, parameters: Map[String, String],
      key: String) : String = {
    val defaultInConfig = if (rootConfig.hasPath(key)) rootConfig.getString(key) else null
    val valueS = parameters.getOrElse(key,null)
    if (sparkConf != null) {
      val default = if (valueS == null)
          sparkConf.get(key, defaultInConfig)
        else
          valueS
      sparkConf.get(s"spark.$key",default)
    }else
      if (valueS == null) defaultInConfig else valueS
  }


  /**
   * The configuration sequence: DataFrame option override SparkConf override defaults
   */
  
  def getConfig(context: SQLContext,  parameters: Map[String, String]): CloudantConfig = {
    
      val sparkConf = context.sparkContext.getConf
      
      implicit val total =  getInt(sparkConf, parameters, PARTITION_CONFIG)
      implicit val max = getInt(sparkConf, parameters, MAX_IN_PARTITION_CONFIG)
      implicit val min = getInt(sparkConf, parameters, MIN_IN_PARTITION_CONFIG)
      implicit val requestTimeout = getLong(sparkConf, parameters, REQUEST_TIMEOUT_CONFIG)
      implicit val bulkSize = getInt(sparkConf, parameters, BULK_SIZE_CONFIG)
      implicit val schemaSampleSize = getInt(sparkConf, parameters, SCHEMA_SAMPLE_SIZE_CONFIG)

      val dbName = parameters.getOrElse("database", parameters.getOrElse("path",null))
      val indexName = parameters.getOrElse("index",null)
      val viewName = parameters.getOrElse("view", null)
      
      println(s"Using connectorVersion=$CLOUDANT_CONNECTOR_VERSION, dbName=$dbName, " +
          s"indexName=$indexName, viewName=$viewName, " +
          s"$PARTITION_CONFIG=$total, + $MAX_IN_PARTITION_CONFIG=$max, " +
          s"$MIN_IN_PARTITION_CONFIG=$min, $REQUEST_TIMEOUT_CONFIG=$requestTimeout, " +
          s"$BULK_SIZE_CONFIG=$bulkSize," + s"$SCHEMA_SAMPLE_SIZE_CONFIG=$schemaSampleSize")

      val protocol = getString(sparkConf, parameters,CLOUDANT_PROTOCOL_CONFIG)
      val host = getString( sparkConf, parameters, CLOUDANT_HOST_CONFIG) 
      val user = getString(sparkConf, parameters,CLOUDANT_USERNAME_CONFIG) 
      val passwd = getString(sparkConf, parameters, CLOUDANT_PASSWORD_CONFIG)
      
      if (host != null) {
        val config= new CloudantConfig(protocol, host, dbName, indexName,
            viewName) (user, passwd, total, max, min, requestTimeout, bulkSize, schemaSampleSize)
         context.sparkContext.addSparkListener(new SparkListener(){
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
              config.shutdown()
              println("Finish shutdown at application end")
          }
        })
        config
     } else {
        throw new RuntimeException("Spark configuration is invalid! Please make sure to supply required values for cloudant.host.")
      }
  }

  def getConfig(parameters: Map[String, String]): CloudantConfig = {
    val sparkConf = null

    implicit val total =  getInt(sparkConf, parameters, PARTITION_CONFIG)
    implicit val max = getInt(sparkConf, parameters, MAX_IN_PARTITION_CONFIG)
    implicit val min = getInt(sparkConf, parameters, MIN_IN_PARTITION_CONFIG)
    implicit val requestTimeout = getLong(sparkConf, parameters, REQUEST_TIMEOUT_CONFIG)
    implicit val bulkSize = getInt(sparkConf, parameters, BULK_SIZE_CONFIG)
    implicit val schemaSampleSize = getInt(sparkConf, parameters, SCHEMA_SAMPLE_SIZE_CONFIG)

    val dbName = parameters.getOrElse("database", null)

    println(s"Use connectorVersion=$CLOUDANT_CONNECTOR_VERSION, dbName=$dbName, " +
        s"$REQUEST_TIMEOUT_CONFIG=$requestTimeout")

    val protocol = getString(sparkConf, parameters,CLOUDANT_PROTOCOL_CONFIG)
    val host = getString( sparkConf, parameters, CLOUDANT_HOST_CONFIG)
    val user = getString(sparkConf, parameters,CLOUDANT_USERNAME_CONFIG)
    val passwd = getString(sparkConf, parameters, CLOUDANT_PASSWORD_CONFIG)

    if (host != null) {
      new CloudantConfig(protocol, host, dbName)(user, passwd,
          total, max, min, requestTimeout, bulkSize, schemaSampleSize)
    } else {
      throw new RuntimeException("Cloudant parameters are invalid! Please make sure to supply required values for cloudant.host.")
    }
  }
}

