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

 object JsonStoreConfigManager
{
  val SCHEMA_FOR_ALL_DOCS_NUM = -1
  
  val CLOUDANT_HOST_CONFIG = "cloudant.host"
  val CLOUDANT_USERNAME_CONFIG = "cloudant.username"
  val CLOUDANT_PASSWORD_CONFIG = "cloudant.password"
  val CLOUDANT_PROTOCOL_CONFIG = "cloudant.protocol"


  val PARTITION_CONFIG = "jsonstore.rdd.partitions"
  val MAX_IN_PARTITION_CONFIG = "jsonstore.rdd.maxInPartition"
  val MIN_IN_PARTITION_CONFIG = "jsonstore.rdd.minInPartition"
  val REQUEST_TIMEOUT_CONFIG = "jsonstore.rdd.requestTimeout"
  val BULK_SIZE_CONFIG = "jsonstore.rdd.bulkSize"
  val SCHEMA_SAMPLE_SIZE_CONFIG = "jsonstore.rdd.schemaSampleSize"
  val PARAM_SCHEMA_SAMPLE_SIZE_CONFIG = "schemaSampleSize"
  
  val configFactory = ConfigFactory.load()

  val ROOT_CONFIG_NAME = "spark-sql"
  val rootConfig = configFactory.getConfig(ROOT_CONFIG_NAME) 
  val defaultPartitions = rootConfig.getInt(PARTITION_CONFIG)
  val defaultMaxInPartition = rootConfig.getInt(MAX_IN_PARTITION_CONFIG)
  val defaultMinInPartition = rootConfig.getInt(MIN_IN_PARTITION_CONFIG)
  val defaultRequestTimeout = rootConfig.getLong(REQUEST_TIMEOUT_CONFIG)
  val defaultBulkSize = rootConfig.getInt(BULK_SIZE_CONFIG)
  val defaultSchemaSampleSize = rootConfig.getInt(SCHEMA_SAMPLE_SIZE_CONFIG)

     
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

  def calculateSchemaSampleSize(schemaSampleSize: String): Int = {
    if (schemaSampleSize != null) {
      schemaSampleSize.toInt;
    } else {
      defaultSchemaSampleSize
    }
  }
  
  def getConfig(context: SQLContext, dbName: String, indexName:String = null, viewName:String = null, schemaSampleSize: String): CloudantConfig = {
      val sparkConf = context.sparkContext.getConf
      implicit val total = sparkConf.getInt(PARTITION_CONFIG,defaultPartitions)
      implicit val max = sparkConf.getInt(MAX_IN_PARTITION_CONFIG,defaultMaxInPartition)
      implicit val min =sparkConf.getInt(MIN_IN_PARTITION_CONFIG,defaultMinInPartition)
      implicit val requestTimeout =sparkConf.getLong(REQUEST_TIMEOUT_CONFIG,defaultRequestTimeout)
      implicit val bulkSize =sparkConf.getInt(BULK_SIZE_CONFIG,defaultBulkSize)
      
      var varSchemaSampleSize = schemaSampleSize;
      if (varSchemaSampleSize == null && sparkConf.contains(SCHEMA_SAMPLE_SIZE_CONFIG)){
        varSchemaSampleSize = sparkConf.get(SCHEMA_SAMPLE_SIZE_CONFIG)
      }
      
      val intSchemaSampleSize = calculateSchemaSampleSize(varSchemaSampleSize);

      println(s"Use dbName=$dbName, indexName=$indexName, $PARTITION_CONFIG=$total, $MAX_IN_PARTITION_CONFIG=$max, $MIN_IN_PARTITION_CONFIG=$min, $REQUEST_TIMEOUT_CONFIG=$requestTimeout,$BULK_SIZE_CONFIG=$bulkSize,$SCHEMA_SAMPLE_SIZE_CONFIG=$intSchemaSampleSize")

      val protocol = if (sparkConf.contains(CLOUDANT_PROTOCOL_CONFIG)) sparkConf.get(CLOUDANT_PROTOCOL_CONFIG) else "https"
      val host = if (sparkConf.contains(CLOUDANT_HOST_CONFIG)) sparkConf.get(CLOUDANT_HOST_CONFIG) else null
      val user = if (sparkConf.contains(CLOUDANT_USERNAME_CONFIG)) sparkConf.get(CLOUDANT_USERNAME_CONFIG) else null
      val passwd = if (sparkConf.contains(CLOUDANT_PASSWORD_CONFIG)) sparkConf.get(CLOUDANT_PASSWORD_CONFIG) else null
      
      if (host != null) {
        val config= new CloudantConfig(protocol, host,  dbName, indexName, viewName, intSchemaSampleSize)(user, passwd, total, max, min,requestTimeout, bulkSize)
        context.sparkContext.addSparkListener(new SparkListener(){
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
              config.shutdown()
              println("Finish shutdown at application end")
          }
        })
        config
      }else{
        throw new RuntimeException("Spark configuration is invalid! Please make sure to supply required values for cloudant.host.")
      }
  }
  
  /**
   * The configuration sequence: DataFrame option override SparkConf override defaults
   */
  
  def getConfig(context: SQLContext,  parameters: Map[String, String]): CloudantConfig = {
    
      val sparkConf = context.sparkContext.getConf
      
      val totalS = parameters.getOrElse(PARTITION_CONFIG,null)
      implicit val total = if (totalS ==null) sparkConf.getInt(PARTITION_CONFIG,defaultPartitions) else totalS.toInt
      val maxS = parameters.getOrElse(MAX_IN_PARTITION_CONFIG,null)
      implicit val max = if (maxS ==null) sparkConf.getInt(MAX_IN_PARTITION_CONFIG,defaultMaxInPartition) else maxS.toInt
      val minS = parameters.getOrElse(MIN_IN_PARTITION_CONFIG,null)
      implicit val min = if (minS ==null) sparkConf.getInt(MIN_IN_PARTITION_CONFIG,defaultMinInPartition) else minS.toInt
      
      implicit val requestTimeout =sparkConf.getLong(REQUEST_TIMEOUT_CONFIG,defaultRequestTimeout)
      implicit val bulkSize =sparkConf.getInt(BULK_SIZE_CONFIG,defaultBulkSize)

      val dbName = parameters.getOrElse("database", parameters.getOrElse("path",null))
      val indexName = parameters.getOrElse("index",null)
      val viewName = parameters.getOrElse("view", null)

      var schemaSampleSize = parameters.getOrElse(PARAM_SCHEMA_SAMPLE_SIZE_CONFIG, null)
      if (schemaSampleSize == null && sparkConf.contains(SCHEMA_SAMPLE_SIZE_CONFIG) ) {
        schemaSampleSize = sparkConf.get(SCHEMA_SAMPLE_SIZE_CONFIG)
      }
      
      val intSchemaSampleSize = calculateSchemaSampleSize(schemaSampleSize);
      
      println(s"Use dbName=$dbName, indexName=$indexName, viewName=$viewName, $PARTITION_CONFIG=$total, $MAX_IN_PARTITION_CONFIG=$max, $MIN_IN_PARTITION_CONFIG=$min, $REQUEST_TIMEOUT_CONFIG=$requestTimeout,$BULK_SIZE_CONFIG=$bulkSize,$SCHEMA_SAMPLE_SIZE_CONFIG=$intSchemaSampleSize")

      val protocolParam = parameters.getOrElse(CLOUDANT_PROTOCOL_CONFIG, null)
      val hostParam = parameters.getOrElse(CLOUDANT_HOST_CONFIG, null)
      val userParam = parameters.getOrElse(CLOUDANT_USERNAME_CONFIG, null)
      val passwdParam = parameters.getOrElse(CLOUDANT_PASSWORD_CONFIG, null)
      val protocol = if (protocolParam == null) sparkConf.get(CLOUDANT_PROTOCOL_CONFIG, "https") else protocolParam
      val host = if (hostParam == null) sparkConf.get(CLOUDANT_HOST_CONFIG, null) else hostParam
      val user = if (userParam == null) sparkConf.get(CLOUDANT_USERNAME_CONFIG, null) else userParam
      val passwd = if (passwdParam == null) sparkConf.get(CLOUDANT_PASSWORD_CONFIG, null) else passwdParam
      if (host != null) {
        val config= new CloudantConfig(protocol, host, dbName, indexName, viewName, intSchemaSampleSize)(user, passwd, total, max, min, requestTimeout, bulkSize)
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

}