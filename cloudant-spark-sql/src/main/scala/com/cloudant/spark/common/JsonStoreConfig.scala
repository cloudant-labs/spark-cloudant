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
import play.api.libs.json.JsValue
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsError
import play.api.libs.json.Json
import scala.util.control.Breaks._
import play.api.libs.json.JsUndefined
import java.net.URLEncoder
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.Duration

/**
 * 
 * @author yanglei
 * Only allow one field pushdown now as the filter today does not tell how to link the filters out And v.s. Or
 */
trait JsonStoreConfig {
  implicit val username: String
  implicit val password: String
  implicit val partitions: Int
  implicit val maxInPartition: Int
  implicit val minInPartition: Int
  def getOneUrl(): String
  def getPostUrl(): String = {""}
  def getRangeUrl(field: String, start: Any, startInclusive:Boolean=false, end:Any, endInclusive:Boolean=false, includeDoc: Boolean = true): (String, Boolean) 
  def getSubSetUrl (url: String, skip: Int, limit: Int) : String
  def getTotalRows(result: JsValue): JsValue
  def getRows(result: JsValue): Seq[JsValue]
  
  val default_filter: String = "*:*"
  
  def calculateCondition(field: String, min:Any, minInclusive: Boolean=false, max: Any, maxInclusive: Boolean = false) : String = {
      if (field!=null && ( min !=null || max!= null))
      {
        var condition = field+":"
        if (min!=null && max!=null && min.equals(max)){
           condition += min
        }
        else{
          if (minInclusive) condition+="["
          else condition +="{"
          if (min!=null) condition += min
          else condition+="*"
          condition+=" TO "
          if (max !=null) condition += max
          else condition += "*"
          if (maxInclusive) condition+="]"
          else condition +="}"
        }
        URLEncoder.encode(condition,"UTF-8")
      }else default_filter
  }
}


private object JsonUtil
{
     def getField(row: JsValue, field: String) : Option[JsValue] = {
      var path = field.split('.')
      var currentValue = row
      var finalValue: Option[JsValue] = None
      breakable { 
        for (i <- path.indices)
        {
          val f = currentValue \ path(i)
          f match {
            case s : JsUndefined => break
            case _ =>  currentValue = f
          }
          if (i == path.length -1) //The leaf node
            finalValue = Some(currentValue)
      }
    }
    finalValue
    }
}

