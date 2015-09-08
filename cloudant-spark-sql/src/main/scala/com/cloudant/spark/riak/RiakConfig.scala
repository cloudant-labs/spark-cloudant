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
package com.cloudant.spark.riak

import org.apache.spark.sql.SQLContext
import play.api.libs.json.JsValue
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsError
import play.api.libs.json.Json
import scala.util.control.Breaks._
import play.api.libs.json.JsUndefined
import play.api.libs.json.JsArray
import java.net.URLEncoder
import com.cloudant.spark.common._
import play.api.libs.json.JsNumber
import play.api.libs.json.JsNumber


/**
 * @author yanglei
 */


@serializable  case class RiakConfig(val host: String, val port: String, val dbName: String)(implicit val username: String=null, val password: String=null,val partitions:Int, val maxInPartition: Int, val minInPartition:Int,val requestTimeout:Long,val concurrentSave:Int, val bulkSize: Int) extends JsonStoreConfig{
  
    private lazy val dbUrl = {"http://"+ host+":"+port+"/search/query/"+dbName+"?wt=json&q="}
    
    def getOneUrl(): String = {dbUrl+ default_filter+ "&start=0&rows=1"}
    
    def getRangeUrl( field:String,  start: Any,  startInclusive: Boolean=false, end:Any, endInclusive: Boolean=false, includeDoc: Boolean = true): (String, Boolean) = {
      val condition = calculateCondition(field, start, startInclusive, end, endInclusive)
      (dbUrl+condition, true)
    }
    
    def getSubSetUrl (url: String, skip: Int, limit: Int)(implicit convertSkip:(Int) => String) : String ={
      url+"&start="+skip+"&rows="+limit
    }
    
    def getTotalRows(result: JsValue): Int = {
        (result \ "response" \ "numFound").as[JsNumber].value.intValue()
    }
    
    def getRows(result: JsValue): Seq[JsValue] = {
        (result \ "response" \ "docs").as[JsArray].value
    }
}
