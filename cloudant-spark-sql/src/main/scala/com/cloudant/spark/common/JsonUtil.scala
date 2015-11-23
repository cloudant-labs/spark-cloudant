package com.cloudant.spark

import play.api.libs.json.{JsUndefined, JsValue}
import scala.util.control.Breaks._

object JsonUtil{
  def getField(row: JsValue, field: String) : Option[JsValue] = {
    var path = field.split('.')
    var currentValue = row
    var finalValue: Option[JsValue] = None
    breakable {
      for (i <- path.indices){
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