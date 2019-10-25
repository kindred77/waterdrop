package io.github.interestinglab.waterdrop.filter
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, _}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.WrappedArray

class ArrayConvert extends BaseFilter{
  var conf: Config = ConfigFactory.empty()


  override def setConfig(config: Config): Unit = {
    this.conf = config
  }


  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    if (!conf.hasPath("source_array_field")) {
      (false, "please specify [source_array_field] as a non-empty string")
    } else if (!conf.hasPath("new_array_type")) {
      (false, "please specify [new_array_type] as a non-empty string")
    } else {
      (true, "")
    }
  }

  override def prepare(spark: SparkSession): Unit = {

    super.prepare(spark)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val srcField = conf.getString("source_array_field")
    val newType = conf.getString("new_array_type")
    val tgfield = conf.getString("target_array_field")

    val arrayStringFunc = udf((s:String) => {
      val values = toStringArray("[".concat(s).concat("]"))
      values
    })

    val arrayLongFunc = udf((s:String) => {
      val values = toLongArray("[".concat(s).concat("]"))
      values
    })

    val arrayDoubleFunc = udf((s:String) => {
      val values = toDoubleArray("[".concat(s).concat("]"))
      values
    })

    val arrayFloatFunc = udf((s:String) => {
      val values = toFloatArray("[".concat(s).concat("]"))
      values
    })


    newType match {
      case "string" => df.withColumn(tgfield, arrayStringFunc(concat_ws(",",col(srcField))))
      case "double" => df.withColumn(tgfield, arrayDoubleFunc(concat_ws(",",col(srcField))))
      case "float" =>  df.withColumn(tgfield, arrayFloatFunc(concat_ws(",",col(srcField))))
      case "long" =>  df.withColumn(tgfield, arrayLongFunc(concat_ws(",",col(srcField))))
      case _: String => df.withColumn(tgfield, arrayStringFunc(concat_ws(",",col(srcField))))
    }
  }

  /*private def toArray(str:String): Seq[Long] = {
    val jsonArray = JSON.parseArray(str)
    val result = new mutable.ArrayBuffer[Long]()
    for (i <- 0 until jsonArray.size) {
      result += jsonArray.getString(i).toLong
    }
    result
  }*/

  private def toLongArray(str:String): mutable.Buffer[Long] = {
    val jsonArray = JSON.parseArray(str)
    val result = new mutable.ArrayBuffer[Long]()
    for (i <- 0 until jsonArray.size) {
      if(!jsonArray.getString(i).trim.equals("")){
        result += jsonArray.getString(i).toLong
      }else{
        result += 0
      }
    }
    result
  }



  private def toStringArray(str:String): mutable.Buffer[String] = {
    val jsonArray = JSON.parseArray(str)
    val result = new mutable.ArrayBuffer[String]()
    for (i <- 0 until jsonArray.size) {
      result += jsonArray.getString(i)
    }
    result
  }


  private def toDoubleArray(str:String): mutable.Buffer[Double] = {
    val jsonArray = JSON.parseArray(str)
    val result = new mutable.ArrayBuffer[Double]()
    for (i <- 0 until jsonArray.size) {
      if(!jsonArray.getString(i).trim.equals("")){
        result += jsonArray.getString(i).toDouble
      }else{
        result += 0.0
      }
    }
    result
  }


  private def toFloatArray(str:String): mutable.Buffer[Float] = {
    val jsonArray = JSON.parseArray(str)
    val result = new mutable.ArrayBuffer[Float]()
    for (i <- 0 until jsonArray.size) {
      if(!jsonArray.getString(i).trim.equals("")){
        result += jsonArray.getString(i).toFloat
      }else{
        result += 0.0F
      }
    }
    result
  }

}
