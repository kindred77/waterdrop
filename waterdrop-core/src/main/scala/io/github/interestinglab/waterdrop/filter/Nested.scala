package io.github.interestinglab.waterdrop.filter

import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.core.RowConstant
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Nested extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  // TODO: check fields.length == field_types.length if field_types exists
  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("source_field") && conf.hasPath("target_field") && conf.hasPath("fields") && conf.getStringList("fields").size() >0 match {
      case true => (true, "")
      case false => (false, "please specify source_field and target_field and fields")
    }
  }

  override def prepare(spark: SparkSession): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> RowConstant.ROOT
      )
    )

    conf = conf.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val srcField = conf.getString("source_field")
    val keys = conf.getStringList("fields")

    // https://stackoverflow.com/a/33345698/1145750
    conf.getString("target_field") match {
      case RowConstant.ROOT => {
        val func = udf((s: String) => {
          tonested(s, keys)
        })
        var filterDf = df.withColumn(RowConstant.TMP, func(col(srcField)))
        for (i <- 0 until keys.size()) {
          filterDf = filterDf.withColumn(keys.get(i), col(RowConstant.TMP)(i))
        }
        filterDf.drop(RowConstant.TMP)
      }
      case targetField: String => {
        println("chai test targetfile")
        val nestedFunc = udf((s: String) => {
          val values = tonested(s,keys)
          values
        })
          df.withColumn(targetField, nestedFunc(col(srcField)))
      }
    }
  }

  /**
   * Split string by delimiter, if size of splited parts is less than fillLength,
   * empty string is filled; if greater than fillLength, parts will be truncated.
   * */


  /*private def tonested(str: String, fileds: java.util.List[String]): Map[String, Seq[String]] = {
    //val nestedMap = new mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val nestedMap = scala.collection.mutable.Map[String, mutable.ArrayBuffer[String]]()
    val seqMap = scala.collection.mutable.Map[String, Seq[String]]()
    if(str==null || str.trim.equals("") || str.trim.endsWith("[]")){
      for(j <- 0 until fileds.size()){
        val listValue = new ArrayBuffer[String]()
        nestedMap += (fileds.get(j) -> listValue)
      }
    }else {
    val rddJsonArray = JSON.parseArray(str)
    for (i <- 0 until rddJsonArray.size()) {
      val item = rddJsonArray.getJSONObject(i)
      for (j <- 0 until fileds.size()) {
        val value = item.getOrDefault(fileds.get(j), "");
        if (!nestedMap.contains(fileds.get(j))) {
          val listValue = new ArrayBuffer[String]()
          listValue += value.toString
          nestedMap += (fileds.get(j) -> listValue)
        } else {
          nestedMap(fileds.get(j)) += value.toString
        }
      }
    }
    }
    for (entry <- nestedMap) {
      val (key, value) = entry
      seqMap += (key -> value.toArray[String].toSeq)
      println("[INFO]  hbaseConfig set :\t" + key + " = " + value.mkString(","))
    }
    seqMap.toMap
  }*/


  private def tonested(str: String, fileds: java.util.List[String]): scala.collection.mutable.Map[String, mutable.Buffer[String]] = {
    val nestedMap = scala.collection.mutable.Map[String, mutable.Buffer[String]]()
    if(str==null || str.trim.equals("") || str.trim.endsWith("[]")){
      for(j <- 0 until fileds.size()){
        val listValue = new ArrayBuffer[String]()
        nestedMap += (fileds.get(j) -> listValue)
      }
    }else {
      val rddJsonArray = JSON.parseArray(str)
      if(fileds.size()==1){
        val item = rddJsonArray.getJSONObject(0)
        val value = item.getString(fileds.get(0))
        val valueArray = value.split(",")
        nestedMap += (fileds.get(0) -> valueArray.toBuffer)
      }else {
        for (i <- 0 until rddJsonArray.size()) {
      val item = rddJsonArray.getJSONObject(i)
      for (j <- 0 until fileds.size()) {
        val value = item.getOrDefault(fileds.get(j), "");
        if (!nestedMap.contains(fileds.get(j))) {
          val listValue = new ArrayBuffer[String]()
          listValue += value.toString
          nestedMap += (fileds.get(j) -> listValue)
        } else {
          nestedMap(fileds.get(j)) += value.toString
        }
      }
    }
      }
    }
    /*for (entry <- nestedMap) {
      val (key, value) = entry
      seqMap += (key -> value.toArray[String].toSeq)
      println("[INFO]  hbaseConfig set :\t" + key + " = " + value.mkString(","))
    }*/
    nestedMap
  }
}
