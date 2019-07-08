package io.github.interestinglab.waterdrop.output.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils
import org.apache.commons.configuration.ConfigurationRuntimeException
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

import scala.collection.JavaConversions._

class Hbase extends BaseOutput {

  var hbaseCfg: Map[String, String] = Map()
  val hbasePrefix = "hbase."

  var config: Config = ConfigFactory.empty()

  var catLog : String = ""

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    if(config.hasPath("haveKerberos") && config.getBoolean("haveKerberos")){
      config.hasPath("catalog_file")&& config.hasPath("kerberosKeytabFilePath") && config.hasPath("table_name") &&
        config.hasPath("hbaseConfigFile") && config.hasPath("kerberosPrincipal")
      match {
        case true => (true, "")
        case false => (false, "please specify [catalog_file] [hbaseConfigFile] [kerberosPrincipal] [kerberosKeytabFilePath] " )
      }
    }else{
      config.hasPath("catalog_file") && config.hasPath("hbaseConfigFile") && config.hasPath("table_name")
      match {
        case true => (true, "")
        case false => (false, "please specify [catalog_file] [hbaseConfigFile] [table_name] " +
          "")
      }
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    if (TypesafeConfigUtils.hasSubConfig(config, hbasePrefix)) {
      val hbaseConfig = TypesafeConfigUtils.extractSubConfig(config, hbasePrefix, true)
      hbaseConfig
        .entrySet()
        .foreach(entry => {
          val key = entry.getKey
          val value = String.valueOf(entry.getValue.unwrapped())
          hbaseCfg += (key -> value)
        })
    }

    if(config.hasPath("haveKerberos") && config.getBoolean("haveKerberos")){
      hbaseCfg += ("hadoop.security.authentication" -> "kerberos")
      hbaseCfg += ("hbase.security.authentication" -> "kerberos")
    }


    println("[INFO] Input Hbase Params:")
    for (entry <- hbaseCfg) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }

  }

  override def process(df: Dataset[Row]): Unit = {
    if(config.hasPath("haveKerberos") && config.getBoolean("haveKerberos")){
      try {
        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.addResource(config.getString("hbaseConfigFile"))

        for (entry <- hbaseCfg) {
          val (key, value) = entry
          hbaseConfig.set(key,value)
          println("[INFO]  hbaseConfig set :\t" + key + " = " + value)
        }
        UserGroupInformation.setConfiguration(hbaseConfig)
        UserGroupInformation.loginUserFromKeytab(config.getString("kerberosPrincipal"),config.getString("kerberosKeytabFilePath"))
        println("[INFO]  kerberos 验证成功")
      } catch {
        case ex: Exception =>{
          ex.printStackTrace()
          throw new ConfigurationRuntimeException("[INFO]  kerberos 验证失败")
        }
      }

    }
    try {
      var option: Map[String, String] = Map()
      option += (HBaseTableCatalog.tableCatalog -> catLog)
      option += ("hbaseConfigFile" -> config.getString("hbaseConfigFile"))
      println("catLog内容%s".format(catLog))
      //df.write.options(Map("schema1"->schema, HBaseTableCatalog.tableCatalog->catalog)).format("org.apache.spark.sql.execution.datasources.hbase").save()
      df.write
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .options(option)
        .save()
    }catch {
      case ex: Exception =>{
        ex.printStackTrace()
        throw new ConfigurationRuntimeException("[INFO]  load data失败")
      }
    }
  }
}
