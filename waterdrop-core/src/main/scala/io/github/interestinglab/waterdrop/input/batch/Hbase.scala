package io.github.interestinglab.waterdrop.input.batch

import java.io.{FileInputStream, InputStreamReader}
import java.security.ProtectionDomain

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.Test
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import io.github.interestinglab.waterdrop.config.{ConfigRuntimeException, TypesafeConfigUtils}
import org.apache.commons.configuration.ConfigurationRuntimeException
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.xml.XML
import scala.runtime.IntRef


class Hbase extends BaseStaticInput{
  val hbasePrefix = "hbase."
  var config: Config = ConfigFactory.empty()

  var hbaseCfg: Map[String, String] = Map()
  val hbase_zk_clientPort="hbase.zookeeper.property.clientPort"
  val hbase_zk_quorum="hbase.zookeeper.quorum"
  val hbase_zk_parent="zookeeper.znode.parent"
  var catLog : String = ""
  /**
   * Get Dataset from this Static Input.
   * */
  override def getDataset(spark: SparkSession)
    : Dataset[Row] = {
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
    //val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConfig, null)
      println("catLog内容%s".format(catLog))
    spark.read
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .options(option)
      .load()
    }catch {
      case ex: Exception =>{
        ex.printStackTrace()
        throw new ConfigurationRuntimeException("[INFO]  load data失败")
      }
    }
  }


    override def setConfig(config: Config): Unit = {
      this.config=config
    }


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

    //hbaseXmlParser(config.getString("hbaseConfigFile"))
    hbaseCatlogJsonParser(config.getString("catalog_file"))
  }

  def hbaseXmlParser(filePath: String): Unit = {
    val xml = XML.load(new InputStreamReader(new FileInputStream(filePath)))
    val propertyNodes = xml \\ "property"
    for(item <- propertyNodes){
      var nameText= (item \ "name").text
      var valueText = (item \ "value").text
      if(nameText == hbase_zk_clientPort || nameText== hbase_zk_quorum || nameText== hbase_zk_parent){
        hbaseCfg += (nameText -> valueText)
      }
    }
    println("[INFO] loading hbaseZkConf: ")
    hbaseCfg.foreach(entry => {
      val (key, value) = entry
      println("\t" + key + " => " + value)
    })
  }

  def hbaseCatlogJsonParser(jsonFilePath: String): Unit = {
    var cat : String =""
    val lines = Source.fromFile(jsonFilePath).getLines().toIterator
    while (lines.hasNext) {
      cat+= "%s\n".format(lines.next())
    }
    catLog=cat
  }

}
