package io.github.tiankx1003

import com.alibaba.fastjson.{JSON, JSONObject}
import io.github.tiankx1003.utils.ArgsParser.{Config, argsParser}
import io.github.tiankx1003.utils.{CompressionUtils, ESContainer}
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.MapType
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty

import java.lang
import java.nio.file.{Files, Paths}
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

/**
 * @author tiankx
 * @since 2022-10-02 12:23
 * @version 1.0
 */
object LuceneGen {

  @transient lazy private val log = LogFactory.getLog(getClass)

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Config()) match {
      case Some(config) =>
        implicit val formats: DefaultFormats.type = DefaultFormats
        log.info("Final application config : \n" + writePretty(config))
        run(config)
      case _ => sys.exit(1)
    }
  }

  def run(config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .appName("hive2es test tiankx")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val dt = config.indexName.substring(config.indexName.lastIndexOf("_") + 1)
    val whereClause = Some(config.where).getOrElse("1 = 1")
    val input = spark.read.table(config.hiveTable).where(whereClause)

    val inputIndex = spark
      .read
      .table("raw.index_list")
      .select("name")
      .where(s"theme = 'custom' and dt = '$dt'")

    val forbid_array = inputIndex.rdd.map(x => x.getString(0)).collect()

    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val tmpWorkDir = config.hdfsWorkDir + "/" + config.indexName
    val delResult = fs.delete(new Path(tmpWorkDir), true)
    log.info(s"clean hdfs tmp work dir: $tmpWorkDir, result: $delResult.")

    //val serverNotifier = new ServerNotifier(config)
    // Clean Zookeeper
    //log.info("clean zk path for clear")
    //serverNotifier.deleteNode()


    def needIndex(fieldName: String): Boolean = {
      if (fieldName.endsWith("_il") || fieldName.endsWith("_ex")) false
      else true
    }

    val dataTypeMappingSql =
      s"""
         |select
         |  index_name,data_type
         |from
         |  tmp.need_index_list
         |where dt = $dt
       """.stripMargin

    val dataTypeMapping = spark
      .sql(dataTypeMappingSql)
      .collect()
      .map(r => {
        (
          r.getAs[String]("index_name").trim,
          r.getAs[String]("data_type").trim
        )
      })
      .toMap

    println("******************************************:" + dt)
    println(dataTypeMapping)
    println("******************************************")

    def dataTypeConvert(fieldName: String, dataType: String): String = {
      dataTypeMapping.getOrElse(fieldName, dataType.toLowerCase match {
        case "bigint" => "long"
        case "int" => "integer"
        case x if x.startsWith("array") => "string"
        case x if x.startsWith("decimal") => "double"
        case _ => dataType
      })
    }

    def mapFieldName(fieldName: String, key: String): String = {
      val esKey = if (fieldName.endsWith("_il")) {
        fieldName + "-" + key
      } else {
        "" + key
      }
      esKey.toLowerCase().replaceAll("&", "-").replaceAll("\\$", "-")
    }

    val fields = input.rdd.flatMap(r => {
      r.schema.fields.flatMap(f => {
        f.dataType match {
          case x: MapType =>
            val mapValue = r.getAs[Map[String, Object]](f.name)
            if (mapValue != null) {
              mapValue.keys.map(key => {
                val esKey = mapFieldName(f.name, key)
                (esKey, (x.valueType.simpleString, false, f.name))
              })
            } else Seq()
          case _ => Seq((f.name, (f.dataType.simpleString, true, f.name)))
        }
      })
    }).filter(_._1 != null).distinct().collect()

    import scala.collection.JavaConverters._

    val indexFieldCount = new AtomicLong()

    val mapping = fields.toMap.map { case (esKey, x) =>
      //是否建立索引
      val indexField = x._2 || needIndex(x._3)
      val dataType = dataTypeConvert(esKey, x._1)
      val index = if (!indexField) {
        "no"
      } else if (dataType.equalsIgnoreCase("string")) {
        indexFieldCount.incrementAndGet()
        "not_analyzed"
      } else {
        indexFieldCount.incrementAndGet()
        null
      }
      var result = Map("type" -> dataType)
      if (index != null) {
        result += ("index" -> index)
      }
      if (dataType.equalsIgnoreCase("date")) {
        result += ("format" -> "yyyyMMdd")
      }
      (esKey, result.asJava)
    }.asJava
    log.info(s"Mapping index field count / total field : [${indexFieldCount.get()} / ${mapping.size()}]")

    val mappingObj = JSON.toJSON(mapping).asInstanceOf[JSONObject]
    Files.write(Paths.get("mapping.json"), mappingObj.toString().getBytes)

    CompressionUtils.upload2HDFS(Paths.get("mapping.json").toString
      , Paths.get(config.hdfsWorkDir).resolve(config.indexName).resolve("mapping.json").toString)

    val mappingString = new String(Files.readAllBytes(Paths.get("mapping.json")))
    Files.delete(Paths.get("mapping.json"))

    //serverNotifier.startIndex()

    def notNullValue(value: Object): Boolean = {
      if (value == null) {
        false
      } else value match {
        case strValue: String =>
          StringUtils.isNotEmpty(strValue) && !"null".equalsIgnoreCase(strValue)
        case _ =>
          true
      }
    }

    def getFinalValue(fieldName: String, dataType: String, value: Object): Object = {
      if (value != null && value.toString.startsWith("[") && value.toString.endsWith("]")) { // Array
        val arrayValue: Array[String] = value.toString.replace("[", "").replace("]", "").replace(" ", "").split(",")
        arrayValue
      } else if (value != null && value.toString.startsWith("WrappedArray(") && value.toString.endsWith(")")) {
        val arrayStrValue: Array[String] = value.toString.replace("WrappedArray(", "").replace(")", "").replace(" ", "").split(",")
        arrayStrValue
      } else if (value != null && dataType.equalsIgnoreCase("date")) { // date
        DateFormatUtils.format(value.asInstanceOf[Date], "yyyyMMdd")
      } else if (null != value && dataTypeMapping.contains(fieldName)) { // manually assign
        val finalDataType = mappingObj.getJSONObject(fieldName).getString("type")
        finalDataType match {
          case "long" => java.lang.Long.valueOf(value.toString)
          case "integer" => value match {
            case double: lang.Double =>
              double.intValue().asInstanceOf[Integer]
            case _ => value match {
              case str: String =>
                new Integer(str)
              case _ =>
                value.asInstanceOf[Integer]
            }
          }
          case "double" => java.lang.Double.valueOf(value.toString)
          case "string" => value.toString
          case _ => value
        }
      } else if (null != value && dataType.startsWith("decimal")) { // decimal
        value.asInstanceOf[java.math.BigDecimal].doubleValue().asInstanceOf[java.lang.Double]
      } else { // use default
        value
      }
    }

    // RDD[Array[(esKey,(dataType,needIndex,value))]]
    var esDocKey: String = null
    val data = input.rdd.map(r => {
      val doc = new JSONObject()
      r.schema.fields.flatMap(f => {
        f.dataType match {
          case x: MapType =>
            val mapValue = r.getAs[Map[String, Object]](f.name)
            if (mapValue == null) {
              Seq()
            } else {
              mapValue.keys.map(key => {
                val esKey = mapFieldName(f.name, key)
                //过滤下线指标数据
                if (!forbid_array.contains(esKey)) {
                  esDocKey = esKey
                } else {
                  esDocKey = null
                }
                (esDocKey, getFinalValue(esKey, x.valueType.simpleString, mapValue(key)))
              })
            }
          case _ =>
            if (forbid_array.contains(f.name)) {
              Seq()
            } else {
              Seq((f.name, getFinalValue(f.name, f.dataType.simpleString, r.getAs(f.name))))
            }
        }
      }).foreach(f => {
        if (f._1 != null && notNullValue(f._2)) doc.put(f._1, f._2)
      })
      (doc.getString(config.id), doc)
    })

    data.foreachPartition(docsP => {
      val partitionId = TaskContext.get.partitionId

      val esContainer = new ESContainer(config, partitionId)
      try {
        esContainer.start()
        esContainer.createIndex()
        esContainer.putMapping(JSON.parseObject(mappingString))
        var count = 0
        docsP.foreach(doc => {
          esContainer.put(doc._2, doc._1)
          count += 1
        })
        log.info(s"partition $partitionId record size : $count")
      } finally {
        esContainer.cleanUp()
      }
    })

    //serverNotifier.completeIndex()
  }
}
