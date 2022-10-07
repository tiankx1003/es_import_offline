package io.github.tiankx1003

import com.alibaba.fastjson.{JSON, JSONObject}
import io.github.tiankx1003.utils.ArgsParser.{Config, argsParser}
import io.github.tiankx1003.utils.{CompressionUtils, ESContainer, ServerNotifier}
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{MapType, StructField}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty

import java.{lang, util}
import java.nio.file.{Files, Paths}
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

/**
 * generate lucene file offline with spark
 *
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
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
      .appName("import es offline test tiankx")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val dt = config.indexName.substring(config.indexName.lastIndexOf("_") + 1)
    val whereClause = Some(config.where).getOrElse("1 = 1")
    val srcTabQryResult: Dataset[Row] = spark.read.table(config.hiveTable).where(whereClause)

    val inputIndex = spark
      .read
      .table("tmp.abd_index_list")
      .select("name")
      .where(s"theme = 'test' and dt = '$dt'")

    val forbid_array = inputIndex.rdd.map(x => x.getString(0)).collect()

    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val tmpWorkDir = config.hdfsWorkDir + "/" + config.indexName
    val delResult = fs.delete(new Path(tmpWorkDir), true)
    log.info(s"clean hdfs tmp work dir: $tmpWorkDir, result: $delResult.")

    val serverNotifier = new ServerNotifier(config)
    // Clean Zookeeper
    log.info("clean zk path for clear")
    serverNotifier.deleteNode()

    /**
     * TODO
     *
     * @param fieldName hive field name
     * @return if need create index
     */
    def needIndex(fieldName: String): Boolean = {
      if (fieldName.endsWith("_map") || fieldName.endsWith("_ex")) false
      else true
    }

    val dataTypeMappingSql =
      s"""
         |select
         |  index_name,data_type
         |from
         |  tmp.column_type_index_mapping
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

    log.info("******************************************:" + dt)
    log.info(dataTypeMapping)
    log.info("******************************************")

    /**
     * hive column type mapping to es index type
     *
     * @param fieldName hive column name
     * @param dataType  hive column type
     * @return es index mapping
     */
    def dataTypeConvert(fieldName: String, dataType: String): String = {
      dataTypeMapping.getOrElse(fieldName, dataType.toLowerCase match {
        case "bigint" => "long"
        case "int" => "integer"
        case x if x.startsWith("array") => "string"
        case x if x.startsWith("decimal") => "double"
        case _ => dataType
      })
    }

    /**
     * concat map type column, generate field name
     *
     * @param fieldName hive field name
     * @param key       hive map key
     * @return
     */
    def mapFieldName(fieldName: String, key: String): String = {
      val esKey = if (fieldName.endsWith("_map")) {
        fieldName + "-" + key
      } else {
        "" + key
      }
      esKey.toLowerCase().replaceAll("&", "-").replaceAll("\\$", "-")
    }

    /**
     * Generate es field info by query result, process hive map type column specially.
     * (es_index_name, (data_type, if_col_eq_index, hive_col_name))
     * */
    val esFieldInfo: Array[(String, (String, Boolean, String))] = srcTabQryResult.rdd.flatMap(resultRdd => {
      resultRdd.schema.fields.flatMap(field => {
        field.dataType match {
          case mapTypeData: MapType =>
            val mapValue: Map[String, Object] = resultRdd.getAs[Map[String, Object]](field.name)
            if (mapValue != null) {
              mapValue.keys.map(key => {
                val esKey = mapFieldName(field.name, key)
                (esKey, (mapTypeData.valueType.simpleString, false, field.name))
              })
            } else Seq()
          case _ => Seq((field.name, (field.dataType.simpleString, true, field.name)))
        }
      })
    }).filter(_._1 != null).distinct().collect()

    import scala.collection.JavaConverters._

    val indexFieldCount = new AtomicLong()

    val esMapping: util.Map[String, util.Map[String, String]] = esFieldInfo.toMap.map { case (esKey, x) =>
      // if build index
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
      if (dataType.equalsIgnoreCase("date")) { // TODO: es date fields
        result += ("format" -> "yyyyMMdd")
      }
      (esKey, result.asJava)
    }.asJava
    log.info(s"Mapping index field count / total field : [${indexFieldCount.get()} / ${esMapping.size()}]")

    val mappingObj = JSON.toJSON(esMapping).asInstanceOf[JSONObject]
    Files.write(Paths.get("mapping.json"), mappingObj.toString().getBytes)

    CompressionUtils.upload2HDFS(
      Paths.get("mapping.json").toString,
      Paths.get(config.hdfsWorkDir).resolve(config.indexName).resolve("mapping.json").toString
    )

    val mappingString = new String(Files.readAllBytes(Paths.get("mapping.json")))
    Files.delete(Paths.get("mapping.json"))

    serverNotifier.startIndex()

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

    /**
     * process data by field type
     *
     * @param fieldName es field name
     * @param dataType  es field type
     * @param value     hive table query result RDD
     * @return type processed data
     */
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

    var esDocKey: String = null
    // RDD[Array[(esKey,(dataType,needIndex,value))]]
    // RDD[(primary_key, (esDocKey, esDocValue))]
    val data: RDD[(String, JSONObject)] = srcTabQryResult.rdd.map(resultRdd => {
      val doc = new JSONObject()
      resultRdd.schema.fields.flatMap((field: StructField) => {
        field.dataType match {
          case mapTypeData: MapType =>
            val mapValue = resultRdd.getAs[Map[String, Object]](field.name)
            if (mapValue == null) {
              Seq()
            } else {
              mapValue.keys.map(key => {
                val esKey = mapFieldName(field.name, key)
                // filter abandoned fields
                if (!forbid_array.contains(esKey)) {
                  esDocKey = esKey
                } else {
                  esDocKey = null
                }
                (esDocKey, getFinalValue(esKey, mapTypeData.valueType.simpleString, mapValue(key)))
              })
            }
          case _ =>
            if (forbid_array.contains(field.name)) {
              Seq()
            } else {
              Seq((field.name, getFinalValue(field.name, field.dataType.simpleString, resultRdd.getAs(field.name))))
            }
        } // (esDocKey, esDocValue)
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

    serverNotifier.completeIndex()
  }
}
