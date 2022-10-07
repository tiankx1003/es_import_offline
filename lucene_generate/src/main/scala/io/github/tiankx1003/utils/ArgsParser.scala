package io.github.tiankx1003.utils

import scopt.OptionParser

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @since 2022-10-02 12:25
 * @version 1.0
 */
object ArgsParser {

  case class Config(
                     hiveTable: String = null,
                     where: String = null,
                     numShards: Int = 3,
                     repartition: Boolean = false,
                     partitionMultiples: Int = 10,
                     jsonSource: Boolean = false,
                     indexName: String = null,
                     typeName: String = null,
                     alias: String = null,
                     mapping: String = null,
                     finalIndexSetting: String = null,
                     id: String = null,
                     routing: String = null,
                     hdfsWorkDir: String = "/tmp/hive2es",
                     localDataDir: String = "/tmp/hive2es",
                     bulkActions: Int = 100,
                     bulkSize: Int = 5,
                     bulkFlushInterval: Int = 5,
                     hiveInputFields: Seq[String] = null,
                     indexESFields: Seq[String] = null,
                     indexHiveFields: Seq[String] = null,
                     zookeeper: String = null,
                     chroot: String = "/es_import_offline"
                   )

  val argsParser: OptionParser[Config] = new scopt.OptionParser[Config]("hive2es offline") {
    head("hive2es offline", "1.0")

    opt[String]("hive-table").required()
      .action((x, c) => c.copy(hiveTable = x))
      .text("Source hive table")

    opt[String]("where")
      .action((x, c) => c.copy(where = x))
      .text("Hive table query where clause")

    opt[Int]("number-of-shards").required()
      .action((x, c) => c.copy(numShards = x))
      .text("Number of ES Index Shards")

    opt[Boolean]("repartition").valueName("<true, false>")
      .action((x, c) => c.copy(repartition = x))
      .text("Whether need repartition, repartition by routing column if set to true")

    opt[Int]("partition-multiples")
      .action((x, c) => c.copy(partitionMultiples = x))
      .text("Multiples of spark partitions to es shards, default is 10, only effected when repartition set to true")

    opt[Boolean]("json-source").valueName("<true, false>")
      .action((x, c) => c.copy(jsonSource = x))
      .text("Json Source")

    opt[String]("index-name").required()
      .action((x, c) => c.copy(indexName = x))
      .text("ES index name")

    opt[String]("type-name").required()
      .action((x, c) => c.copy(typeName = x))
      .text("ES type name")

    opt[String]("alias")
      .action((x, c) => c.copy(alias = x))
      .text("ES index alias")

    opt[String]("mapping")
      .action((x, c) => c.copy(mapping = x))
      .text("ES index mapping, json format")

    opt[String]("final-index-setting")
      .action((x, c) => c.copy(finalIndexSetting = x))
      .text("Final es index setting, will be set after index loaded")

    opt[String]("id")
      .action((x, c) => c.copy(id = x))
      .text("ES ID column") // always primary key

    opt[String]("routing")
      .action((x, c) => c.copy(routing = x))
      .text("ES Routing column, default same with id if id specified")

    opt[String]("hdfs-work-dir")
      .action((x, c) => c.copy(hdfsWorkDir = x))
      .text("Hdfs work dir")

    opt[String]("local-data-dir")
      .action((x, c) => c.copy(localDataDir = x))
      .text("Local data dir")

    opt[Int]("bulk-actions")
      .action((x, c) => c.copy(bulkActions = x))
      .text("Number of bulk actions, default 100")

    opt[Int]("bulk-size")
      .action((x, c) => c.copy(bulkSize = x))
      .text("Size of bulk actions, unit M, default 5M")

    opt[Int]("bulk-flush-interval")
      .action((x, c) => c.copy(bulkFlushInterval = x))
      .text("Interval of bulk flush, unit Second, default 5s")

    opt[Seq[String]]("hive-input-fields")
      .valueName("<field_name1>,<field_name2>")
      .action((x, c) => c.copy(hiveInputFields = x))
      .text("Hive input fields")

    opt[Seq[String]]("index-es-fields")
      .valueName("<field_name1>,<field_name2>")
      .action((x, c) => c.copy(indexESFields = x))
      .text("ES Field need index")

    opt[Seq[String]]("index-hive-fields")
      .valueName("<field_name1>,<field_name2>")
      .action((x, c) => c.copy(indexHiveFields = x))
      .text("Hive Field need index")

    opt[String]("zookeeper").required()
      .action((x, c) => c.copy(zookeeper = x))
      .text("Zookeeper connect address")

    opt[String]("chroot").required()
      .action((x, c) => c.copy(chroot = x))
      .text("Zookeeper chroot path")
  }
}
