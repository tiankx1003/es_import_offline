package io.github.tiankx1003.utils

import com.alibaba.fastjson.JSON
import com.google.common.base.Charsets
import io.github.tiankx1003.utils.ArgsParser.Config
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.CreateMode

/**
 * @author <a href="https://github.com/tiankx1003">tiankx</a>
 * @since 2022-10-03 10:21
 * @version 1.0
 */
class ServerNotifier(config: Config) {

  @transient private lazy val logger: Log = LogFactory.getLog(getClass)

  private val client: CuratorFramework = CuratorFrameworkFactory.newClient(
    config.zookeeper,
    60000,
    5000,
    new RetryNTimes(5, 5000)
  )

  client.start()

  private val indexName: String = config.indexName

  private val path = s"${config.chroot}/indices/$indexName"

  def isExisted(path: String): Boolean = {
    null != client.checkExists.forPath(path)
  }

  def persist(path: String, value: String): Unit = {
    logger.info("Persist zk path [{}] value [{}]", path, value)
    if (!isExisted(path)) {
      client
        .create()
        .withMode(CreateMode.PERSISTENT)
        .forPath(path, value.getBytes())
    }else{
      update(path, value)
    }
  }

  def update(path:String, value:String): Unit={
    logger.info("Update zk path [{}] value [{}]", path, value)
    client.setData().forPath(path, value.getBytes(Charsets.UTF_8))
  }

  def startIndex(): Boolean = {
    val data = JSON.parse(
      s"""
         |{
         |  "numberShards": ${config.numShards},
         |  "hdfsWorkDir": "${config.hdfsWorkDir}",
         |  "indexName": "$indexName",
         |  "typeName": "${config.typeName}",
         |  "state": "started"
         |}
         |""".stripMargin).toString
    persist(path, data)
    true
  }

  def completeIndex(): Boolean = {
    val data = JSON.parse(
      s"""
         |{
         |  "numberShards": ${config.numShards},
         |  "hdfsWorkDir": "${config.hdfsWorkDir}",
         |  "indexName": "${config.typeName}",
         |  "state": "completed"
         |}
         |""".stripMargin).toString
    persist(path, data)
    true
  }

  def deleteNode(): Unit = {
    logger.info(s"check path exist or not: $path")
    val stat = client.checkExists().forPath(path)
    if (stat != null) {
      client.delete().deletingChildrenIfNeeded().forPath(path)
      logger.info(s"delete path: $path")
    } else {
      logger.info("spark job don't create path yet")
    }
  }

}
