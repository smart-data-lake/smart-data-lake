package org.apache.spark.metrics.sink

import  org.apache.spark.metrics.sink.util.Logging
/**
 * This code originates from https://github.com/mspnp/spark-monitoring and is protected by its corresponding MIT license
 */
trait LogAnalyticsConfiguration extends Logging {
  protected def getWorkspaceId: Option[String]

  protected def getSecret: Option[String]

  protected def getLogType: String

  protected def getTimestampFieldName: Option[String]

  val workspaceId: String = {
    val value = getWorkspaceId
    require(value.isDefined, "A Log Analytics Workspace ID is required")
    logger.info(s"Setting workspaceId to ${value.get}")
    value.get

  }

  val secret: String = {
    val value = getSecret
    require(value.isDefined, "A Log Analytics Workspace Key is required")
    value.get
  }

  val logType: String = {
    val value = getLogType
    logger.info(s"Setting logType to $value")
    value
  }

  val timestampFieldName: String = {
    val value = getTimestampFieldName
    logger.info(s"Setting timestampNameField to $value")
    value.orNull
  }
}
