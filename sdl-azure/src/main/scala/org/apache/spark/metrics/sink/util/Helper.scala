package org.apache.spark.metrics.sink.util

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}