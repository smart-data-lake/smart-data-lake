/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.smartdatalake.workflow.dataobject

import java.time.Duration.ofMinutes
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ofPattern
import java.time.{Duration, LocalDateTime}

import com.splunk._
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.spark.sql._


/**
 * [[DataObject]] of type KafkaTopic.
 * Provides details to an action to read from Kafka Topics using either
  * [[org.apache.spark.sql.DataFrameReader]] or [[org.apache.spark.sql.streaming.DataStreamReader]]
  *
  * @param stream Use checkpointing to manage own state and read only new records from topic (default = true)
  * @param topicName The name of the topic to read
  * @param kafkaOptions Options for the Kafka stream reader (see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
  */
case class KafkaTopicDataObject(override val id: DataObjectId,
                                stream: String = "true",
                                topicName: String,
                                kafkaOptions: Map[String, String] = Map(),
                                override val metadata: Option[DataObjectMetadata] = None
                           )(implicit instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame {


  val options: Map[String, String] = kafkaOptions

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    //Default behaviour is to stream with checkpointing
    //as this mirrors default behaviour in non-Spark Kafka applications
    //with auto-commit = true
    val dfs = if(stream.toBoolean) {
      // Initialise a DataStream based on a topic to write using
      // DataObject with CanWriteDataStream
      spark
        .readStream
        .format("kafka")
        .options(options)
        .option("subscribe", topicName)
        .load()
    } else {
      // Initialise a DataFrame to read an entire topic to write using
      // DataObject with CanWriteDataFrame
      spark
        .read
        .format("kafka")
        .options(options)
        .option("subscribe", topicName)
        .load()
    }

    // Deserialize key/value to make human readable
    //TODO: Deal with other payload types
    dfs
      .withColumn("key", $"key" cast "string")
      .withColumn("value", $"value" cast "string")

  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = KafkaTopicDataObject
}

object KafkaTopicDataObject extends FromConfigFactory[DataObject] {

  /**
    * @inheritdoc
    */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): KafkaTopicDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[KafkaTopicDataObject].value
  }
}
