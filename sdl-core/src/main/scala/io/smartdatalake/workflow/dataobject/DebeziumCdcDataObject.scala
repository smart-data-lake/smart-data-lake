/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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
import com.typesafe.config.Config
import io.debezium.engine.format.Json
import io.debezium.engine.{ChangeEvent, DebeziumEngine}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.DebeziumConnection
import org.apache.spark.sql.DataFrame

import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

case class DebeziumCdcDataObject(override val id: DataObjectId,
                                 connectionId: ConnectionId,
                                 table: Table,
                                 debeziumProperties: Option[Map[String, String]] = None,
                                 maxWaitTimeInSeconds: Int = 10,
                                 override val metadata: Option[DataObjectMetadata] = None)
                                (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanCreateSparkDataFrame {

  val connection: DebeziumConnection = getConnection[DebeziumConnection](connectionId)

  private val executorService: ExecutorService  = Executors.newSingleThreadExecutor

  private def getConfigPropertiesMap: Map[String, String] = {

    // If duplicate connection properties are set, prefer the ones coming from the connections
    var props: Map[String, String] = debeziumProperties.getOrElse(Map()) ++ connection.connectionPropertiesMap.map {
      case (key, value) => if (connection.connectionPropertiesMap.contains(key)) key -> connection.connectionPropertiesMap(key) else key -> debeziumProperties.getOrElse(Map())(key)
    }

    val defaultOffsetProperties: Map[String, String] = Map(
      "offset.storage" -> "org.apache.kafka.connect.storage.FileOffsetBackingStore", // TODO: implement custom backing store to store the data in sdlb state
      "offset.storage.file.filename" -> "C://TEMP/offsets.dat", //TODO: change before commit and push
      "offset.flush.interval.ms" -> "1000")

    // If duplicate offset properties are set, prefer the ones the user has set in the config file
    props = props ++ defaultOffsetProperties.map {
      case (key, value) => if (props.contains(key)) key -> props(key) else key -> defaultOffsetProperties(key)
    }

    val defaultSchemaHistoryProperties: Map[String, String] = Map(
      "schema.history.internal" -> "io.debezium.storage.file.history.FileSchemaHistory", // TODO: Implement custom schema history that ignores the changes and just logs
      "schema.history.internal.file.filename" -> "C://TEMP/schemahistory.dat" // TODO: change before commit an push
    )

    // If duplicate schema history properties are set, prefer the ones the user has set in the config file
    props = props ++ defaultSchemaHistoryProperties.map {
      case (key, value) => if (props.contains(key)) key -> props(key) else key -> defaultSchemaHistoryProperties(key)
    }

    val defaultProperties: Map[String, String] = Map(
      "topic.prefix" -> table.fullName
    )

    // If duplicate default properties are set, prefer the ones the user has set in the config file
    props = props ++ defaultProperties.map {
      case (key, value) => if (props.contains(key)) key -> props(key) else key -> defaultProperties(key)
    }

    // Always overwrite table.include.list property to include only the changes of the table specified in the data object
    props = props ++ Map("table.include.list" -> table.fullName)

    props
  }

  private val properties: Properties = {
    val props = new Properties()
    getConfigPropertiesMap.foreach { case (key, value) => props.setProperty(key, value) }
    props
  }

  override def factory: FromConfigFactory[DataObject] = DebeziumCdcDataObject

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {

    val spark = context.sparkSession

    import spark.implicits._

    val changeConsumer = new DebeziumChangeConsumer
    val completionCallback = new DebeziumCompletionCallback(this.executorService)

    val engine = DebeziumEngine.create(classOf[Json])
      .using(properties)
      .notifying(changeConsumer)
      .using(completionCallback)
      .build()


    executorService.execute(engine)

    Thread.sleep(10000)
    engine.close()
    executorService.shutdown()

    completionCallback.error.foreach(err => throw new Exception(err))

    val jsonDataset = spark.sparkContext.parallelize(changeConsumer.records).toDS()

    val df = spark.read.json(jsonDataset)

    df

  }
}

object DebeziumCdcDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DebeziumCdcDataObject = {
    extract[DebeziumCdcDataObject](config)
  }
}


private[smartdatalake] class DebeziumChangeConsumer extends DebeziumEngine.ChangeConsumer[ChangeEvent[String, String]] {


  var records: List[String] = List()

  override def handleBatch(batch: util.List[ChangeEvent[String, String]], recordCommitter: DebeziumEngine.RecordCommitter[ChangeEvent[String, String]]): Unit = {

    batch.forEach(record => {

      this.records = this.records :+ record.value()

      recordCommitter.markProcessed(record)
    })

    recordCommitter.markBatchFinished()

  }
}

private[smartdatalake] class DebeziumCompletionCallback(executorService: ExecutorService) extends DebeziumEngine.CompletionCallback with SmartDataLakeLogger {

  var error: Option[Throwable] = None;

  override def handle(success: Boolean, message: String, error: Throwable): Unit = {
    if (success) logger.info(s"Debezium ended successfully with {$message}")
    else logger.warn(s"Debezium failed with {$message}")

    this.error = Some(error)
    this.executorService.shutdown()
  }
}


