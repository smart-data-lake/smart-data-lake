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
import io.debezium.embedded.Connect
import io.debezium.engine.{ChangeEvent, DebeziumEngine}
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.debezium.{DebeziumChangeConsumer, DebeziumCompletionCallback, DebeziumSchemaConsumer, SdlbDebeziumChangeConsumerState}
import io.smartdatalake.util.concurrent.Await
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import io.smartdatalake.workflow.connection.DebeziumConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.time.{Duration, ZonedDateTime}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.{Properties, UUID}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * [[DataObject]] of type DebeziumCdcDataObject.
 * Provides details to access Change data over Debezium Engine.
 *
 * @param id unique name of this data object
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.DebeziumConnection]]
 * @param table Source table to get change data from
 * @param debeziumProperties Properties for the specific Debezium connector
 * @param metadata (optional) data object metadata
 * @param maxWaitTimeAfterLastBatchMilliSeconds (optional) Waiting time interval for debezium to finish (when a batch arrived, the engine waits the defined interval for completion), default = 10 seconds
 * @param maxSnapshotWaitTimeMilliSeconds (optional) The maximum duration waiting for the snapshot to end, default = 5 seconds
 *
 * Example config:
 *
 * Source {
 *	type = DebeziumCdcDataObject
 *	connectionId = "connection1"
 *	table = "Test"
 *	debeziumProperties = {
 *		"database.server.id" = "1234345345"
 *		"plugin.name" = "pgoutput"
 *		"schema.history.internal" = "io.debezium.storage.file.history.FileSchemaHistory"
 *		"schema.history.internal.file.filename" = "C://TEMP/schemahistory.dat"
 *	}
 * }
 */
case class DebeziumCdcDataObject(override val id: DataObjectId,
                                 connectionId: ConnectionId,
                                 table: Table,
                                 schemaMin: Option[GenericSchema] = None,
                                 debeziumProperties: Option[Map[String, String]] = None,
                                 maxWaitTimeAfterLastBatchMilliSeconds: Option[Int] = Some(10000),
                                 maxSnapshotWaitTimeMilliSeconds: Option[Int] = Some(5000),
                                 override val metadata: Option[DataObjectMetadata] = None)
                                (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanCreateSparkDataFrame with CanCreateIncrementalOutput with SchemaValidation {

  val connection: DebeziumConnection = getConnection[DebeziumConnection](connectionId)

  private def debeziumPropertiesForEngine: Properties = {

    // If duplicate connection properties are set, prefer the ones the user has set in the config file
    var props: Map[String, String] = debeziumProperties.getOrElse(Map()) ++ connection.connectionPropertiesMap.map {
      case (key, value) => if (debeziumProperties.getOrElse(Map()).contains(key)) key -> debeziumProperties.getOrElse(Map())(key) else key -> connection.connectionPropertiesMap(key)
    }

    val defaultOffsetProperties: Map[String, String] = Map(
      "offset.storage" -> "io.smartdatalake.debezium.SDLBDebeziumOffsetStorage",
      "offset.storage.sdlb.data.object.id" -> this.id.id,
      "offset.flush.interval.ms" -> "10000")

    // If duplicate offset properties are set, prefer the ones the user has set in the config file
    props = props ++ defaultOffsetProperties.map {
      case (key, value) => if (props.contains(key)) key -> props(key) else key -> defaultOffsetProperties(key)
    }

    val defaultProperties: Map[String, String] = Map(
      "topic.prefix" -> table.fullName,
      "include.schema.changes" -> "false",
      "name" -> id.toString,
      "tombstones.on.delete" -> "false",
      "decimal.handling.mode" -> "string" // string, because double cannot handle fully completely decimal value and precise is not possible because spark returns an error when processing -> Root cause is 'ClassCastException: class java.math.BigDecimal cannot be cast to class [B (java.math.BigDecimal and [B are in module java.base of loader 'bootstrap')' correct data type can be adjusted f.ex. with a cast in a transformer
    )

    // If duplicate default properties are set, prefer the ones the user has set in the config file
    props = props ++ defaultProperties.map {
      case (key, value) => if (props.contains(key)) key -> props(key) else key -> defaultProperties(key)
    }

    // Always overwrite table.include.list property to include only the changes of the table specified in the data object
    props = props ++ Map("table.include.list" -> table.fullName)

    val propsForEngine = new Properties();
    props.foreach { case (key, value) => propsForEngine.setProperty(key, value) }
    propsForEngine
  }

  override def factory: FromConfigFactory[DataObject] = DebeziumCdcDataObject

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {

    implicit val spark: SparkSession = context.sparkSession

    def getRecordsFromDebeziumEngine(
                                      properties: Properties,
                                      changeConsumer: DebeziumEngine.ChangeConsumer[ChangeEvent[SourceRecord, SourceRecord]] with SdlbDebeziumChangeConsumerState,
                                      executorService: ExecutorService = Executors.newSingleThreadExecutor
                         ): Seq[SourceRecord] = {

      val completionCallback = new DebeziumCompletionCallback(executorService)

      val engine = DebeziumEngine.create(classOf[Connect])
        .using(properties)
        .notifying(changeConsumer)
        .using(completionCallback)
        .build()

      executorService.execute(engine)

      val snapshotStarted = ZonedDateTime.now()
      var isConsuming = false

      do {

        logger.info(s"($id) Start consuming records from Debezium")
        val startCount = changeConsumer.records.size

        Await.until({
          logger.info(s"($id) Waiting for Debezium engine to shutdown or if maxWaitTimeAfterLastBatch is reached")
          checkDebeziumEngineEnded(executorService, changeConsumer)

        }, Duration.ofSeconds(1))

        logger.info(s"($id) checkDebeziumEngineEnded done")

        val endCount = changeConsumer.records.size
        isConsuming = endCount > startCount

      } while(changeConsumer.isSnapshotting
        && isConsuming
        && ZonedDateTime.now().isBefore(snapshotStarted.plus(Duration.ofMillis(maxSnapshotWaitTimeMilliSeconds.get)))
      )

      engine.close()

      executorService.shutdown()

      while(!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.info(s"($id) Waiting another 5 seconds for the Debezium embedded engine to shutdown")
      }

      completionCallback.error.foreach(err => throw new Exception(err))

      changeConsumer.records

    }

    def checkDebeziumEngineEnded(service: ExecutorService, changeConsumer: SdlbDebeziumChangeConsumerState): Boolean = {

      if(service.isShutdown) {
        logger.info(s"($id) Executor service is shutdown")
        return true
      }

      if(changeConsumer.records.nonEmpty && context.phase == ExecutionPhase.Init){
        logger.info(s"($id) Got first record for schema detection.")
        return true
      }

      val lastRecordTimestamp = changeConsumer.lastRecordTimestamp
      if(maxWaitTimeAfterLastBatchMilliSeconds.isDefined && ZonedDateTime.now().isAfter(lastRecordTimestamp.plus(Duration.ofMillis(maxWaitTimeAfterLastBatchMilliSeconds.get)))) {
        logger.info(s"($id) Max waiting time after last batch reached")
        return true
      }

      false
    }

    def createEmptyDataFrame(): DataFrame = {

      /**
       * Schema for empty dataframe is created based on one of the following cases:
       * 1. Init Phase
       *    Debezium is called with the custom SchemaConsumer
       *    - Table is empty with no previous events: -> No records are returned so schema is inferred from schemaMin. If not set an exception is thrown.
       *    - Table has records: -> Schema is inferred from the first record
       * 2. Exec Phase -> Schema is inferred from schemaMin
       */

        val schema = if (schemaMin.isDefined && context.isExecPhase) {
          schemaMin.get.asInstanceOf[SparkSchema].inner
        } else {
          val schemaProperties = debeziumPropertiesForEngine

          Seq("offset.storage", "offset.storage.sdlb.data.object.id", "snapshot.mode", "name").foreach(schemaProperties.remove(_))

          schemaProperties.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
          schemaProperties.put("snapshot.mode", "initial_only")
          schemaProperties.put("name", UUID.randomUUID().toString)
          schemaProperties.put("snapshot.select.statement.overrides", table.fullName)
          schemaProperties.put(s"snapshot.select.statement.overrides.${table.fullName}", s"SELECT * FROM ${table.fullName} LIMIT 1")


          val records = getRecordsFromDebeziumEngine(schemaProperties, changeConsumer = new DebeziumSchemaConsumer)

          if (records.isEmpty) {
            schemaMin match {
              case Some(schemaMin) => schemaMin.asInstanceOf[SparkSchema].inner
              case None => throw new IllegalArgumentException(
                s"""($id) missing schemaMin on empty table.
                   |Schema could not be determined by SDLB, because Debezium did not return a record.
                   |Please set schemaMin parameter in the data object.
                   |Use the data types that Debezium would return, not the one JDBC would return. Check the Debezium Connector documentation.
                   |""".stripMargin)
            }
          } else {
            val df = DebeziumEventConverter.convert(records)(spark)
            df.schema
          }
        }

      DataFrameUtil.getEmptyDataFrame(schema)

    }

    val df = if (context.isExecPhase) {

      val records = getRecordsFromDebeziumEngine(debeziumPropertiesForEngine, changeConsumer = new DebeziumChangeConsumer)

      records.headOption match {
        case Some(_) => DebeziumEventConverter.convert(records)(spark)
        case None => createEmptyDataFrame()
      }

    } else {
      createEmptyDataFrame()
    }

    validateSchemaMin(SparkSchema(df.schema), "read")
    df

  }

  private[smartdatalake] var incrementalState: mutable.Map[String, String] = mutable.Map()

  /**
   * To implement incremental processing this function is called to initialize the DataObject with its state from the last increment.
   * The state is just a string. It's semantics is internal to the DataObject.
   * Note that this method is called on initializiation of the SmartDataLakeBuilder job (init Phase).
   * When starting SDLB with the --streaming option, it will be called after every execution of an Action involving this DataObject (postExec).
   *
   * @param state Internal state of last increment. If None then the first increment (may be a full increment) is delivered.
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {

    state match {
      case Some(s) =>
        s.split(",").foreach { pair =>
          val Array(key, value) = pair.split(":")
          incrementalState.put(key, value)
        }
      case _ => incrementalState = mutable.Map()
    }
  }

  /**
   * Return the state of the last increment or empty if no increment was processed.
   */
  override def getState: Option[String] = {
    val state = incrementalState.map { case (key, value) =>
      s"$key:$value"
    }.mkString(",")

    Some(state)
  }
}

object DebeziumCdcDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DebeziumCdcDataObject = {
    extract[DebeziumCdcDataObject](config)
  }
}

/**
 * Helper object to convert from debezium events in SourceRecord format to sdlb compatible spark dataframe
 */
private object DebeziumEventConverter {

  def convert(records: Seq[SourceRecord])(implicit spark: SparkSession): DataFrame = {

    val sparkSchema = inferSparkSchema(records.head.valueSchema())
    val rows = records.map(recordToRow)

    val df = spark.createDataFrame(rows.asJava, sparkSchema)
    extractCdcEvents(df)
  }

  private def inferSparkSchema(schema: Schema): StructType = {
    val fields = schema.fields().asScala.map { field =>
      val fieldName = field.name()
      val fieldType: DataType = field.schema().`type`() match {
        case Type.INT8 => ByteType
        case Type.INT16 => ShortType
        case Type.INT32 => IntegerType
        case Type.INT64 => LongType
        case Type.FLOAT32 => FloatType
        case Type.FLOAT64 => DoubleType
        case Type.BOOLEAN => BooleanType
        case Type.STRING => StringType
        case Type.BYTES => BinaryType
        case Type.MAP => {
          // Infer key and value types for MapType
          val keyType = inferSparkSchema(field.schema().keySchema())
          val valueType = inferSparkSchema(field.schema().valueSchema())
          MapType(keyType, valueType)
        }
        case Type.ARRAY => {
          // Infer the element type for ArrayType
          val elementType = inferSparkSchema(field.schema().valueSchema())
          ArrayType(elementType)
        }
        case Type.STRUCT => inferSparkSchema(field.schema())
        case _ => StringType
      }
      StructField(fieldName, fieldType, nullable = true)
    }

    StructType(fields.toArray)
  }

  private def recordToRow(record: SourceRecord): Row = {

    val valueStruct = record.value().asInstanceOf[org.apache.kafka.connect.data.Struct]
    structToRow(valueStruct)
  }

  // Helper function to extract data from a Struct
  private def structToRow(struct: Struct): Row = {
    val values = struct.schema().fields().asScala.map { field: Field =>
      val fieldValue = struct.get(field)
      fieldValue match {
        case s: Struct => structToRow(s) // Recursively handle nested structs
        case _ => fieldValue // Primitive types
      }
    }.toSeq

    Row(values: _*)

  }

  private def flattenDebeziumDf(df: DataFrame): DataFrame = {
    var newDF = df
    for (colName <- df.columns) {
      val colType = df.schema(colName).dataType
      colType match {
        case structType: StructType =>
          for (fieldName <- structType.fieldNames) {
            newDF = newDF.withColumn(fieldName, col(s"$colName.$fieldName"))
          }
          newDF = newDF.drop(colName)
        case _ =>
      }
    }
    newDF
  }

  private def reorderCdcColumns(df: DataFrame): DataFrame = {

    val colsToMove = Seq(COMMIT_TYPE_COLUMN_NAME, COMMIT_TIMESTAMP_COLUMN_NAME)

    val allColumns = df.columns

    val remainingColumns = allColumns.filterNot(colsToMove.contains)

    // Create the new order: remaining columns + colsToMove at the end
    val newColumnOrder = remainingColumns ++ colsToMove

    // Reorder DataFrame by selecting columns in the new order
    val reorderedDF = df.select(newColumnOrder.head, newColumnOrder.tail: _*)

    reorderedDF
  }

  private val COMMIT_TYPE_COLUMN_NAME = "__commit_event"
  private val COMMIT_TIMESTAMP_COLUMN_NAME = "__event_timestamp"
  private def extractCdcEvents(df: DataFrame): DataFrame = {

    val updateBeforeDf = df.filter(col("op") === "u")
      .withColumn("event_data", col("before"))
      .withColumn(COMMIT_TYPE_COLUMN_NAME, lit("update_preimage"))

    val updateAfterDf = df.filter(col("op") === "u")
      .withColumn("event_data", col("after"))
      .withColumn(COMMIT_TYPE_COLUMN_NAME, lit("update_postimage"))

    val otherOperationsDf = df.filter(col("op") =!= "u")
      .withColumn("event_data", coalesce(col("after"), col("before")))
      .withColumn(COMMIT_TYPE_COLUMN_NAME,
        when(col("op") === "c", lit("create"))
          .when(col("op") === "d", lit("delete"))
          .otherwise(lit("read")))

    val unionDf = updateBeforeDf.union(updateAfterDf).union(otherOperationsDf)
      .withColumn(COMMIT_TIMESTAMP_COLUMN_NAME, from_unixtime(col("source.ts_ms") / 1000).cast(TimestampType))
      .drop("before", "after", "source", "op", "ts_ms", "ts_us", "ts_ns", "transaction")

    reorderCdcColumns(flattenDebeziumDf(unionDf))

  }

}
