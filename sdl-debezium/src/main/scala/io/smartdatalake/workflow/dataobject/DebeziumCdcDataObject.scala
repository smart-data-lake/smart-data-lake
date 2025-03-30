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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.DebeziumConnection
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.runtime.WorkerConfig
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetBackingStore
import org.apache.kafka.connect.util.Callback
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import java.nio.ByteBuffer
import java.util
import java.util.{Base64, Properties}
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, Future}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter, setAsJavaSetConverter, mapAsScalaMapConverter, mutableMapAsJavaMap, seqAsJavaListConverter}

case class DebeziumCdcDataObject(override val id: DataObjectId,
                                 connectionId: ConnectionId,
                                 table: Table,
                                 debeziumProperties: Option[Map[String, String]] = None,
                                 maxWaitTimeInSeconds: Int = 10,
                                 override val metadata: Option[DataObjectMetadata] = None)
                                (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanCreateSparkDataFrame with CanCreateIncrementalOutput {

  val connection: DebeziumConnection = getConnection[DebeziumConnection](connectionId)

  private def getConfigPropertiesMap: Map[String, String] = {

    // If duplicate connection properties are set, prefer the ones the user has set in the config file
    var props: Map[String, String] = debeziumProperties.getOrElse(Map()) ++ connection.connectionPropertiesMap.map {
      case (key, value) => if (debeziumProperties.getOrElse(Map()).contains(key)) key -> debeziumProperties.getOrElse(Map())(key) else key -> connection.connectionPropertiesMap(key)
    }

    val defaultOffsetProperties: Map[String, String] = Map(
      "offset.storage" -> "io.smartdatalake.workflow.dataobject.SDLBDebeziumOffsetStorage",
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

    def createEmptyDataFrame(): DataFrame = {

        val schemaProperties = new Properties()
       getConfigPropertiesMap.foreach { case (key, value) => schemaProperties.setProperty(key, value) }

        Seq("offset.storage", "offset.storage.sdlb.data.object.id").foreach(schemaProperties.remove(_))

        schemaProperties.put("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")

        val schemaConsumer = new DebeziumSchemaConsumer
        val executorService = Executors.newSingleThreadExecutor
        val completionCallback = new DebeziumCompletionCallback(executorService)
        val engine = DebeziumEngine.create(classOf[Connect])
          .using(schemaProperties)
          .notifying(schemaConsumer)
          .using(completionCallback)
          .build()


        executorService.execute(engine)

        Thread.sleep(10000)
        engine.close()
        executorService.shutdown()

        val records = schemaConsumer.records

        val sparkSchema = inferSparkSchema(records.head.valueSchema())

        val rows = records.map {
          DebeziumRowConverter.convert
        }

        val df = spark.createDataFrame(rows.asJava, sparkSchema)

        val schema = extractCdcEvents(df).schema

        spark.createDataFrame(new util.ArrayList[Row](), schema)

    }

    if (context.isExecPhase) {

      val changeConsumer = new DebeziumChangeConsumer
      val executorService = Executors.newSingleThreadExecutor
      val completionCallback = new DebeziumCompletionCallback(executorService)

      val engine = DebeziumEngine.create(classOf[Connect])
        .using(properties)
        .notifying(changeConsumer)
        .using(completionCallback)
        .build()


      executorService.execute(engine)

      Thread.sleep(10000)
      engine.close()
      executorService.shutdown()

      completionCallback.error.foreach(err => throw new Exception(err))

      val records = changeConsumer.records

      records.headOption match {
        case Some(record) => {
          val sparkSchema = inferSparkSchema(record.valueSchema())
          val rows = records.map {
            DebeziumRowConverter.convert
          }

          val df = spark.createDataFrame(rows.asJava, sparkSchema)

          extractCdcEvents(df)

        }
        case None => createEmptyDataFrame()
      }

    } else {
      createEmptyDataFrame()
    }

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
      .orderBy(col(COMMIT_TIMESTAMP_COLUMN_NAME).desc)

    reorderCdcColumns(flattenDf(unionDf))

  }

  private def flattenDf(df: DataFrame): DataFrame = {
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

  protected[dataobject] var incrementalState: mutable.Map[String, String] = mutable.Map()

  /**
   * To implement incremental processing this function is called to initialize the DataObject with its state from the last increment.
   * The state is just a string. It's semantics is internal to the DataObject.
   * Note that this method is called on initializiation of the SmartDataLakeBuilder job (init Phase) and for streaming execution after every execution of an Action involving this DataObject (postExec).
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


private[smartdatalake] class DebeziumChangeConsumer extends DebeziumEngine.ChangeConsumer[ChangeEvent[SourceRecord, SourceRecord]] {


  var records: List[SourceRecord] = List()

  override def handleBatch(batch: util.List[ChangeEvent[SourceRecord, SourceRecord]], recordCommitter: DebeziumEngine.RecordCommitter[ChangeEvent[SourceRecord, SourceRecord]]): Unit = {

    batch.forEach(r => {

      records = records :+ r.value()

      recordCommitter.markProcessed(r)
    })

    recordCommitter.markBatchFinished()

  }
}

private[smartdatalake] class DebeziumSchemaConsumer extends DebeziumEngine.ChangeConsumer[ChangeEvent[SourceRecord, SourceRecord]] {


  var records: List[SourceRecord] = List()

  override def handleBatch(batch: util.List[ChangeEvent[SourceRecord, SourceRecord]], recordCommitter: DebeziumEngine.RecordCommitter[ChangeEvent[SourceRecord, SourceRecord]]): Unit = {

    if(records.isEmpty) {
     records  = records :+ batch.get(0).value() // read only the first record
    }

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

private[smartdatalake] object DebeziumRowConverter {
  def convert(record: SourceRecord): Row = {

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

}

class SDLBDebeziumOffsetStorage() extends OffsetBackingStore with SmartDataLakeLogger {

  private val SDLB_DATA_OBJECT_ID_CONFIG = "offset.storage.sdlb.data.object.id"
  private var dataObjectId: String = ""

  private val instanceRegistry = Environment.instanceRegistry

  private var data: mutable.Map[ByteBuffer, ByteBuffer] = mutable.Map()

  override def start(): Unit = {
    logger.info(s"Start SDLBDebeziumOffsetStorage for data object DebeziumCdcDataObject($dataObjectId)")
    instanceRegistry.get[DebeziumCdcDataObject](DataObjectId(dataObjectId)).incrementalState.foreach(state => {
      val key = stringToByteBuffer(state._1)
      val value = stringToByteBuffer(state._2)

      data.put(key, value)

    })

  }

  // Helper function to convert Base64 string back to ByteBuffer
  private def stringToByteBuffer(str: String): ByteBuffer = {
    val bytes = Base64.getDecoder.decode(str)
    ByteBuffer.wrap(bytes)
  }

  override def stop(): Unit = {
    logger.info(s"Stop SDLBDebeziumOffsetStorage for data object DebeziumCdcDataObject($dataObjectId)")
    data.clear()
  }

  override def get(keys: util.Collection[ByteBuffer]): Future[util.Map[ByteBuffer, ByteBuffer]] = {
    CompletableFuture.completedFuture(data.filterKeys(k => keys.contains(k)).asJava)
  }

  override def set(values: util.Map[ByteBuffer, ByteBuffer], callback: Callback[Void]): Future[Void] = {

    values.asScala.foreach(state => {
      val key = byteBufferToString(state._1)
      val value = byteBufferToString(state._2)
      instanceRegistry.get[DebeziumCdcDataObject](DataObjectId(dataObjectId)).incrementalState.put(key, value)
    })

    CompletableFuture.completedFuture(null)
  }

  // Helper function to convert ByteBuffer to Base64 string
  private def byteBufferToString(buffer: ByteBuffer): String = {
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    Base64.getEncoder.encodeToString(bytes)
  }

  override def configure(config: WorkerConfig): Unit = {
    dataObjectId = config.originalsStrings().get(SDLB_DATA_OBJECT_ID_CONFIG)
  }

  override def connectorPartitions(s: String): util.Set[util.Map[String, AnyRef]] = {
    // Not used
    Set.empty[util.Map[String, AnyRef]].asJava
  }
}
