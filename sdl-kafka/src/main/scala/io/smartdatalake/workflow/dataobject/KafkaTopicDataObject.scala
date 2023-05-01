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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.KafkaConnection
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.KafkaColumnType.{AvroSchemaRegistry, JsonSchemaRegistry, KafkaColumnType}
import io.smartdatalake.workflow.dataobject.TopicPartitionOffsets.getOffsetForSpark
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.confluent.SubjectType.SubjectType
import org.apache.spark.sql.confluent.avro.{AvroSchemaConverter, ConfluentAvroConnector}
import org.apache.spark.sql.confluent.json.ConfluentJsonConnector
import org.apache.spark.sql.confluent.{ConfluentConnector, SubjectType}
import org.apache.spark.sql.functions.{col, from_json, to_json, udf}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAccessor, TemporalQuery}
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Definition of date partition column to extract formatted time into column.
 *
 * @param colName date partition column name to extract time into column on batch read
 * @param timeFormat time format for timestamp in date partition column, definition according to java DateTimeFormatter. Default is "yyyyMMdd".
 * @param timeUnit time unit for timestamp in date partition column, definition according to java ChronoUnit. Default is "days".
 * @param timeZone time zone used for date logic. If not specified, java system default is used.
 * @param includeCurrentPartition If the current partition should be included. Default is to list only completed partitions.
 *                                Attention: including the current partition might result in data loss if there is more data arriving.
 *                                But it might be useful to export all data before a scheduled maintenance.
 */
case class DatePartitionColumnDef(colName: String, timeFormat: String = "yyyyMMdd", timeUnit: String = "days", timeZone: Option[String] = None, includeCurrentPartition: Boolean = false) {
  @transient lazy private[smartdatalake] val formatter = DateTimeFormatter.ofPattern(timeFormat) // not serializable -> transient lazy to use in udf
  private[smartdatalake] val chronoUnit = ChronoUnit.valueOf(timeUnit.toUpperCase)
  private[smartdatalake] val zoneId = timeZone.map(ZoneId.of).getOrElse(ZoneId.systemDefault)
  private[smartdatalake] def parse(value: String ): LocalDateTime = {
    formatter.parseBest(value, TemporalQueries.LocalDateTimeQuery, TemporalQueries.LocalDateQuery, TemporalQueries.LocalYearMonthQuery ) match {
      case d: LocalDateTime => d
      case d: LocalDate => d.atStartOfDay()
      case d: YearMonth => d.atDay(1).atStartOfDay()
    }
  }
  private[smartdatalake] def format( dateTime: LocalDateTime ) = dateTime.format(formatter)
  private[smartdatalake] def next(dateTime: LocalDateTime, units: Int = 1) = dateTime.plus(units, chronoUnit)
  private[smartdatalake] def previous(dateTime: LocalDateTime, units: Int = 1) = dateTime.minus(units, chronoUnit)
  private[smartdatalake] def current = LocalDateTime.now().truncatedTo(chronoUnit)
}

private object TemporalQueries {
  val LocalDateTimeQuery: TemporalQuery[LocalDateTime] = (temporal: TemporalAccessor) => LocalDateTime.from(temporal)
  val LocalDateQuery: TemporalQuery[LocalDate] = (temporal: TemporalAccessor) => LocalDate.from(temporal)
  val LocalYearMonthQuery: TemporalQuery[YearMonth] = (temporal: TemporalAccessor) => YearMonth.from(temporal)
}

/**
 * [[DataObject]] of type KafkaTopic.
 * Provides details to an action to read from Kafka Topics using either
 * [[org.apache.spark.sql.DataFrameReader]] or [[org.apache.spark.sql.streaming.DataStreamReader]]
 *
 * Key & value schema can be automatically read from and written to confluent schema registry for Json and Avro.
 * Json and Avro can also be parsed with a fixed schema.
 *
 * Can interpret record timestamp as SDLB partition values by setting datePartitionCol attribute. This allows to use this DataObject as input for PartitionDiffMode.
 * The DataObject does not support writing with SDLB partition values, as timestamp is autogenerated by Kafka using current time.
 *
 * Support incremental output and use with DataObjectStateIncrementalMode.
 *
 * @param topicName The name of the topic to read
 * @param keyType    Optional type the key column should be converted to. If none is given it will be interpreted as string.
 * @param keySchema  An optional schema for parsing the key column. This can be used if keyType = Json or Avro to parse the corresponding content.
 *                   Define the schema by using one of the schema providers DDL, jsonSchemaFile, avroSchemaFile, xsdFile or caseClassName.
 *                   The schema provider and its configuration value must be provided in the format <PROVIDERID>#<VALUE>.
 *                   A DDL-formatted string is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param valueType  Optional type the value column should be converted to. If none is given it will be interpreted as string.
 * @param valueSchema An optional schema for parsing the value column. This has to be specified if valueType = Json or Avro to parse the corresponding content.
 *                    Define the schema by using one of the schema providers DDL, jsonSchemaFile, avroSchemaFile, xsdFile or caseClassName.
 *                    The schema provider and its configuration value must be provided in the format <PROVIDERID>#<VALUE>.
 *                    A DDL-formatted string is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param allowSchemaEvolution If set to true schema evolution within schema registry will automatically occur when writing to this DataObject with different key or value schema, otherwise SDL will stop with error.
 *                             This only applies if keyType or valueType is set to Json/AvroSchemaRegistry.
 *                             Kafka Schema Evolution implementation will update schema if existing records with old schema can be read with new schema (backward compatible). Otherwise an IncompatibleSchemaException is thrown.
 * @param selectCols Columns to be selected when reading the DataFrame. Available columns are key, value, topic,
 *                   partition, offset, timestamp, timestampType. If key/valueType is AvroSchemaRegistry the key/value column are
 *                   convert to a complex type according to the avro schema. To expand it select "value.*".
 *                   Default is to select key and value.
 * @param datePartitionCol definition of date partition column to extract formatted timestamp into column.
 *                   This is used to list existing partition and is added as additional column on batch read.
 * @param batchReadConsecutivePartitionsAsRanges Set to true if consecutive partitions should be combined as one range of offsets when batch reading from topic. This results in less tasks but can be a performance problem when reading many partitions. (default=false)
 * @param batchReadMaxOffsetsPerTask Set number of offsets per Spark task when batch reading from topic.
 * @param options    Options for the Kafka stream reader (see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).
 *                   These options override connection.options.
 */
case class KafkaTopicDataObject(override val id: DataObjectId,
                                topicName: String,
                                connectionId: ConnectionId,
                                keyType: KafkaColumnType = KafkaColumnType.String,
                                keySchema: Option[GenericSchema] = None,
                                valueType: KafkaColumnType = KafkaColumnType.String,
                                valueSchema: Option[GenericSchema] = None,
                                override val allowSchemaEvolution: Boolean = false,
                                selectCols: Seq[String] = Seq("key", "value"),
                                datePartitionCol: Option[DatePartitionColumnDef] = None,
                                batchReadConsecutivePartitionsAsRanges: Boolean = false,
                                batchReadMaxOffsetsPerTask: Option[Int] = None,
                                override val options: Map[String, String] = Map(),
                                override val metadata: Option[DataObjectMetadata] = None
                           )(implicit instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateIncrementalOutput with CanCreateSparkDataFrame with CanCreateStreamingDataFrame with CanWriteSparkDataFrame with CanHandlePartitions with SchemaValidation with CanEvolveSchema {

  override val partitions: Seq[String] = datePartitionCol.map(_.colName).toSeq
  override val expectedPartitionsCondition: Option[String] = None // expect all partitions to exist
  private val udfFormatPartition = udf((ts:Timestamp) => ts.toLocalDateTime.truncatedTo(datePartitionCol.get.chronoUnit).format(datePartitionCol.get.formatter))

  override def schemaMin: Option[GenericSchema] = None // minimal schema doesn't make sense, as schema is always fully defined.

  private val connection = getConnection[KafkaConnection](connectionId)

  if (keyType==KafkaColumnType.JsonSchemaRegistry || valueType==KafkaColumnType.JsonSchemaRegistry) assert(connection.schemaRegistry.nonEmpty, s"($id) If key or value is of type JsonSchemaRegistry, the schemaRegistry must be defined in the connection")
  if (keyType==KafkaColumnType.AvroSchemaRegistry || valueType==KafkaColumnType.AvroSchemaRegistry) assert(connection.schemaRegistry.nonEmpty, s"($id) If key or value is of type AvroSchemaRegistry, the schemaRegistry must be defined in the connection")
  if (keyType==KafkaColumnType.Json || keyType==KafkaColumnType.Avro) assert(keySchema.nonEmpty, s"($id) If key type is Json or Avro, a keySchema must be specified")
  else if (keySchema.isDefined) logger.warn(s"($id) keySchema is ignored if keyType = $keyType")
  if (valueType==KafkaColumnType.Json || valueType==KafkaColumnType.Avro) assert(valueSchema.nonEmpty, s"($id) If value type is Json or Avro, a valueSchema must be specified")
  else if (valueSchema.isDefined) logger.warn(s"($id) valueSchema is ignored if valueType = $valueType")
  if (allowSchemaEvolution && Seq(keyType,valueType).intersect(Seq(JsonSchemaRegistry,AvroSchemaRegistry)).isEmpty) logger.warn(s"($id) allowSchemaEvolution=true is ignored if keyType or valueType is not set to Json/AvroSchemaRegistry")
  require(batchReadMaxOffsetsPerTask.isEmpty || batchReadMaxOffsetsPerTask.exists(_>0), s"($id) batchReadMaxOffsetsPerTask must be greater than 0")

  @transient lazy val keyConfluentConnector: Option[ConfluentConnector] = keyType match {
    case KafkaColumnType.JsonSchemaRegistry => connection.schemaRegistry.map(ConfluentJsonConnector(_))
    case KafkaColumnType.AvroSchemaRegistry => connection.schemaRegistry.map(ConfluentAvroConnector(_))
    case _ => None
  }

  @transient lazy val valueConfluentConnector: Option[ConfluentConnector] = valueType match {
    case KafkaColumnType.JsonSchemaRegistry => connection.schemaRegistry.map(ConfluentJsonConnector(_))
    case KafkaColumnType.AvroSchemaRegistry => connection.schemaRegistry.map(ConfluentAvroConnector(_))
    case _ => None
  }
  private val instanceOptions = connection.sparkOptions ++ options

  // consumer for reading topic metadata and committing offsets wiht KafkaStateIncrementalMode
  private def consumer(implicit context: ActionPipelineContext) = {
    if (_consumer.isEmpty) {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.brokers)
      // consumer is only used for reading topic metadata; auto commit is never needed and de/serializers are not relevant
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      options.get("groupIdPrefix").foreach(prefix => props.put(ConsumerConfig.GROUP_ID_CONFIG, prefix + context.application))
      instanceOptions
        .filter { case (k,v) => k.startsWith(connection.KafkaConfigOptionPrefix)} // only kafka specific options
        .foreach { case (k,v) => props.put(k.stripPrefix(connection.KafkaConfigOptionPrefix),v)}
      _consumer = Some(new KafkaConsumer(props))
    }
    _consumer.get
  }
  @transient private var _consumer: Option[KafkaConsumer[_,_]] = None

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    // test schema registry connection
    connection.testSchemaRegistry()
    // test kafka connection and topic existing
    require(connection.topicExists(topicName), s"($id) topic $topicName doesn't exist")
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    // check schema compatibility
    require(df.columns.toSet == Set("key","value"), s"($id) Expects columns key, value in DataFrame for writing to Kafka. Given: ${df.columns.mkString(", ")}")
    keySchema.foreach(schema => validateSchema(schema, SparkSchema(df.schema("key").dataType.asInstanceOf[StructType]), "write (keySchema)"))
    valueSchema.foreach(schema => validateSchema(schema, SparkSchema(df.schema("value").dataType.asInstanceOf[StructType]), "write (valueSchema)"))
    convertToKafka(keyType, df("key"), SubjectType.key, keySchema, eagerCheck = true)
    convertToKafka(valueType, df("value"), SubjectType.value, valueSchema, eagerCheck = true)
  }

  override def getStreamingDataFrame(options: Map[String,String], schema: Option[StructType])(implicit context: ActionPipelineContext): DataFrame = {
    val dfRaw = context.sparkSession
      .readStream
      .format("kafka")
      .options(instanceOptions ++ options) // options override kafkaOptions override connection.kafkaOptions
      .option("subscribe", topicName)
      .load()
    convertToReadDataFrame(dfRaw)
  }

  private def convertToReadDataFrame(dfRaw: DataFrame): DataFrame = {
    import DataFrameUtil._

    // convert key & value
    val colsToSelect = ((if (selectCols.nonEmpty) selectCols else Seq("kafka.*")) ++ partitions).distinct.map(col)
    val df = dfRaw
      .withColumn("key", convertFromKafka(keyType, col("key"), SubjectType.key, keySchema))
      .withColumn("value", convertFromKafka(valueType, col("value"), SubjectType.value, valueSchema))
      .as("kafka")
      .withOptionalColumn(datePartitionCol.map(_.colName), udfFormatPartition(col("timestamp")))
      .select(colsToSelect:_*)
    // return
    df
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession

    // get DataFrame from topic
    val dfRaw = if (_kafkaStateIncrementalModeEnabled && context.isExecPhase) {
      val partitions = consumer.partitionsFor(topicName)
      val topicPartitions = partitions.asScala.map(p => new TopicPartition(topicName, p.partition))
      val committedOffsets = getCommittedOffsets(partitions.asScala.map(p => new TopicPartition(topicName, p.partition)))
      val currentOffsets = getCurrentOffsets(topicPartitions)
      val currentOffsetsLkp = getCurrentOffsets(topicPartitions).toMap
      val endingOffsets = if (_delayedMaxTimestamp.isDefined) {
        getTopicPartitionsAtTstmp(topicPartitions, _delayedMaxTimestamp.get.toLocalDateTime)
          .map { case (p, o) => (p.partition, Option(o).map(_.offset).orElse(currentOffsetsLkp(p.partition)).orElse(Some(0L))) }
      } else currentOffsets
      incrementalOutputState = Some(endingOffsets)
      logger.debug(s"($id) incremental state current offsets are: ${endingOffsets.mkString(",")}")
      createDataFrameForTopicPartitionOffsets(TopicPartitionOffsets.fromOffsets(topicName, committedOffsets, endingOffsets), s"increment (kafka state)")
    } else if (incrementalOutputState.nonEmpty && context.isExecPhase) {
      val lastOffsets = incrementalOutputState.get
      val partitions = consumer.partitionsFor(topicName)
      assert(lastOffsets.map(_._1).sorted == partitions.asScala.map(_.partition).sorted, s"($id) last incremental state kafka partitions are different from current kafak topics partitions: ${lastOffsets.map(_._1).sorted.mkString(",")} != ${partitions.asScala.map(_.partition).sorted.mkString(",")}")
      val currentOffsets = getCurrentOffsets(partitions.asScala.map(p => new TopicPartition(topicName, p.partition)))
      incrementalOutputState = Some(currentOffsets)
      logger.debug(s"($id) incremental state current offsets are: ${currentOffsets.mkString(",")}")
      createDataFrameForTopicPartitionOffsets(TopicPartitionOffsets.fromOffsets(topicName, lastOffsets, currentOffsets), s"increment")
    } else if (partitionValues.nonEmpty) {
      assert(datePartitionCol.nonEmpty, s"($id) Can not process partition values when datePartitionCol is not configured!")
      assert(partitionValues.flatMap(_.keys).distinct == datePartitionCol.map(_.colName).toSeq, s"($id) partition value keys (${partitionValues.flatMap(_.keys).distinct}) must match datePartitionCol.colName (${datePartitionCol.map(_.colName)})!")
      implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(_.atZone(datePartitionCol.get.zoneId).toInstant)
      // create date for partition values
      val dateRanges = partitionValues.map {
        partitionValue =>
          val startTimeIncl = try {
            datePartitionCol.get.parse(partitionValue(datePartitionCol.get.colName).toString)
          } catch {
            case ex: Exception => throw new IllegalStateException(s"($id) Can not parse startTime from partition value $partitionValue with ${datePartitionCol.get.formatter}", ex)
          }
          val endTimeExcl = datePartitionCol.get.next(startTimeIncl)
          (startTimeIncl, endTimeExcl)
      }
      // combine consecutive partition values for performance reasons
      val dateRangesCombined = if (batchReadConsecutivePartitionsAsRanges) {
        dateRanges.sortBy(_._1).foldLeft(Seq[(LocalDateTime,LocalDateTime)]()) {
          case (acc, (startTimeIncl, endTimeExcl)) =>
            if (acc.isEmpty) acc :+ (startTimeIncl, endTimeExcl)
            else {
              val (prevStartTimeIncl, prevEndTimeExcl) = acc.last
              if (prevEndTimeExcl == startTimeIncl) acc.init :+ (prevStartTimeIncl, endTimeExcl)
              else acc :+ (startTimeIncl, endTimeExcl)
            }
        }
      } else dateRanges
      logger.debug(s"($id) querying date ranges $dateRangesCombined for topic $topicName with readConsecutivePartitionsAsRanges=$batchReadConsecutivePartitionsAsRanges")
      // create and union DataFrames
      val dfsRaw = dateRangesCombined.map {
        case (startTimeIncl, endTimeExcl) =>
          val topicPartitionOffsets = getPartitionOffsetsForTimePeriod(startTimeIncl, endTimeExcl)
          createDataFrameForTopicPartitionOffsets(topicPartitionOffsets, s"time period $startTimeIncl - $endTimeExcl")
      }
      dfsRaw.reduce(_ union _)
    } else {
      logger.info(s"($id) creating data frame for whole topic $topicName")
      createSparkDataFrameInternal("earliest", "latest")
    }

    convertToReadDataFrame(dfRaw)
  }

  private def createSparkDataFrameInternal(startingOffsets: String, endingOffsets: String)(implicit session: SparkSession) = {
    session
      .read
      .format("kafka")
      .options(instanceOptions)
      .option("subscribe", topicName)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", endingOffsets) // endingOffsets are exclusive
      .load()
  }

  private def getPartitionOffsetsForTimePeriod(startTimeIncl: LocalDateTime, endTimeExcl: LocalDateTime)(implicit context: ActionPipelineContext): Seq[TopicPartitionOffsets] = {
    val partitions = consumer.partitionsFor(topicName)
    require(partitions!=null, s"($id) topic $topicName doesn't exist")
    val topicPartitions = partitions.asScala.map(p => new TopicPartition(topicName, p.partition))
    val topicPartitionsStart = getTopicPartitionsAtTstmp(topicPartitions, startTimeIncl).toMap
    val topicPartitionsEnd = getTopicPartitionsAtTstmp(topicPartitions, endTimeExcl).toMap
    topicPartitions.map( tp => TopicPartitionOffsets( tp, Option(topicPartitionsStart(tp)).map(_.offset), Option(topicPartitionsEnd(tp)).map(_.offset)))
  }

  /**
   * Create a DataFrame filtered to the given offsets:
   *   1. split offsets into tasks according to maxOffsetsPerTask
   *   1. Create DataFrames for all tasks
   *   1. Union DataFrames
   *
   * @return a DataFrame filtered to given offsets.
   */
  def createDataFrameForTopicPartitionOffsets(topicPartitionOffsets: Seq[TopicPartitionOffsets], logInfo: String)(implicit session: SparkSession): DataFrame = {
    // split offsets according to maxOffsetsPerTask
    val topicPartitionOffsetsSplitted = topicPartitionOffsets.map( tpo => if (batchReadMaxOffsetsPerTask.isDefined) tpo.split(batchReadMaxOffsetsPerTask.get) else Seq(tpo))
    // ensure that every partition has the same number of tasks by adding empty entries
    val maxNbOfTasksPerPartition = topicPartitionOffsetsSplitted.map(_.size).max
    val topicPartitionOffsetsBalanced = topicPartitionOffsetsSplitted.map( tpos => tpos ++ tpos.last.getEmptyEndEntries(maxNbOfTasksPerPartition-tpos.size))
    // transpose so that we have a list of tasks for every partition per query
    val topicPartitionOffsetsQueries = topicPartitionOffsetsBalanced.transpose
    // create data frames
    val dfs = topicPartitionOffsetsQueries.zipWithIndex.map {
      case (tpos,idx) =>
        val startingOffsets = tpos.sortBy(_.topicPartition.partition).map(_.getStartOffsetForSpark).mkString(",")
        val endingOffsets = tpos.sortBy(_.topicPartition.partition).map(_.getEndOffsetForSpark).mkString(",")
        logger.info(s"($id) creating data frame $idx for $logInfo of topic $topicName: startingOffsets=$startingOffsets, endingOffsets=$endingOffsets")
        createSparkDataFrameInternal(s"""{"$topicName":{$startingOffsets}}""", s"""{"$topicName":{$endingOffsets}}""")
    }
    dfs.reduce(_ union _)
  }

  private def convertToWriteDataFrame(df: DataFrame): DataFrame = {
    require(df.columns.toSet == Set("key","value"), s"($id) Expects columns key, value in DataFrame for writing to Kafka. Given: ${df.columns.mkString(", ")}")
    keySchema.foreach(schema => validateSchema(schema, SparkSchema(df.schema("key").dataType.asInstanceOf[StructType]), "read (keySchema)"))
    valueSchema.foreach(schema => validateSchema(schema, SparkSchema(df.schema("value").dataType.asInstanceOf[StructType]), "read (valueSchema)"))
    df.select(
      convertToKafka(keyType, col("key"), SubjectType.key, keySchema).as("key"),
      convertToKafka(valueType, col("value"), SubjectType.value, valueSchema).as("value")
    )
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): Unit = {
    assert(partitionValues.isEmpty, s"($id) KafkaTopicDataObject does not support writing using partition values: partitionValues=${partitionValues.mkString(",")}")
    convertToWriteDataFrame(df)
      .write
      .format("kafka")
      .options(instanceOptions)
      .option("topic", topicName)
      .save
  }

  override def writeStreamingDataFrame(df: GenericDataFrame, trigger: Trigger, options: Map[String, String], checkpointLocation: String, queryName: String, outputMode: OutputMode, saveModeOptions: Option[SaveModeOptions] = None)
                                      (implicit context: ActionPipelineContext): StreamingQuery = {
    df match {
      case sparkDf: SparkDataFrame =>
        convertToWriteDataFrame(sparkDf.inner)
          .writeStream
          .format("kafka")
          .trigger(trigger)
          .queryName(queryName)
          .outputMode(outputMode)
          .options(instanceOptions ++ options)
          .option("checkpointLocation", checkpointLocation)
          .option("topic", topicName)
          .start()
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeStreamingDataFrame")
    }
  }

  private def getCommittedOffsets(partitions: Seq[TopicPartition])(implicit context: ActionPipelineContext) = {
    consumer.committed(partitions.toSet.asJava).asScala.map {
      case (topicPartition, offsetMeta) => (topicPartition.partition, Option(offsetMeta).flatMap(o => Option(o.offset)))
    }.toSeq
  }

  private def getCurrentOffsets(partitions: Seq[TopicPartition])(implicit context: ActionPipelineContext) = {
    consumer.endOffsets(partitions.asJava).asScala.map {
      case (topicPartition, offset) => (topicPartition.partition, Option(offset).map(_.toLong))
    }.toSeq
  }

  private def getTopicPartitionsAtTstmp(topicPartitions: Seq[TopicPartition], localDateTime: LocalDateTime)(implicit context: ActionPipelineContext) = {
    val topicPartitionsStart = topicPartitions.map( p => (p, java.lang.Long.valueOf(localDateTime.atZone(datePartitionCol.map(_.zoneId).getOrElse(ZoneId.systemDefault)).toInstant.toEpochMilli))).toMap.asJava
    consumer.offsetsForTimes(topicPartitionsStart).asScala.toSeq.sortBy(_._1.partition)
  }

  private def convertFromKafka(colType: KafkaColumnType, dataCol: Column, subjectType: SubjectType, schema: Option[GenericSchema]): Column = {
    colType match {
      case KafkaColumnType.Binary => dataCol // default is that we get a byte array -> binary from kafka
      case KafkaColumnType.String => dataCol.cast(StringType)
      case KafkaColumnType.Json =>
        // reading is done with the specified schema.
        val sparkSchema = schema.getOrElse(throw new IllegalStateException(s"($id) schema not defined in convertFromKafka"))
          .convert(SparkSubFeed.subFeedType).asInstanceOf[SparkSchema].inner
        from_json(dataCol.cast(StringType), sparkSchema)
      case KafkaColumnType.Avro =>
        import org.apache.spark.sql.avro.functions.from_avro
        // reading is done with the specified schema. It needs to be converted to an avro schema for from_avro.
        val sparkSchema = schema.getOrElse(throw new IllegalStateException(s"($id) schema not defined in convertFromKafka"))
          .convert(SparkSubFeed.subFeedType).asInstanceOf[SparkSchema].inner
        val avroSchema = AvroSchemaConverter.toAvroType(sparkSchema)
        from_avro(dataCol, avroSchema.toString)
      case KafkaColumnType.JsonSchemaRegistry | KafkaColumnType.AvroSchemaRegistry =>
        subjectType match {
          case SubjectType.key => keyConfluentConnector.get.from_confluent(dataCol, topicName, subjectType)
          case SubjectType.value => valueConfluentConnector.get.from_confluent(dataCol, topicName, subjectType)
        }
    }
  }

  private def convertToKafka(colType: KafkaColumnType, dataCol: Column, subjectType: SubjectType, schema: Option[GenericSchema], eagerCheck: Boolean = false): Column = {
    colType match {
      case KafkaColumnType.Binary => dataCol // we let spark/kafka convert the column to binary
      case KafkaColumnType.String => dataCol.cast(StringType)
      case KafkaColumnType.Json => to_json(dataCol)
      case KafkaColumnType.Avro =>
        import org.apache.spark.sql.avro.functions.to_avro
        // writing is done with the specified schema. It needs to be converted to an avro schema for to_avro.
        val sparkSchema = schema.getOrElse(throw new IllegalStateException(s"($id) schema not defined in convertFromKafka")).convert(SparkSubFeed.subFeedType).asInstanceOf[SparkSchema].inner
        val avroSchema = AvroSchemaConverter.toAvroType(sparkSchema)
        to_avro(dataCol, avroSchema.toString)
      case KafkaColumnType.JsonSchemaRegistry | KafkaColumnType.AvroSchemaRegistry =>
        subjectType match {
          case SubjectType.key => keyConfluentConnector.get.to_confluent(dataCol, topicName, subjectType, eagerCheck = eagerCheck, updateAllowed = allowSchemaEvolution)
          case SubjectType.value => valueConfluentConnector.get.to_confluent(dataCol, topicName, subjectType, eagerCheck = eagerCheck, updateAllowed = allowSchemaEvolution)
        }
    }
  }

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    require(datePartitionCol.isDefined, s"($id) datePartitionCol column must be defined for listing partition values")
    val maxEmptyConsecutive: Int = 10 // number of empty partitions to stop searching for partitions
    val pctChronoUnitWaitToComplete = 0.02 // percentage of one chrono unit to wait after partition end date until the partition is assumed to be complete. This is to handle kafka late data.
    val partitions = consumer.partitionsFor(topicName)
    require(partitions!=null, s"($id) topic $topicName doesn't exist")
    logger.debug(s"($id) got kafka partitions ${partitions.asScala.map(_.partition)} for topic $topicName")
    val topicPartitions = partitions.asScala.map( p => new TopicPartition(topicName, p.partition))
    // determine last completed partition - we need to wait some time after considering a partition to be complete because of late data
    val currentPartitionStartTime = datePartitionCol.get.current
    val minDurationWaitToComplete = Duration.ofMillis((datePartitionCol.get.chronoUnit.getDuration.toMillis * pctChronoUnitWaitToComplete).toLong)
    val lastCompletedPartitionStartTime = if (datePartitionCol.get.includeCurrentPartition) {
      currentPartitionStartTime
    } else if (currentPartitionStartTime.isBefore(LocalDateTime.now().minus(minDurationWaitToComplete))) {
      datePartitionCol.get.previous(currentPartitionStartTime)
    } else {
      datePartitionCol.get.previous(currentPartitionStartTime, 2)
    }

    // search how many partitions / chrono units back of data we have
    var cntEmptyConsecutive = 0
    val detectedPartitions = Stream.from(0).map {
      unitsBack =>
        val startTimeIncl = datePartitionCol.get.previous(lastCompletedPartitionStartTime, unitsBack)
        val endTimeExcl = datePartitionCol.get.next(startTimeIncl)
        val topicPartitionsStartRaw = getTopicPartitionsAtTstmp(topicPartitions, startTimeIncl)
        val topicPartitionsStart = topicPartitionsStartRaw.map{ case (topicPartition, start) => (topicPartition, Option(start).map(_.timestamp))}
        val minStartTime = topicPartitionsStart.flatMap(_._2).sorted.headOption
        val isEmpty = minStartTime.isEmpty || minStartTime.exists(_ >= endTimeExcl.atZone(datePartitionCol.get.zoneId).toInstant.toEpochMilli)
        (startTimeIncl, isEmpty, minStartTime)
    }.takeWhile {
      case (startTimeIncl, isEmpty, minStartTime) =>
        cntEmptyConsecutive = if (isEmpty) cntEmptyConsecutive + 1
        else 0
        (cntEmptyConsecutive <= maxEmptyConsecutive)
    }.toVector
    logger.debug(s"($id) detected completed date partitions $detectedPartitions for topic $topicName")

    // convert to partition values
    detectedPartitions.reverse.dropWhile(_._2).map(_._1)
      .map( startTime => PartitionValues(Map(datePartitionCol.get.colName -> datePartitionCol.get.format(startTime))))
  }

  override def createReadSchema(writeSchema: GenericSchema)(implicit context: ActionPipelineContext): GenericSchema = {
    writeSchema match {
      case sparkWriteSchema: SparkSchema =>
        implicit val session: SparkSession = context.sparkSession
        // add additional columns created by kafka source
        val readSchemaRaw = sparkWriteSchema.inner
          .add("topic", StringType)
          .add("partition", IntegerType)
          .add("offset", LongType)
          .add("timestamp", TimestampType)
          .add("timestampType", IntegerType)
        // apply selected columns and return schema
        SparkSchema(convertToReadDataFrame(DataFrameUtil.getEmptyDataFrame(readSchemaRaw)).schema)
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${writeSchema.subFeedType.typeSymbol.name} in method createReadSchema")
    }
  }

  // Store incremental output state. This is a list of partitionNb and corresponding offset.
  private var incrementalOutputState: Option[Seq[(Int,Option[Long])]] = None

  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    incrementalOutputState = state
      // parse offsets from String
      .map(s => s.split(',').map(TopicPartitionOffsets.parseOffsetForSpark).toSeq)
      // else prepare for first increment
      .orElse(Some(consumer.partitionsFor(topicName).asScala.map(p => (p.partition, None))))
  }

  override def getState: Option[String] = {
    incrementalOutputState.map(_.map { case (partition, offset) =>
      TopicPartitionOffsets.getOffsetForSpark(partition, offset, TopicPartitionOffsets.defaultOffsetEarliest)
    }.mkString(","))
  }

  /**
   * Enable kafka incremental mode, e.g. storing state via Kafka Consumer as comitted offsets.
   * This is controlled by execution mode KafkaStateIncrementalMode.
   *
   * TODO: this method and the two variables can be removed once execution mode result options are passed through the Action to the DataObject.
   */
  private[workflow] def enableKafkaStateIncrementalMode(delayedMaxTimestamp: Option[Timestamp] = None): Unit = {
    assert(options.isDefinedAt("groupIdPrefix"), s"($id) option groupIdPrefix must be set for KafkaTopicDataObject in order to use KafkaStateIncrementalMode. groupIdPrefix is used as prefix for kafka consumer group identifiers.")
    _kafkaStateIncrementalModeEnabled = true
    _delayedMaxTimestamp = delayedMaxTimestamp
  }
  private var _kafkaStateIncrementalModeEnabled = false
  private var _delayedMaxTimestamp: Option[Timestamp] = None

  /**
   * Commits incremental output state current offsets to Kafka for execution mode KafkaStateIncrementalMode.
   * Incremental output state is set by getSparkDataFrame.
   */
  private[workflow] def commitIncrementalOutputState(implicit context: ActionPipelineContext): Unit = {
    assert(incrementalOutputState.nonEmpty, s"($id) commitIncrementalOutputState called but incrementalOutputState is not defined")
    assert(_kafkaStateIncrementalModeEnabled, s"($id) commitIncrementalOutputState called but enableKafkaStateIncrementalMode is not enabled")
    val offsetsToCommit = incrementalOutputState.get
      .filter(_._2.nonEmpty) // dont commit empty offset
      .map {
        case (partition, offset) => (new TopicPartition(topicName, partition), new OffsetAndMetadata(offset.get))
      }
    consumer.commitSync(offsetsToCommit.toMap.asJava)
    logger.info(s"($id) committed offsets: ${incrementalOutputState.get.mkString(",")}")
  }

  override def factory: FromConfigFactory[DataObject] = KafkaTopicDataObject
}

object KafkaColumnType extends Enumeration {
  type KafkaColumnType = Value
  // TODO: implement fixed AvroSchema
  val AvroSchemaRegistry, JsonSchemaRegistry, Avro, Json, Binary, String = Value
}

/**
 * Offsets to process per topic partition
 *
 * Note: endOffset is exclusive
 */
private case class TopicPartitionOffsets(topicPartition: TopicPartition, startOffset: Option[Long], endOffset: Option[Long]) {

  /**
   * Splits this TopicPartitionOffsets instance into multiple instances given the maximum offsets per task
   *
   * Note: implementation is recursive
   */
  def split(maxOffsets: Int): Seq[TopicPartitionOffsets] = {
    if (startOffset.isDefined && endOffset.isDefined) {
      if (startOffset.get + maxOffsets < endOffset.get) {
        Seq(TopicPartitionOffsets(topicPartition, startOffset, startOffset.map(_ + maxOffsets))) ++ TopicPartitionOffsets(topicPartition, startOffset.map(_ + maxOffsets), endOffset).split(maxOffsets)
      } else Seq(this)
    } else Seq(this)
  }

  def getEmptyEndEntries(size: Int): Seq[TopicPartitionOffsets] = {
    Seq.fill(size)(this.copy(startOffset = endOffset))
  }

  def getStartOffsetForSpark: String = getOffsetForSpark(topicPartition.partition, startOffset, TopicPartitionOffsets.defaultOffsetEarliest)
  def getEndOffsetForSpark: String = getOffsetForSpark(topicPartition.partition, endOffset, TopicPartitionOffsets.defaultOffsetLatest)
}
private object TopicPartitionOffsets {
  // default offset definitions according to spark
  val defaultOffsetEarliest: Int = -2
  val defaultOffsetLatest: Int = -1

  /**
   * Create string to use as starting/endingOffset option for Spark Kafka data source
   *
   * Output format: `"<partitionNb:integer>":<offset:long>``
   */
  def getOffsetForSpark(partition: Int, offset: Option[Long], defaultOffset: Int): String = {
    // if partition is empty we get no offset, but we have to define one for spark. Define defaultOffset for this.
    s""""$partition":${offset.getOrElse(defaultOffset)}"""
  }

  /**
   * Parse starting/endingOffset used for Spark Kakfa data source from string
   *
   * Expected format: `"<partitionNb:integer>":<offset:long>`
   */
  def parseOffsetForSpark(offsetStr: String): (Int,Option[Long]) = {
    val offsetRegex = "\"([0-9]*)\":(-?[0-9]*)".r.anchored
    val (partition, offset) = offsetStr match {
      case offsetRegex(partitionStr, offsetStr) => (partitionStr.toInt, offsetStr.toLong)
      case _ => throw new IllegalStateException(s"OffsetsForSpark '$offsetStr' does not match regex pattern '${offsetRegex.pattern.toString}'")
    }
    val offsetOption = if (offset >= 0) Some(offset) else None
    (partition, offsetOption)
  }

  def fromOffsets(topic: String, startingOffsets: Seq[(Int,Option[Long])], endingOffsets: Seq[(Int,Option[Long])]): Seq[TopicPartitionOffsets] = {
    assert(startingOffsets.size == endingOffsets.size)
    val endingOffsetsLkp = endingOffsets.toMap
    startingOffsets.map(s => TopicPartitionOffsets(new TopicPartition(topic, s._1), s._2, endingOffsetsLkp(s._1)))
  }
}

object KafkaTopicDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): KafkaTopicDataObject = {
    extract[KafkaTopicDataObject](config)
  }
  final val delayedMaxTimestampOption = "delayedMaxTimestamp"
}
