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

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAccessor, TemporalQuery}
import java.time._
import java.util.Properties

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.connection.KafkaConnection
import io.smartdatalake.workflow.dataobject.KafkaColumnType.KafkaColumnType
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import za.co.absa.abris.avro.functions.{from_confluent_avro, to_confluent_avro}
import za.co.absa.abris.avro.read.confluent.SchemaManager

import scala.collection.JavaConverters._

/**
 * Definition of date partition column to extract formatted timestamp into column.
 *
 * @param colName date partition column name to extract timestamp into column on batch read
 * @param timeFormat time format for timestamp in date partition column, definition according to java DateTimeFormatter. Default is "yyyyMMdd".
 * @param timeUnit time unit for timestamp in date partition column, definition according to java ChronoUnit. Default is "days".
 * @param timeZone time zone used for date logic. If not specified, java system default is used.
 */
case class DatePartitionColumnDef(colName: String, timeFormat: String = "yyyyMMdd", timeUnit: String = "days", timeZone: Option[String] = None ) {
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
  val LocalDateTimeQuery: TemporalQuery[LocalDateTime] = new TemporalQuery[LocalDateTime] {
    override def queryFrom(temporal: TemporalAccessor): LocalDateTime = LocalDateTime.from(temporal)
  }
  val LocalDateQuery: TemporalQuery[LocalDate] = new TemporalQuery[LocalDate] {
    override def queryFrom(temporal: TemporalAccessor): LocalDate = LocalDate.from(temporal)
  }
  val LocalYearMonthQuery: TemporalQuery[YearMonth] = new TemporalQuery[YearMonth] {
    override def queryFrom(temporal: TemporalAccessor): YearMonth = YearMonth.from(temporal)
  }
}

/**
 * [[DataObject]] of type KafkaTopic.
 * Provides details to an action to read from Kafka Topics using either
  * [[org.apache.spark.sql.DataFrameReader]] or [[org.apache.spark.sql.streaming.DataStreamReader]]
  *
  * @param topicName The name of the topic to read
  * @param keyType    Optional type the key column should be converted to. If none is given it will remain a bytearray / binary.
  * @param valueType  Optional type the value column should be converted to. If none is given it will remain a bytearray / binary.
  * @param schemaMin  An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
  * @param selectCols Columns to be selected when reading the DataFrame. Available columns are key, value, topic,
  *                   partition, offset, timestamp timestampType. If key/valueType is AvroSchemaRegistry the key/value column are
  *                   convert to a complex type according to the avro schema. To expand it select "value.*".
  *                   Default is to select key and value.
  * @param datePartitionCol definition of date partition column to extract formatted timestamp into column.
  *                   This is used to list existing partition and is added as additional column on batch read.
  * @param batchReadConsecutivePartitionsAsRanges Set to true if consecutive partitions should be combined as one range of offsets when batch reading from topic. This results in less tasks but can be a performance problem when reading many partitions. (default=false)
  * @param batchReadMaxOffsetsPerTask Set number of offsets per Spark task when batch reading from topic.
  * @param dataSourceOptions Options for the Kafka stream reader (see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).
  *                      These options override connection.kafkaOptions.
  */
case class KafkaTopicDataObject(override val id: DataObjectId,
                                topicName: String,
                                connectionId: ConnectionId,
                                keyType: KafkaColumnType = KafkaColumnType.String,
                                valueType: KafkaColumnType = KafkaColumnType.String,
                                override val schemaMin: Option[StructType] = None,
                                selectCols: Seq[String] = Seq("key", "value"),
                                datePartitionCol: Option[DatePartitionColumnDef] = None,
                                batchReadConsecutivePartitionsAsRanges: Boolean = false,
                                batchReadMaxOffsetsPerTask: Option[Int] = None,
                                dataSourceOptions: Map[String, String] = Map(),
                                override val metadata: Option[DataObjectMetadata] = None
                           )(implicit instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanCreateStreamingDataFrame with CanWriteDataFrame with CanHandlePartitions with SchemaValidation {

  override val partitions: Seq[String] = datePartitionCol.map(_.colName).toSeq
  private val udfFormatPartition = udf((ts:Timestamp) => ts.toLocalDateTime.truncatedTo(datePartitionCol.get.chronoUnit).format(datePartitionCol.get.formatter))

  private val connection = getConnection[KafkaConnection](connectionId)

  require((keyType!=KafkaColumnType.AvroSchemaRegistry && valueType!=KafkaColumnType.AvroSchemaRegistry) || connection.schemaRegistry.nonEmpty, s"($id) If key or value is of type AvroSchemaRegistry, the schemaRegistry must be defined in the connection")
  require(batchReadMaxOffsetsPerTask.isEmpty || batchReadMaxOffsetsPerTask.exists(_>0), s"($id) batchReadMaxOffsetsPerTask must be greater than 0")

  private val instanceOptions = connection.sparkOptions ++ dataSourceOptions

  private val schemaRegistryConfig = connection.schemaRegistry.map (
    schemaRegistry => Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistry,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topicName,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
    )
  )

  // consumer for reading topic metadata
  @transient private lazy val consumer = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.brokers)
    // consumer is only used for reading topic metadata; auto commit is never needed and de/serializers are not relevant
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.putAll(connection.authProps)
    new KafkaConsumer(props)
  }

  override def prepare(implicit session: SparkSession): Unit = {
    // test kafka connection
    require(connection.topicExists(topicName), s"($id) topic $topicName doesn't exist")
    // test schema registry connection
    if (schemaRegistryConfig.isDefined) {
      SchemaManager.configureSchemaRegistry(schemaRegistryConfig.get)
      SchemaManager.exists("dummy") // this is just a dummy request to check connection
    }
  }

  override def getStreamingDataFrame(options: Map[String,String], schema: Option[StructType])(implicit session: SparkSession): DataFrame = {
    val dfRaw = session
      .readStream
      .format("kafka")
      .options(instanceOptions ++ options) // options override kafkaOptions override connection.kafkaOptions
      .option("subscribe", topicName)
      .load()
    prepareDataFrame(dfRaw)
  }

  private def prepareDataFrame(dfRaw: DataFrame): DataFrame = {
    import io.smartdatalake.util.misc.DataFrameUtil._

    // Deserialize key/value to make human readable
    // convert key & value
    val colsToSelect = ((if (selectCols.nonEmpty) selectCols else Seq("kafka.*")) ++ partitions).distinct.map(col)
    val df = dfRaw
      .withColumn("key", convertFromKafka(keyType, col("key")))
      .withColumn("value", convertFromKafka(valueType, col("value")))
      .as("kafka")
      .withOptionalColumn(datePartitionCol.map(_.colName), udfFormatPartition(col("timestamp")))
      .select(colsToSelect:_*)
    validateSchemaMin(df)
    // return
    df
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): DataFrame = {

    // get DataFrame from topic
    val dfRaw = if (partitionValues.nonEmpty) {
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
      // map date ranges to offsets and create data frames
      val dfsRaw = dateRangesCombined.flatMap {
        case (startTimeIncl, endTimeExcl) =>
          val partitions = consumer.partitionsFor(topicName)
          require(partitions!=null, s"($id) topic $topicName doesn't exist")
          val topicPartitions = partitions.asScala.map(p => new TopicPartition(topicName, p.partition))
          val topicPartitionsStart = getTopicPartitionsAtTstmp(topicPartitions, startTimeIncl).toMap
          val topicPartitionsEnd = getTopicPartitionsAtTstmp(topicPartitions, endTimeExcl).toMap
          val topicPartitionOffsetsRaw = topicPartitions.map( tp => TopicPartitionOffsets( tp, Option(topicPartitionsStart(tp)).map(_.offset), Option(topicPartitionsEnd(tp)).map(_.offset)))
          // split offsets according to maxOffsetsPerTask
          val topicPartitionOffsetsSplitted = topicPartitionOffsetsRaw.map( tpo => if (batchReadMaxOffsetsPerTask.isDefined) tpo.split(batchReadMaxOffsetsPerTask.get) else Seq(tpo))
          // ensure that every partition has the same number of tasks by adding empty entries
          val maxNbOfTasksPerPartition = topicPartitionOffsetsSplitted.map(_.size).max
          val topicPartitionOffsetsBalanced = topicPartitionOffsetsSplitted.map( tpos => tpos ++ tpos.last.getEmptyEndEntries(maxNbOfTasksPerPartition-tpos.size))
          // transpose so that we have a list of tasks for every partition per query
          val topicPartitionOffsetsQueries = topicPartitionOffsetsBalanced.transpose
          // create data frames
          topicPartitionOffsetsQueries.zipWithIndex.map {
            case (tpos,idx) =>
              val startingOffsets = tpos.sortBy(_.topicPartition.partition).map(_.getStartOffsetsForSpark).mkString(",")
              val endingOffsets = tpos.sortBy(_.topicPartition.partition).map(_.getEndOffsetsForSpark).mkString(",")
              logger.info(s"($id) creating data frame $idx for time period $startTimeIncl - $endTimeExcl of topic $topicName: startingOffsets=$startingOffsets, endingOffsets=$endingOffsets")
              session
                .read
                .format("kafka")
                .options(instanceOptions)
                .option("subscribe", topicName)
                .option("startingOffsets", s"""{"$topicName":{$startingOffsets}}""")
                .option("endingOffsets", s"""{"$topicName":{$endingOffsets}}""") // endingOffsets are exclusive
                .load()
          }
      }
      def unionDf(df1: DataFrame, df2: DataFrame) = df1.union(df2)
      dfsRaw.reduce(unionDf)
    } else {
      logger.info(s"($id) creating data frame for whole topic $topicName, no partition values given")
      session.read
        .format("kafka")
        .options(instanceOptions)
        .option("subscribe", topicName)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    }

    prepareDataFrame(dfRaw)
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    import session.implicits._
    require(df.columns.toSet == Set("key","value"), s"(${id}) Expects columns Set(key, value) in DataFrame for writing to Kafka. Given: ${df.columns.mkString(", ")}")
    df.select(convertToKafka(keyType,$"key").as("key"), convertToKafka(valueType,$"value").as("value"))
      .write
      .format("kafka")
      .options(instanceOptions)
      .option("topic", topicName)
      .save
  }

  override def writeStreamingDataFrame(df: DataFrame, trigger: Trigger, options: Map[String, String], checkpointLocation: String, queryName: String, outputMode: OutputMode)(implicit session: SparkSession): StreamingQuery = {
    import session.implicits._
    require(df.columns.toSet == Set("key","value"), s"(${id}) Expects columns Set(key, value) in DataFrame for writing to Kafka. Given: ${df.columns.mkString(", ")}")
    df.select(convertToKafka(keyType,$"key").as("key"), convertToKafka(valueType,$"value").as("value"))
      .writeStream
      .format("kafka")
      .trigger(trigger)
      .queryName(queryName)
      .outputMode(outputMode)
      .options(instanceOptions ++ options)
      .option("checkpointLocation", checkpointLocation)
      .option("topic", topicName)
      .start()
  }

  private def getTopicPartitionsAtTstmp(topicPartitions: Seq[TopicPartition], localDateTime: LocalDateTime) = {
    val topicPartitionsStart = topicPartitions.map( p => (p, new java.lang.Long(localDateTime.atZone(datePartitionCol.get.zoneId).toInstant.toEpochMilli))).toMap.asJava
    consumer.offsetsForTimes(topicPartitionsStart).asScala.toSeq.sortBy(_._1.partition)
  }

  private def convertFromKafka(colType: KafkaColumnType, col: Column ): Column = {
    colType match {
      case KafkaColumnType.Binary => col // default is that we get a byte array -> binary from kafka
      case KafkaColumnType.AvroSchemaRegistry => from_confluent_avro(col, schemaRegistryConfig.get)
      case KafkaColumnType.String => col.cast(StringType)
    }
  }

  private def convertToKafka(colType: KafkaColumnType, col: Column ): Column = {
    colType match {
      case KafkaColumnType.Binary => col // we let spark/kafka convert the column to binary
      case KafkaColumnType.AvroSchemaRegistry => to_confluent_avro(col, schemaRegistryConfig.get)
      case KafkaColumnType.String => col.cast(StringType)
    }
  }

  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = {
    require(datePartitionCol.isDefined, s"(${id}) datePartitionCol column must be defined for listing partition values")
    val maxEmptyConsecutive: Int = 10 // number of empty partitions to stop searching for partitions
    val pctChronoUnitWaitToComplete = 0.02 // percentage of one chrono unit to wait after partition end date to wait until the partition is assumed to be complete. This is to handle kafka late data.
    val partitions = consumer.partitionsFor(topicName)
    require(partitions!=null, s"($id) topic $topicName doesn't exist")
    logger.debug(s"($id) got kafka partitions ${partitions.asScala.map(_.partition)} for topic $topicName")
    val topicPartitions = partitions.asScala.map( p => new TopicPartition(topicName, p.partition))
    // determine last completed partition - we need to wait some time after considering a partition to be complete because of late data
    val currentPartitionStartTime = datePartitionCol.get.current
    val minDurationWaitToComplete = Duration.ofMillis((datePartitionCol.get.chronoUnit.getDuration.toMillis * pctChronoUnitWaitToComplete).toLong)
    val lastCompletedPartitionStartTime = if (currentPartitionStartTime.isBefore(LocalDateTime.now().minus(minDurationWaitToComplete))) {
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

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = KafkaTopicDataObject

}

object KafkaColumnType extends Enumeration {
  type KafkaColumnType = Value
  // TODO: implement fixed AvroSchema
  val AvroSchemaRegistry, Binary, String = Value
}

/**
 * Offsets to process per topic partition
 * Note: endOffset is exclusive
 */
private case class TopicPartitionOffsets(topicPartition: TopicPartition, startOffset: Option[Long], endOffset: Option[Long]) {

  // default offset definitions according to spark
  private val defaultOffsetEarliest = -2
  private val defaultOffsetLatest = -1

  /**
   * Splits this TopicPartitionOffsets instance into multiple instances given the maximum offsets per task
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

  /**
   * Create string to use as starting/endingOffsets option for Spark Kafka data source
   */
  def getStartOffsetsForSpark: String = getOffsetsForSpark(topicPartition, startOffset, defaultOffsetEarliest)
  def getEndOffsetsForSpark: String = getOffsetsForSpark(topicPartition, endOffset, defaultOffsetLatest)
  private def getOffsetsForSpark(tp: TopicPartition, ot: Option[Long], defaultOffset: Int): String = {
    // if partition is empty we get no offset, but we have to define one for spark. Define defaultOffset for this.
    s""""${tp.partition}":${ot.getOrElse(defaultOffset)}"""
  }
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
