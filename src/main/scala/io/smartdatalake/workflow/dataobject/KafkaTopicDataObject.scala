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
import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDate, LocalDateTime, ZoneId, ZoneOffset}
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
import za.co.absa.abris.avro.functions.from_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager

import scala.util.Try
import scala.collection.JavaConverters._

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
 *                    Default is to select key and value.
  * @param datePartitionCol partition column name to extract timestamp into on batch read
  * @param datePartitionTimeFormat time format for timestamp in datePartitionCol, definition according to java DateTimeFormatter (e.g. "yyyyMMdd").
  * @param datePartitionTimeUnit time unit for timestamp in datePartitionCol, definition according to java ChronoUnit (e.g. "days").
  * @param batchReadConsecutivePartitionsAsRanges Set to true if consecutive partitions should be combined as one range of offsets when batch reading from topic. This results in less tasks but can be a performance problem when reading many partitions. (default=false)
  * @param batchReadMaxOffsetsPerTask Set number of offsets per Spark task when batch reading from topic.
  * @param kafkaOptions Options for the Kafka stream reader (see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).
 *                      These options override connection.kafkaOptions.
  */
case class KafkaTopicDataObject(override val id: DataObjectId,
                                topicName: String,
                                connectionId: ConnectionId,
                                keyType: KafkaColumnType = KafkaColumnType.String,
                                valueType: KafkaColumnType = KafkaColumnType.String,
                                override val schemaMin: Option[StructType] = None,
                                selectCols: Seq[String] = Seq("key", "value"),
                                datePartitionCol: Option[String] = None,
                                datePartitionTimeFormat: Option[String] = None,
                                datePartitionTimeUnit: Option[String] = None,
                                batchReadConsecutivePartitionsAsRanges: Boolean = false,
                                batchReadMaxOffsetsPerTask: Option[Int] = None,
                                kafkaOptions: Map[String, String] = Map(),
                                override val metadata: Option[DataObjectMetadata] = None
                           )(implicit instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanCreateStreamingDataFrame with CanWriteDataFrame with CanHandlePartitions with SchemaValidation {

  override val partitions: Seq[String] = datePartitionCol.toSeq

  private val connection = getConnection[KafkaConnection](connectionId)

  require(datePartitionCol.isEmpty || datePartitionTimeFormat.nonEmpty, s"($id) If a datePartitionCol column is defined, also a datePartitionTimeFormat must be given to build this column from the kafka topics timestamp")
  require(datePartitionCol.isEmpty || datePartitionTimeUnit.nonEmpty, s"($id) If a datePartitionCol column is defined, also a datePartitionTimeUnit must be given to build this column from the kafka topics timestamp")
  require((keyType!=KafkaColumnType.AvroSchemaRegistry && valueType!=KafkaColumnType.AvroSchemaRegistry) || connection.schemaRegistry.nonEmpty, s"($id) If key or value is of type AvroSchemaRegistry, the schemaRegistry must be defined in the connection")
  require(batchReadMaxOffsetsPerTask.isEmpty || batchReadMaxOffsetsPerTask.exists(_>0), s"($id) batchReadMaxOffsetsPerTask must be greater than 0")

  @transient lazy private val datePartitionFormatter = datePartitionTimeFormat.map(DateTimeFormatter.ofPattern) // not serializable -> transient lazy to use in udf
  private val datePartitionChronoUnit = datePartitionTimeUnit.map( unit => ChronoUnit.valueOf(unit.toUpperCase))
  private val udfFormatPartition = udf((ts:Timestamp) => ts.toLocalDateTime.truncatedTo(datePartitionChronoUnit.get).format(datePartitionFormatter.get))
  private val schemaRegistryConfig = connection.schemaRegistry.map (
    schemaRegistry => Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistry,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topicName,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
    )
  )
  private val zoneUTC = ZoneId.of("UTC")

  @transient private lazy val consumer = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.brokers)
    // consumer is only used for reading topic metadata; auto commit is never needed and de/serializers are not relevant
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
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
      .option("kafka.bootstrap.servers", connection.brokers)
      .options(connection.kafkaOptions ++ kafkaOptions ++ options) // options override kafkaOptions override connection.kafkaOptions
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
      .withColumn("key", convertKafkaType(keyType, col("key")))
      .withColumn("value", convertKafkaType(valueType, col("value")))
      .as("kafka")
      .withOptionalColumn(datePartitionCol, udfFormatPartition(col("timestamp")))
      .select(colsToSelect:_*)
    validateSchemaMin(df)
    // return
    df
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): DataFrame = {
    implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(_.toInstant(ZoneOffset.UTC))

    // get DataFrame from topic
    val dfRaw = if (partitionValues.nonEmpty) {
      // create date for partition values
      val dateRanges = partitionValues.map {
        partitionValue =>
          val startTimeIncl = Try(LocalDateTime.parse(partitionValue(datePartitionCol.get).toString, datePartitionFormatter.get))
            .recover { case ex => LocalDate.parse(partitionValue(datePartitionCol.get).toString, datePartitionFormatter.get).atStartOfDay() }
            .recover { case ex => throw new IllegalStateException(s"($id) Can not parse startTime from partition value $partitionValues with $datePartitionFormatter", ex) }
            .get
          val endTimeExcl = startTimeIncl.plus(1, datePartitionChronoUnit.get)
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
                .options(connection.kafkaOptions ++ kafkaOptions)
                .option("kafka.bootstrap.servers", connection.brokers)
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
        .options(connection.kafkaOptions ++ kafkaOptions)
        .option("kafka.bootstrap.servers", connection.brokers)
        .option("subscribe", topicName)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    }

    prepareDataFrame(dfRaw)
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    validateSchemaMin(df)
    // TODO: implement using avro schema registry
    df.write
      .format("kafka")
      .options(connection.kafkaOptions ++ kafkaOptions)
      .option("kafka.bootstrap.servers", connection.brokers)
      .option("topic", topicName)
      .save
  }

  override def writeStreamingDataFrame(df: DataFrame, trigger: Trigger, options: Map[String, String], checkpointLocation: String, queryName: String, outputMode: OutputMode)(implicit session: SparkSession): StreamingQuery = {
    validateSchemaMin(df)
    // TODO: implement using avro schema registry
    df.writeStream
      .format("kafka")
      .trigger(trigger)
      .queryName(queryName)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation)
      .options(connection.kafkaOptions ++ kafkaOptions ++ options)
      .start()
  }

  private def getTopicPartitionsAtTstmp(topicPartitions: Seq[TopicPartition], localDateTime: LocalDateTime) = {
    val topicPartitionsStart = topicPartitions.map( p => (p, new java.lang.Long(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli))).toMap.asJava
    consumer.offsetsForTimes(topicPartitionsStart).asScala.toSeq.sortBy(_._1.partition)
  }

  private def convertKafkaType(colType: KafkaColumnType, col: Column ): Column = {
    colType match {
      case KafkaColumnType.Binary => col // default is that we get a byte array -> binary from kafka
      case KafkaColumnType.AvroSchemaRegistry => from_confluent_avro(col, schemaRegistryConfig.get)
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
    val currentPartitionStartTime = LocalDateTime.now().truncatedTo(datePartitionChronoUnit.get)
    val minDurationWaitToComplete = Duration.ofMillis((datePartitionChronoUnit.get.getDuration.toMillis * pctChronoUnitWaitToComplete).toLong)
    val lastCompletedPartitionStartTime = if (currentPartitionStartTime.isBefore(LocalDateTime.now().minus(minDurationWaitToComplete))) {
      currentPartitionStartTime.minus(1, datePartitionChronoUnit.get)
    } else {
      currentPartitionStartTime.minus(2, datePartitionChronoUnit.get)
    }

    // search how many partitions / chrono units back of data we have
    var cntEmptyConsecutive = 0
    val detectedPartitions = Stream.from(0).map {
      unitsBack =>
        val startTimeIncl = lastCompletedPartitionStartTime.minus(unitsBack, datePartitionChronoUnit.get)
        val endTimeExcl = startTimeIncl.plus(1, datePartitionChronoUnit.get)
        val topicPartitionsStart = getTopicPartitionsAtTstmp(topicPartitions, startTimeIncl)
          .map{ case (topicPartition, start) => (topicPartition, Option(start).map(_.timestamp))}
        val minStartTime = topicPartitionsStart.flatMap(_._2).sorted.headOption
        val isEmpty = minStartTime.isEmpty || minStartTime.exists(_ >= endTimeExcl.toInstant(ZoneOffset.UTC).toEpochMilli)
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
      .map( startTime => PartitionValues(Map(datePartitionCol.get->startTime.format(datePartitionFormatter.get))))
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
case class TopicPartitionOffsets(topicPartition: TopicPartition, startOffset: Option[Long], endOffset: Option[Long]) {

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
