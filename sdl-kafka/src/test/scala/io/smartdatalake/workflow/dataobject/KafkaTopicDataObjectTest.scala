/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.confluent.kafka.serializers.{KafkaJsonDeserializer, KafkaJsonDeserializerConfig, KafkaJsonSerializer}
import io.github.embeddedkafka.schemaregistry.{EmbeddedKafka => EmbeddedKafkaWithSchemaRegistry}
import io.smartdatalake.testutil.KafkaTestUtil
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.util.misc.{SchemaUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.SparkSchemaUtil
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.connection.KafkaConnection
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.confluent.IncompatibleSchemaException
import org.apache.spark.sql.avro.{IncompatibleSchemaException => AvroIncompatibleSchemaException}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.slf4j.Logger

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

class KafkaTopicDataObjectTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfter
  with EmbeddedKafkaWithSchemaRegistry with DataObjectTestSuite with SmartDataLakeLogger
  with Equality {

  private implicit val loggImp: Logger = logger
  import session.implicits._

  private val kafkaConnection = KafkaConnection("kafkaCon1",
    brokers = "localhost:"+KafkaTestUtil.embeddedKafkaConfig.kafkaPort,
    schemaRegistry = Some("http://localhost:" + KafkaTestUtil.embeddedKafkaConfig.schemaRegistryPort)
  )

  override def beforeAll(): Unit = {

    KafkaTestUtil.start()
  }

  test("Can read and write from Kafka") {
    val topic = "readWriteTopic1"
    createCustomTopic(topic, Map(), 1, 1)
    publishStringMessageToKafka(topic, "message")
    assert(consumeFirstStringMessageFrom(topic) == "message", "Whoops - couldn't read message")
  }

  test("DataObject can write and read kafka topic") {
    val topic = "readWriteTopic2"
    createCustomTopic(topic, Map(), 1, 1)
    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafkaReadWrite1", topicName = topic, connectionId = "kafkaCon1")
    val df = Seq(("john doe", "5"), ("peter smith", "3"), ("emma brown", "7")).toDF("key", "value")
    dataObject.writeSparkDataFrame(df)
    val dfRead = dataObject.getSparkDataFrame(Seq())
    assert(dfRead.getSymmetricDifference(df).isEmpty)
  }

  test("DataObject can write and stream once kafka topic") {
    val topic1 = "readWriteTopicOnce1"
    val topic2 = "readWriteTopicOnce2"
    val tempDir = Files.createTempDirectory("streamTest")
    createCustomTopic(topic1, Map(), 1, 1)
    createCustomTopic(topic2, Map(), 1, 1)
    instanceRegistry.register(kafkaConnection)
    val dataObject1 = KafkaTopicDataObject("kafkaReadWriteOnce1", topicName = topic1, connectionId = "kafkaCon1")
    val dataObject2 = KafkaTopicDataObject("kafkaReadWriteOnce2", topicName = topic2, connectionId = "kafkaCon1")

    // prepare data
    val df1 = Seq(("john doe", "5"), ("peter smith", "3"), ("emma brown", "7")).toDF("key", "value")
    dataObject1.writeSparkDataFrame(df1, Seq())

    // stream
    val dfStream1 = dataObject1.getStreamingDataFrame(Map("startingOffsets" -> "earliest"), None)
    val query = dataObject2.writeStreamingDataFrame(SparkDataFrame(dfStream1), Trigger.AvailableNow, Map(), checkpointLocation = tempDir.resolve("state").toString, "test")
    query.awaitTermination()
    logger.info(s"streaming query finished, rows processed = ${query.lastProgress.numInputRows}")

    // check
    val df2 = dataObject2.getSparkDataFrame().cache()
    assert(df2.getSymmetricDifference(df1).isEmpty)
  }

  test("Can list and query partitions") {
    val topic1 = "testPartitionTopic1"
    createCustomTopic(topic1, Map(), 1, 1)
    logger.info("topic created")

    // publish several messages with some delay between to have different timestamps
    implicit val stringSerializer: StringSerializer = new StringSerializer
    publishToKafka(topic1, "A", "1")
    Thread.sleep(1000)
    publishToKafka(topic1, "B", "2")
    Thread.sleep(2000)
    publishToKafka(topic1, "C", "3")
    logger.info("3 test messages written")

    // configure DataObject with partition column defined as seconds
    instanceRegistry.register(kafkaConnection)
    val dataObject1 = KafkaTopicDataObject("kafkaTestPartition1", topicName = topic1, connectionId = "kafkaCon1"
      , datePartitionCol = Some(DatePartitionColumnDef(colName = "sec", timeUnit = ChronoUnit.SECONDS.toString, timeFormat = "yyyyMMddHHmmss")))

    // list and check partitions
    val partitions = dataObject1.listPartitions
    assert(partitions.size >= 3) // as we have written messages over a timestamp of 3secs

    // check query first partitions data
    val dfP1 = dataObject1.getSparkDataFrame(Seq(partitions.minBy(p => p("sec").toString.toLong))).cache()
    assert(dfP1.columns.contains("sec"))
    val dataP1 = dfP1.select($"key", $"value").as[(String, String)].collect().toSeq
    assert(dataP1 == Seq(("A", "1")))
  }

  test("Exclude or include current date partition in list partitions") {
    val topic1 = "testPartitionTopic2"
    createCustomTopic(topic1, Map(), 1, 1)
    logger.info("topic created")

    // publish one messages
    implicit val stringSerializer: StringSerializer = new StringSerializer
    publishToKafka(topic1, "A", "1")

    // configure DataObject with partition column defined as day and excluding current partition
    instanceRegistry.register(kafkaConnection)
    val dataObject1 = KafkaTopicDataObject("kafkaTestPartition2", topicName = topic1, connectionId = "kafkaCon1"
      , datePartitionCol = Some(DatePartitionColumnDef(colName = "dt", timeUnit = ChronoUnit.DAYS.toString)))

    // list and check partitions
    val partitions1 = dataObject1.listPartitions
    assert(partitions1.isEmpty) // only current partition holds data, but it is excluded

    // configure DataObject with partition column defined as day and including current partition
    val dataObject2 = KafkaTopicDataObject("kafkaTestPartition22", topicName = topic1, connectionId = "kafkaCon1"
      , datePartitionCol = Some(DatePartitionColumnDef(colName = "dt", timeUnit = ChronoUnit.DAYS.toString, includeCurrentPartition = true)))

    // list and check partitions
    val partitions2 = dataObject2.listPartitions
    assert(partitions2.size == 1) // current partition is included
  }

  test("Can read and write Json from Kafka") {
    val topic = "readWriteJson1"
    createCustomTopic(topic, Map(), 1, 1)

    // write json record using KafkaJsonSerializer
    implicit val jsonSerializer: KafkaJsonSerializer[User] = new KafkaJsonSerializer[User]
    jsonSerializer.configure(new java.util.HashMap[String, String](), false)
    val test = new User
    test.setUserId(1)
    test.setLastName("hello")
    publishToKafka(topic, test)

    // read json record using KafkaJsonDeserializer
    implicit val jsonDeserializer: KafkaJsonDeserializer[User] = new KafkaJsonDeserializer[User]
    val deserializerConfig = new java.util.HashMap[String, Any]
    deserializerConfig.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, classOf[User])
    jsonDeserializer.configure(deserializerConfig, false)
    val t = consumeFirstMessageFrom(topic)
    logger.info("Message read: " + t)
  }

  test("SDLB can parse messages written with KafkaJsonSerializer") {
    val topic = "sdlbReadWriteJson1"
    createCustomTopic(topic, Map(), 1, 1)

    // write json record using KafkaJsonSerializer
    implicit val jsonSerializer: KafkaJsonSerializer[User] = new KafkaJsonSerializer[User]
    jsonSerializer.configure(new java.util.HashMap[String, String](), false)
    val expected = new User
    expected.setUserId(1)
    expected.setLastName("hello")
    publishToKafka(topic, expected)

    // parse json record with spark
    instanceRegistry.register(kafkaConnection)
    val userSchema = SparkSchemaUtil.getSchemaFromJavaBean(classOf[User])
    val dataObject = KafkaTopicDataObject("kafkaReadWriteJson1", topicName = topic, connectionId = "kafkaCon1", valueType = KafkaColumnType.Json, valueSchema = Some(SparkSchema(userSchema)))
    val df = dataObject.getSparkDataFrame()
      .select($"value.*")
    val (actFirstName, actLastName, actUserId) = df.as[(String, String, Long)].head()
    assert(actFirstName == expected.getFirstName && actLastName == expected.getLastName && actUserId == expected.getUserId)
  }

  test("read and write json with schema registry") {
    val topic = "sdlbReadWriteJsonRegistry1"
    logger.info("START "+topic)
    createCustomTopic(topic, Map(), 1, 1)

    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafkaReadWriteJsonRegistry1", topicName = topic, connectionId = "kafkaCon1", valueType = KafkaColumnType.JsonSchemaRegistry)
    val expected = Seq(("hello", 1L))

    // write json message incl. schema
    val dfExp = expected.toDF("txt", "num")
      .select(lit(1).as("key"), struct("*").as("value"))
    dataObject.writeSparkDataFrame(dfExp)

    // read again
    val dfAct = dataObject.getSparkDataFrame()
      .select($"value.*")

    val actual = dfAct.as[(String, Long)].collect()
    assert(actual.toSeq == expected)
    logger.info("END "+topic)
  }

  test("read and write avro with schema registry") {
    val topic = "sdlbReadWriteAvroRegistry1"
    logger.info("START "+topic)
    createCustomTopic("topicAvro", Map(), 1, 1)

    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafkaReadWriteAvroRegistry1", topicName = topic, connectionId = "kafkaCon1", valueType = KafkaColumnType.AvroSchemaRegistry)
    val expected = Seq(("hello", 1L))

    // write json message incl. schema
    val dfExp = expected.toDF("txt", "num")
      .select(lit(1).as("key"), struct("*").as("value"))
    dataObject.writeSparkDataFrame(dfExp)

    // read again
    val dfAct = dataObject.getSparkDataFrame()
      .select($"value.*")

    val actual = dfAct.as[(String, Long)].collect()
    assert(actual.toSeq == expected)
    logger.info("END "+topic)
  }


  test("incremental output mode with schema registry") {
    val topic = "sdlbIncrementalReadWriteAvroRegistry1"
    // create data object
    instanceRegistry.register(kafkaConnection)
    val targetDO = KafkaTopicDataObject("kafkaIncrementalReadWriteAvroRegistry1", topicName = topic, connectionId = "kafkaCon1", valueType = KafkaColumnType.AvroSchemaRegistry)

    // write test data 1
    val df1 = Seq((1, ("A", 1)), (2, ("A", 2)), (3, ("B", 3)), (4, ("B", 4))).toDF("key", "value")
    targetDO.writeSparkDataFrame(df1)

    // test 1
    targetDO.setState(None) // initialize incremental output with empty state
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 4
    val newState1 = targetDO.getState

    // append test data 2
    val df2 = Seq((5, ("B", 5))).toDF("key", "value")
    targetDO.writeSparkDataFrame(df2)

    // test 2
    targetDO.setState(newState1)
    val df2result = targetDO.getSparkDataFrame()(contextExec)
    df2result.count() shouldEqual 1
    val newState2 = targetDO.getState

    // test 3
    targetDO.setState(newState2)
    val df3result = targetDO.getSparkDataFrame()(contextExec)
    df3result.count() shouldEqual 0
    val newState3 = targetDO.getState
    assert(newState3 == newState2)

    targetDO.getSparkDataFrame()(contextInit).count() shouldEqual 5
  }

  test("kafka incremental mode") {
    val topic = "sdlbIncrementalReadWriteString1"
    // create data object
    instanceRegistry.register(kafkaConnection)
    val targetDO = KafkaTopicDataObject("kafkaIncrementalReadWriteString1", topicName = topic, connectionId = "kafkaCon1",
      valueType = KafkaColumnType.String, options = Map("groupIdPrefix" -> "sdlb-testIncMode"))

    // test 0a - read empty topic with delayedMaxTimestamp=now
    targetDO.enableKafkaStateIncrementalMode(Some(Timestamp.from(Instant.now())))
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 0
    targetDO.commitIncrementalOutputState

    // test 0b - read empty topic
    targetDO.enableKafkaStateIncrementalMode()
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 0
    targetDO.commitIncrementalOutputState

    // write test data 1
    val df1 = Seq((1, "A"), (2, "A"), (3, "B"), (4, "B")).toDF("key", "value")
    targetDO.writeSparkDataFrame(df1)

    // test 1 - read first batch
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 4
    targetDO.commitIncrementalOutputState

    // append test data 2
    val df2 = Seq((5, "B")).toDF("key", "value")
    targetDO.writeSparkDataFrame(df2)

    // test 2 - get new data
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 1
    targetDO.commitIncrementalOutputState

    // test 3 - no data
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 0
    targetDO.commitIncrementalOutputState

    // save current time to test delayedMaxTimestamp feature
    val tstmpBeforeData3 = Timestamp.from(Instant.now())

    targetDO.writeSparkDataFrame(df2)

    // test 4 - no new data with delayedMaxTimestamp=tstmpBeforeData3
    targetDO.enableKafkaStateIncrementalMode(Some(tstmpBeforeData3))
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 0
    targetDO.commitIncrementalOutputState

    // test 5 - new data with delayedMaxTimestamp=now
    targetDO.enableKafkaStateIncrementalMode(Some(Timestamp.from(Instant.now())))
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 1
    targetDO.commitIncrementalOutputState

    // test 4 - no new data without delayedMaxTimestamp
    targetDO.enableKafkaStateIncrementalMode()
    targetDO.getSparkDataFrame()(contextExec).count() shouldEqual 0

    // all data without incremental mode
    targetDO.getSparkDataFrame()(contextInit).count() shouldEqual 6
  }

  test("json schema evolution") {
    val topic = "sdlbJsonEvolutionRegistry1"
    // create data object
    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafkaJsonEvolutionRegistry1", topicName = topic, connectionId = "kafkaCon1", valueType = KafkaColumnType.JsonSchemaRegistry)
    val dataObjectAllowSchemaEvo = dataObject.copy(allowSchemaEvolution = true)

    // write json message incl. schema
    val dfExp = Seq(("hello", 1L)).toDF("txt", "num")
      .select(lit(1).as("key"), struct("*").as("value"))
    dataObject.writeSparkDataFrame(dfExp)

    // prepare data with updated schema (new column)
    val dfExp1 = Seq(("hello", 1L, "test")).toDF("txt", "num", "test")
      .select(lit(1).as("key"), struct("*").as("value"))

    // check schema evolution disabled
    intercept[IncompatibleSchemaException](dataObject.initSparkDataFrame(dfExp1, Seq()))
    intercept[IncompatibleSchemaException](dataObject.writeSparkDataFrame(dfExp1))

    dataObjectAllowSchemaEvo.initSparkDataFrame(dfExp1, Seq())
    dataObjectAllowSchemaEvo.writeSparkDataFrame(dfExp1)

    val dfResult = dataObjectAllowSchemaEvo.getSparkDataFrame().select($"value.*")
    assert(dfResult.columns.toSeq == Seq("txt", "num", "test"))
  }

  test("avro schema evolution") {
    val topic = "sdlbAvroEvolutionRegistry1"
    // create data object
    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafkaAvroEvolutionRegistry1", topicName = topic, connectionId = "kafkaCon1", valueType = KafkaColumnType.AvroSchemaRegistry)
    val dataObjectAllowSchemaEvo = dataObject.copy(allowSchemaEvolution = true)

    // write json message incl. schema
    val dfExp = Seq(("hello", 1L)).toDF("txt", "num")
      .select(lit(1).as("key"), struct("*").as("value"))
    dataObject.writeSparkDataFrame(dfExp)

    // prepare data with updated schema (new nullable column)
    val dfExp1 = Seq(("hello", 1L, Some("test"))).toDF("txt", "num", "test")
      .select(lit(1).as("key"), struct("*").as("value"))

    // check schema evolution disabled
    // avro.IncompatibleSchemaException is private... so we cant use original intercept method here!!
    interceptWithCheck(() => dataObject.initSparkDataFrame(dfExp1, Seq()), _.getClass.getSimpleName == "IncompatibleSchemaException")
    interceptWithCheck(() => dataObject.writeSparkDataFrame(dfExp1), _.getClass.getSimpleName == "IncompatibleSchemaException")

    dataObjectAllowSchemaEvo.initSparkDataFrame(dfExp1, Seq())
    dataObjectAllowSchemaEvo.writeSparkDataFrame(dfExp1)

    assert(dataObjectAllowSchemaEvo.getSparkDataFrame().select($"value.*").columns.toSeq == Seq("txt", "num", "test"))
  }

  def interceptWithCheck(func: () => Unit, check: Exception => Boolean): Unit = {
    try {
      func()
      throw new IllegalStateException(s"interceptWithCheck: no exception thrown")
    } catch {
      case ex: Exception =>  assert(check(ex), s"interceptWithCheck: Unexpected exception '${ex.getClass.getSimpleName}: ${ex.getMessage}' thrown")
    }

  }
}
