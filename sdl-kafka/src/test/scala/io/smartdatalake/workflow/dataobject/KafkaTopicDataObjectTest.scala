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

import io.confluent.kafka.serializers.{KafkaJsonDeserializer, KafkaJsonDeserializerConfig, KafkaJsonSerializer}
import io.github.embeddedkafka.schemaregistry.{EmbeddedKafka => EmbeddedKafkaWithSchemaRegistry}
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.util.misc.{SchemaUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.connection.KafkaConnection
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import java.nio.file.Files
import java.time.temporal.ChronoUnit


/**
 * Note about EmbeddedKafka compatibility:
 * The currently used version 2.4.1 (in sync with the kafka version of sparks parent pom) is not compatible with JDK14+
 * because of a change of InetSocketAddress::toString. Zookeeper doesn't start because of
 * "java.nio.channels.UnresolvedAddressException: Session 0x0 for server localhost/<unresolved>:6001, unexpected error, closing socket connection and attempting reconnect"
 * see also https://www.oracle.com/java/technologies/javase/14all-relnotes.html#JDK-8225499
 */
class KafkaTopicDataObjectTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with EmbeddedKafkaWithSchemaRegistry with DataObjectTestSuite with SmartDataLakeLogger {

  import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
  import session.implicits._

  private val kafkaConnection = KafkaConnection("kafkaCon1", brokers = "localhost:6001", schemaRegistry = Some("http://localhost:6002"))

  private lazy val kafka = {
    EmbeddedKafkaWithSchemaRegistry.start()
  }

  override def beforeAll() {
    kafka // initialize lazy variable
    Thread.sleep(1000)
  }

  override def afterAll(): Unit = {
    kafka.stop(true)
  }

  test("Can read and write from Kafka") {
    createCustomTopic("topic", Map(), 1, 1)
    publishStringMessageToKafka("topic", "message")
    assert(consumeFirstStringMessageFrom("topic")=="message", "Whoops - couldn't read message")
  }

  test("DataObject can write and read kafka topic") {
    val topic = "testTopic"
    createCustomTopic(topic, Map(), 1, 1)
    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafka1", topicName = topic, connectionId = "kafkaCon1")
    val df = Seq(("john doe", "5"), ("peter smith", "3"), ("emma brown", "7")).toDF("key", "value")
    dataObject.writeSparkDataFrame(df, Seq())
    val dfRead = dataObject.getSparkDataFrame(Seq())
    assert(dfRead.symmetricDifference(df).isEmpty)
  }

  test("DataObject can write and stream once kafka topic") {
    val topic1 = "testTopic1"
    val topic2 = "testTopic2"
    val tempDir = Files.createTempDirectory("streamTest")
    createCustomTopic(topic1, Map(), 1, 1)
    createCustomTopic(topic2, Map(), 1, 1)
    instanceRegistry.register(kafkaConnection)
    val dataObject1 = KafkaTopicDataObject("kafka1", topicName = topic1, connectionId = "kafkaCon1")
    val dataObject2 = KafkaTopicDataObject("kafka1", topicName = topic2, connectionId = "kafkaCon1")

    // prepare data
    val df1 = Seq(("john doe", "5"), ("peter smith", "3"), ("emma brown", "7")).toDF("key", "value")
    dataObject1.writeSparkDataFrame(df1, Seq())

    // stream
    val dfStream1 = dataObject1.getStreamingDataFrame(Map("startingOffsets"->"earliest"), None)
    val query = dataObject2.writeStreamingDataFrame(SparkDataFrame(dfStream1), Trigger.Once, Map(), checkpointLocation = tempDir.resolve("state").toString, "test")
    query.awaitTermination()
    logger.info(s"streaming query finished, rows processed = ${query.lastProgress.numInputRows}")

    // check
    val df2 = dataObject2.getSparkDataFrame().cache
    assert(df2.symmetricDifference(df1).isEmpty)
  }

  test("Can list and query partitions") {
    val topic1 = "testPartitionTopic1"
    createCustomTopic(topic1, Map(), 1, 1)
    logger.info("topic created")

    // publish several messages with some delay between to have different timestamps
    implicit val stringSerializer = new StringSerializer
    publishToKafka(topic1, "A", "1")
    Thread.sleep(1000)
    publishToKafka(topic1, "B", "2")
    Thread.sleep(2000)
    publishToKafka(topic1, "C", "3")
    logger.info("3 test messages written")

    // configure DataObject with partition column defined as seconds
    instanceRegistry.register(kafkaConnection)
    val dataObject1 = KafkaTopicDataObject("kafka1", topicName = topic1, connectionId = "kafkaCon1"
      , datePartitionCol = Some(DatePartitionColumnDef( colName = "sec", timeUnit = ChronoUnit.SECONDS.toString, timeFormat ="yyyyMMddHHmmss")))

    // list and check partitions
    val partitions = dataObject1.listPartitions
    assert(partitions.size >= 3) // as we have written messages over a timestamp of 3secs

    // check query first partitions data
    val dfP1 = dataObject1.getSparkDataFrame(Seq(partitions.minBy( p => p("sec").toString.toLong))).cache
    assert(dfP1.columns.contains("sec"))
    val dataP1 = dfP1.select($"key",$"value").as[(String,String)].collect.toSeq
    assert(dataP1 == Seq(("A","1")))
  }

  test("Exclude or include current date partition in list partitions") {
    val topic1 = "testPartitionTopic2"
    createCustomTopic(topic1, Map(), 1, 1)
    logger.info("topic created")

    // publish one messages
    implicit val stringSerializer = new StringSerializer
    publishToKafka(topic1, "A", "1")

    // configure DataObject with partition column defined as day and excluding current partition
    instanceRegistry.register(kafkaConnection)
    val dataObject1 = KafkaTopicDataObject("kafka1", topicName = topic1, connectionId = "kafkaCon1"
      , datePartitionCol = Some(DatePartitionColumnDef( colName = "dt", timeUnit = ChronoUnit.DAYS.toString, timeFormat ="yyyyMMdd")))

    // list and check partitions
    val partitions1 = dataObject1.listPartitions
    assert(partitions1.isEmpty) // only current partition holds data, but it is excluded

    // configure DataObject with partition column defined as day and including current partition
    val dataObject2 = KafkaTopicDataObject("kafka2", topicName = topic1, connectionId = "kafkaCon1"
      , datePartitionCol = Some(DatePartitionColumnDef( colName = "dt", timeUnit = ChronoUnit.DAYS.toString, timeFormat ="yyyyMMdd", includeCurrentPartition = true)))

    // list and check partitions
    val partitions2 = dataObject2.listPartitions
    assert(partitions2.size == 1) // current partition is included
  }

  test("Can read and write Json from Kafka") {
    createCustomTopic("topic", Map(), 1, 1)

    // write json record using KafkaJsonSerializer
    implicit val jsonSerializer = new KafkaJsonSerializer[User]
    jsonSerializer.configure(new java.util.HashMap[String,String](), false)
    val test = new User
    test.setUserId(1)
    test.setLastName("hello")
    publishToKafka("topic", test)

    // read json record using KafkaJsonDeserializer
    implicit val jsonDeserializer = new KafkaJsonDeserializer[User]
    val deserializerConfig = new java.util.HashMap[String,Any]
    deserializerConfig.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, classOf[User])
    jsonDeserializer.configure(deserializerConfig, false)
    val t = consumeFirstMessageFrom("topic")
    logger.info("Message read: " + t)
  }

  test("SDL can parse messages written with KafkaJsonSerializer") {
    createCustomTopic("topicJsonRead", Map(), 1, 1)

    // write json record using KafkaJsonSerializer
    implicit val jsonSerializer: KafkaJsonSerializer[User] = new KafkaJsonSerializer[User]
    jsonSerializer.configure(new java.util.HashMap[String,String](), false)
    val expected = new User
    expected.setUserId(1)
    expected.setLastName("hello")
    publishToKafka("topicJsonRead", expected)

    // parse json record with spark
    instanceRegistry.register(kafkaConnection)
    val userSchema = SchemaUtil.getSchemaFromJavaBean(classOf[User])
    val dataObject = KafkaTopicDataObject("kafka1", topicName = "topicJsonRead", connectionId = "kafkaCon1", valueType = KafkaColumnType.Json, valueSchema = Some(SparkSchema(userSchema)))
    val df = dataObject.getSparkDataFrame()
      .select($"value.*")
    val (actFirstName, actLastName, actUserId) = df.as[(String,String,Long)].head
    assert(actFirstName == expected.getFirstName && actLastName == expected.getLastName && actUserId == expected.getUserId)
  }

  test("read and write json with schema registry") {
    createCustomTopic("topicJson", Map(), 1, 1)

    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafka1", topicName = "topicJson", connectionId = "kafkaCon1", valueType = KafkaColumnType.JsonSchemaRegistry)
    val expected = Seq(("hello", 1L))

    // write json message incl. schema
    val dfExp = expected.toDF("txt","num")
      .select(lit(1).as("key"), struct("*").as("value"))
    dataObject.writeSparkDataFrame(dfExp)

    // read again
    val dfAct = dataObject.getSparkDataFrame()
      .select($"value.*")

    val actual = dfAct.as[(String,Long)].collect
    assert(actual.toSeq == expected)
  }

  test("read and write avro with schema registry") {
    createCustomTopic("topicAvro", Map(), 1, 1)

    instanceRegistry.register(kafkaConnection)
    val dataObject = KafkaTopicDataObject("kafka1", topicName = "topicAvro", connectionId = "kafkaCon1", valueType = KafkaColumnType.AvroSchemaRegistry)
    val expected = Seq(("hello", 1L))

    // write json message incl. schema
    val dfExp = expected.toDF("txt","num")
      .select(lit(1).as("key"), struct("*").as("value"))
    dataObject.writeSparkDataFrame(dfExp)

    // read again
    val dfAct = dataObject.getSparkDataFrame()
      .select($"value.*")

    val actual = dfAct.as[(String,Long)].collect
    assert(actual.toSeq == expected)
  }
}
