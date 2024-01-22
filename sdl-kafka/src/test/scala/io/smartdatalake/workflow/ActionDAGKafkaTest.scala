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
package io.smartdatalake.workflow

import io.github.embeddedkafka.EmbeddedKafka
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutil.KafkaTestUtil
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.connection.KafkaConnection
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.executionMode.KafkaStateIncrementalMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import java.nio.file.Files

/**
 * Note about EmbeddedKafka compatibility:
 * The currently used version 2.4.1 (in sync with the kafka version of sparks parent pom) is not compatible with JDK14+
 * because of a change of InetSocketAddress::toString. Zookeeper doesn't start because of
 * "java.nio.channels.UnresolvedAddressException: Session 0x0 for server localhost/<unresolved>:6001, unexpected error, closing socket connection and attempting reconnect"
 * see also https://www.oracle.com/java/technologies/javase/14all-relnotes.html#JDK-8225499
 */
class ActionDAGKafkaTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with EmbeddedKafka with SmartDataLakeLogger {
  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  private val contextPrep = contextInit.copy(phase = ExecutionPhase.Prepare)
  private val contextExec = contextInit.copy(phase = ExecutionPhase.Exec)

  KafkaTestUtil.start()

  before {
    instanceRegistry.clear()
  }

  test("action dag with 2 actions in sequence where 2nd action reads different schema than produced by last action") {
    // Note: Some DataObjects remove & add columns on read (e.g. KafkaTopicDataObject, SparkFileDataObject)
    // In this cases we have to break the lineage und create a dummy DataFrame in init phase.

    // setup DataObjects
    val feed = "actionpipeline"
    val kafkaConnection = KafkaConnection("kafkaCon1", "localhost:6001")
    instanceRegistry.register(kafkaConnection)
    val srcDO = MockDataObject( "src1").register
    createCustomTopic("topic1", Map(), 1, 1)
    val tgt1DO = KafkaTopicDataObject("kafka1", topicName = "topic1", connectionId = "kafkaCon1", valueType = KafkaColumnType.String, selectCols = Seq("value", "timestamp"))
    instanceRegistry.register(tgt1DO)
    createCustomTopic("topic2", Map(), 1, 1)
    val tgt2DO = KafkaTopicDataObject("kafka2", topicName = "topic2", connectionId = "kafkaCon1", valueType = KafkaColumnType.String, selectCols = Seq("value", "timestamp"))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val l1 = Seq(("doe-john", 5)).toDF("key", "value")
    srcDO.writeSparkDataFrame(l1, Seq())
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, transformer = Some(CustomDfTransformerConfig(sqlCode = Some("select 'test' as key, value from kafka1"))))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1, action2))

    // exec dag
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check result
    val dfR1 = tgt2DO.getSparkDataFrame(Seq())
    assert(dfR1.columns.toSet == Set("value","timestamp"))
    val r1 = dfR1
      .select($"value")
      .as[String].collect().toSeq
    assert(r1 == Seq("5"))

    // check metrics
    // note: metrics don't work for Kafka sink in Spark 2.4
    //val action2MainMetrics = action2.getRuntimeMetrics()(action2.outputId).get.getMainInfos
    //assert(action2MainMetrics("records_written") == 1)
  }

  test("action dag with 2 actions in sequence and executionMode=KafkaStateIncrementalMode") {

    // setup DataObjects
    val optionsGroupIdPrefix = Map("groupIdPrefix" -> "sdlb-testDagIncMode")
    val kafkaConnection = KafkaConnection("kafkaCon1", "localhost:6001")
    instanceRegistry.register(kafkaConnection)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int")
    createCustomTopic("topicIncSrc", Map(), 1, 1)
    val srcDO = KafkaTopicDataObject("kafkaSrc", topicName = "topicIncSrc", connectionId = "kafkaCon1", valueType = KafkaColumnType.Json, valueSchema = Some(SparkSchema(schema)), options = optionsGroupIdPrefix)
    instanceRegistry.register(srcDO)
    createCustomTopic("topicInc1", Map(), 1, 1)
    val tgt1DO = KafkaTopicDataObject("kafka1", topicName = "topicInc1", connectionId = "kafkaCon1", valueType = KafkaColumnType.Json, valueSchema = Some(SparkSchema(schema)), options = optionsGroupIdPrefix)
    instanceRegistry.register(tgt1DO)
    createCustomTopic("topicInc2", Map(), 1, 1)
    val tgt2DO = KafkaTopicDataObject("kafka2", topicName = "topicInc2", connectionId = "kafkaCon1", valueType = KafkaColumnType.Json, valueSchema = Some(SparkSchema(schema)))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val df1 = Seq(("r1",TestPerson("doe", "john", 5))).toDF("key", "value")
    srcDO.writeSparkDataFrame(df1, Seq())

    val action1 = CopyAction("a", srcDO.id, tgt1DO.id, executionMode = Some(KafkaStateIncrementalMode()))
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, executionMode = Some(KafkaStateIncrementalMode()))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1, action2))

    // first dag run
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r1 = tgt2DO.getSparkDataFrame()
      .select($"value.rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r1.size == 1)
    assert(r1.head == 5)

    // second dag run - no data to process
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r2 = tgt2DO.getSparkDataFrame()
      .select($"value.rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r2.size == 1)
    assert(r2.head == 5)

    // third dag run - new data to process
    val df2 = Seq(("r2", TestPerson("doe", "john 2", 10))).toDF("key", "value")
    srcDO.writeSparkDataFrame(df2, Seq())
    dag.reset
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    // check
    val r3 = tgt2DO.getSparkDataFrame()
      .select($"value.rating".cast("int"))
      .as[Int].collect().toSeq
    assert(r3.size == 2)

    // check metrics
    // note: metrics don't work for Kafka sink in Spark 2.4
    //val action2MainMetrics = action2.runtimeData.getFinalMetrics(action2.outputId).get.getMainInfos
    //assert(action2MainMetrics("records_written") == 1)
  }

  test("action dag with 1 action, executionMode=KafkaStateIncrementalMode and delayedMaxTimestamp") {

    // setup DataObjects
    val optionsGroupIdPrefix = Map("groupIdPrefix" -> "sdlb-testDagIncMode")
    val kafkaConnection = KafkaConnection("kafkaCon1", "localhost:6001")
    instanceRegistry.register(kafkaConnection)
    val schema = StructType.fromDDL("lastname string, firstname string, rating int")
    createCustomTopic("topicIncDelaySrc", Map(), 1, 1)
    val srcDO = KafkaTopicDataObject("kafkaSrc", topicName = "topicIncDelaySrc", connectionId = "kafkaCon1", valueType = KafkaColumnType.Json, valueSchema = Some(SparkSchema(schema)), options = optionsGroupIdPrefix)
    instanceRegistry.register(srcDO)
    createCustomTopic("topicIncDelay1", Map(), 1, 1)
    val tgt1DO = KafkaTopicDataObject("kafka1", topicName = "topicIncDelay1", connectionId = "kafkaCon1", valueType = KafkaColumnType.Json, valueSchema = Some(SparkSchema(schema)), options = optionsGroupIdPrefix)
    instanceRegistry.register(tgt1DO)

    // prepare DAG
    val df1 = Seq(("r1", TestPerson("doe", "john", 5))).toDF("key", "value")
    logger.info("Prepare data")
    srcDO.writeSparkDataFrame(df1, Seq())

    val delaySecs = 10
    val action1Delay10s = CopyAction("a", srcDO.id, tgt1DO.id,
      executionMode = Some(KafkaStateIncrementalMode(delayedMaxTimestampExpr = Some(s"timestamp_seconds(unix_seconds(now()) - $delaySecs)"))))

    // first dag run
    {
      val dag: ActionDAGRun = ActionDAGRun(Seq(action1Delay10s))
      dag.prepare(contextPrep)
      dag.init(contextInit)
      dag.exec(contextExec)
    }

    // check there is no data because of delay not yet over.
    assert(tgt1DO.getSparkDataFrame().count() == 0)

    // second dag run
    {
      val delaySecs = 1
      Thread.sleep(delaySecs*1000 + 500) // make sure to wait 1s
      val action1Delay1s = action1Delay10s.copy(
        executionMode = Some(KafkaStateIncrementalMode(delayedMaxTimestampExpr = Some(s"timestamp_seconds(unix_seconds(now()) - $delaySecs)"))))
      val dag: ActionDAGRun = ActionDAGRun(Seq(action1Delay1s))
      dag.prepare(contextPrep)
      dag.init(contextInit)
      dag.exec(contextExec)
    }

    // check data now written to tgt1
    assert(tgt1DO.getSparkDataFrame().count() == 1)
  }

}

case class TestPerson(lastname: String, firstname: String, rating: Int)
