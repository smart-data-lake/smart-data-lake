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

import java.nio.file.Files
import java.time.LocalDateTime
import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.action.{CopyAction, SDLExecutionId}
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.connection.KafkaConnection
import io.smartdatalake.workflow.dataobject._
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
 * Note about EmbeddedKafka compatibility:
 * The currently used version 2.4.1 (in sync with the kafka version of sparks parent pom) is not compatible with JDK14+
 * because of a change of InetSocketAddress::toString. Zookeeper doesn't start because of
 * "java.nio.channels.UnresolvedAddressException: Session 0x0 for server localhost/<unresolved>:6001, unexpected error, closing socket connection and attempting reconnect"
 * see also https://www.oracle.com/java/technologies/javase/14all-relnotes.html#JDK-8225499
 */
class ActionDAGKafkaTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with EmbeddedKafka {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  private lazy val kafka = EmbeddedKafka.start()
  override def beforeAll() {
    kafka // initialize lazy variable
  }

  override def afterAll(): Unit = {
    kafka.stop(true)
  }

  before {
    instanceRegistry.clear()
  }

  test("action dag with 2 actions in sequence where 2nd action reads different schema than produced by last action") {
    // Note: Some DataObjects remove & add columns on read (e.g. KafkaTopicDataObject, SparkFileDataObject)
    // In this cases we have to break the lineage und create a dummy DataFrame in init phase.
    implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

    // setup DataObjects
    val feed = "actionpipeline"
    val kafkaConnection = KafkaConnection("kafkaCon1", "localhost:6000")
    instanceRegistry.register(kafkaConnection)
    val srcTable = Table(Some("default"), "ap_input")
    val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
    srcDO.dropTable
    instanceRegistry.register(srcDO)
    createCustomTopic("topic1", Map(), 1, 1)
    val tgt1DO = KafkaTopicDataObject("kafka1", topicName = "topic1", connectionId = "kafkaCon1", valueType = KafkaColumnType.String, selectCols = Seq("value", "timestamp"), schemaMin = Some(StructType(Seq(StructField("timestamp", TimestampType)))))
    instanceRegistry.register(tgt1DO)
    createCustomTopic("topic2", Map(), 1, 1)
    val tgt2DO = KafkaTopicDataObject("kafka2", topicName = "topic2", connectionId = "kafkaCon1", valueType = KafkaColumnType.String, selectCols = Seq("value", "timestamp"), schemaMin = Some(StructType(Seq(StructField("timestamp", TimestampType)))))
    instanceRegistry.register(tgt2DO)

    // prepare DAG
    val l1 = Seq(("doe-john", 5)).toDF("key", "value")
    srcDO.writeDataFrame(l1, Seq())
    val action1 = CopyAction("a", srcDO.id, tgt1DO.id)
    val action2 = CopyAction("b", tgt1DO.id, tgt2DO.id, transformer = Some(CustomDfTransformerConfig(sqlCode = Some("select 'test' as key, value from kafka1"))))
    val dag: ActionDAGRun = ActionDAGRun(Seq(action1, action2))

    // exec dag
    dag.prepare
    dag.init
    dag.exec

    // check result
    val dfR1 = tgt2DO.getDataFrame(Seq())
    assert(dfR1.columns.toSet == Set("value","timestamp"))
    val r1 = dfR1
      .select($"value")
      .as[String].collect().toSeq
    assert(r1 == Seq("5"))

    // check metrics
    // note: metrics don't work for Kafka sink in Spark 2.4
    //val action2MainMetrics = action2.getFinalMetrics(action2.outputId).get.getMainInfos
    //assert(action2MainMetrics("records_written") == 1)
  }

}