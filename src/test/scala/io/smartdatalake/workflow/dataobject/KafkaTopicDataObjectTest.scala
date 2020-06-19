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

import io.smartdatalake.workflow.connection.KafkaConnection
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.FunSuite

class KafkaTopicDataObjectTest extends FunSuite with EmbeddedKafka with DataObjectTestSuite {

  import testSession.implicits._
  import io.smartdatalake.util.misc.DataFrameUtil.DfSDL

  private val kafkaConnection = KafkaConnection("kafkaCon1", "localhost:6000")

  test("Can read and write from Kafka") {
    withRunningKafka {
      createCustomTopic("topic", Map(), 1, 1)
      publishStringMessageToKafka("topic", "message")
      assert(consumeFirstStringMessageFrom("topic")=="message", "Whoops - couldn't read message")
    }
  }

  test("DataObject write and read kafka topic") {
    val topic = "testTopic"
    withRunningKafka {
      createCustomTopic(topic, Map(), 1, 1)
      instanceRegistry.register(kafkaConnection)
      val dataObject = KafkaTopicDataObject("kafka1", topicName = topic, connectionId = "kafkaCon1")
      val df = Seq(("john doe", "5"), ("peter smith", "3"), ("emma brown", "7")).toDF("key", "value")
      dataObject.writeDataFrame(df, Seq())
      val dfRead = dataObject.getDataFrame(Seq())
      assert(dfRead.symmetricDifference(df).isEmpty)
    }
  }

  after {
    EmbeddedKafka.stop()
  }
}
