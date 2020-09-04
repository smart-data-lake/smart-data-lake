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
/*
Code copied/adapted/simplified from https://github.com/tbfenet/spark-jms-receiver
*/

package io.smartdatalake.util.jms

import io.smartdatalake.util.misc.SmartDataLakeLogger
import javax.jms.{Message, Session}
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._


/**
 * Synchronized JMS Receiver
 *
 * @param consumerFactory  JMS Message Consumer Factory
 *
 * @param messageConverter function to map JMS message to a type
 * @param batchSize        Number of JMS messages being processed in one batch
 * @param maxWait          Max. wait time per iteration to fetch JMS messages
 * @param maxBatchAge      Max. age of collected JMS messages
 * @tparam T               Type to which the JMS messages get mapped to
 */
private[smartdatalake] class SynchronousJmsReceiver[T](override val consumerFactory: MessageConsumerFactory,
                                override val messageConverter: Message => Option[T],
                                val batchSize: Int = 100000,
                                val maxWait: Duration = 1.second,
                                val maxBatchAge: Duration = 120.seconds,
                                val txBatchSize: Int = 100,
                                override val session: SparkSession)
  extends BaseJmsReceiver[T](consumerFactory, messageConverter, session) with SmartDataLakeLogger {


  /**
   * @inheritdoc
   *
   * Logic: JMS messages are being collected until 1 of 2 conditions are met:
   * - Timeout maxBatchAge was reached
   * - Number of collected JMS messages has reached batchSize
   */
  override def receiveMessages(): Option[DataFrame] = {
    var df: Option[DataFrame] = None
    var running = true
    var lastCommitted = 0
    val buffer = ArrayBuffer[Message]()
    val startTime = System.currentTimeMillis()
    logger.info(
      s"""
      |JMS message processing modality:
      |Over a timeframe of ${maxBatchAge} a maximum of  ${batchSize} messages are collected.
      |For each message, the wait time is ${maxWait}.
      |For each batch of ${txBatchSize} messages, the messages are acknowledged to MQ (committed).
       """.stripMargin)
    try {
      val consumer = consumerFactory.newConsumer(Session.CLIENT_ACKNOWLEDGE)
      while (running) {
        logger.debug("Polling for JMS messages ...")
        val message = if (maxWait.toMillis > 0) {
          consumer.receive(maxWait.toMillis)
        } else {
          consumer.receiveNoWait()
        }
        if (message != null && running) {
          buffer += message
          logger.debug("JMS message fetched.")
        }
        val endTime = System.currentTimeMillis()
        val elapsedTime = endTime - startTime
        if (elapsedTime >= maxBatchAge.toMillis || buffer.size >= batchSize) {
          storeBuffer
          running = false
        } else {
          if (lastCommitted + txBatchSize == buffer.size) {
            buffer.last.acknowledge()
            logger.info(s"Intermediate result of committed JMS messages: ${lastCommitted}")
            lastCommitted = buffer.size
          }
        }
      }
    } catch {
      case e: Exception => {
        throw new JmsReceiverException(
          s"Problem when processing JMS messages: ${e}")
      }
      // TODO: Implement recovery/restart? How?
      // TODO: Committed messages should actually be persisted on Hadoop already. How can we make sure of that?
    }

    def storeBuffer() = {
      if (buffer.nonEmpty) {
        df = Some(store(buffer.flatMap(x => messageConverter(x))))
        buffer.last.acknowledge()
        lastCommitted = buffer.size
        logger.info(s"Total number of committed JMS messages: ${lastCommitted}")
        logger.info(s"A total of ${buffer.size} JMS messages were read.")
        buffer.clear()
      } else {
        logger.info("No JMS messages in queue found.")
        df = None
      }
    }
    df
  }
}
