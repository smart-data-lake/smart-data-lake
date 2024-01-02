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

import javax.jms._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Base class for JMS receiver
 *
 * @param consumerFactory JMS Consumer Factory
 * @param messageConverter Function to map JMS message to a data type
 * @param session [[SparkSession]]
 * @tparam T Type on which the JMS message should be mapped to
 */
private[smartdatalake] abstract class BaseJmsReceiver[T](val consumerFactory: MessageConsumerFactory,
                                  val messageConverter: Message => Option[T],
                                  val session: SparkSession) {

  /**
   * Pulls messages from a queue via JMS, collects them and  makes them available as a [[DataFrame]].
   * @return [[Option[DataFrame] ]] with JMS messages (their content respectively)
   */
  // TODO: Implement
  def receiveMessages(): Option[DataFrame] = ???



  /**
   * Transforms the collected JMS messages into a [[DataFrame]].
   *
   * @param dataBuffer Buffer containing the collected JMS text messages
   * @return [[DataFrame]] with 1 column containing the content of the messages as string(1 row for each message).
   */
  def store(dataBuffer : scala.collection.mutable.ArrayBuffer[T]) : DataFrame = {
    val msgStrings: RDD[TextMessageString] = session.sparkContext.parallelize(dataBuffer.map(x =>
      TextMessageString(x.asInstanceOf[String])).toSeq)
    session.createDataFrame(msgStrings)
  }
}





