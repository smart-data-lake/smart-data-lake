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

import javax.jms.{Connection, MessageConsumer, Session}

/**
 * JMS Message Consumer Factory
 */
private[smartdatalake] trait MessageConsumerFactory extends Serializable {

  /** JMS Connection
    *
    */
  var connection: Connection = _

  /**
   * Creates a new (freshly initialized) JMS message consumer.
   *
   * @param acknowledgeMode Message acknowledgement mode
   * @return JMS Message Consumer
   */
  def newConsumer(acknowledgeMode: Int): MessageConsumer = {
    stopConnection()
    connection = makeConnection
    val session = makeSession(acknowledgeMode)
    val consumer = makeConsumer(session)
    connection.start()
    consumer
  }

  private def makeSession(acknowledgeMode: Int): Session = {
    connection.createSession(false, acknowledgeMode)
  }

  /**
   * Stopps the JMS connection
   */
  def stopConnection(): Unit = {
    try {
      if (connection != null) {
        connection.close()
      }
    } finally {
      connection = null
    }
  }

  /**
   * Creates a JMS connection
   *
   * @return JMS Connection
   */
  def makeConnection: Connection

  /**
   * Creates a JMS message consumer.
   *
   * @param session JMS Session
   * @return JMS Message Consumer
   */
  def makeConsumer(session: Session): MessageConsumer
}
