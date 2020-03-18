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
package io.smartdatalake.util.jms

import io.smartdatalake.util.misc.SmartDataLakeLogger
import javax.jms.{Message, TextMessage}

/**
 * Handler for JMS text messages.
 */
private[smartdatalake] object TextMessageHandler extends SmartDataLakeLogger {


  /**
   * Convertes a JMS text message to String.
   *
   * @param msg JMS message that is read as JMS text message
   * @return Message content as String
   */
  def convert2Text(msg: Message): Option[String] = {
    val res: Option[String] =
    try {
      val txt = msg.asInstanceOf[TextMessage].getText
      logger.debug(s"JMS text message received: ${txt}")
      Some(txt)
    } catch {
      case e: Exception => {
        logger.warn(
          s"JMS message ${msg} couldn't be read as text message. Exception: ${e.toString}")
        None
      }
    }
    res
  }
}
