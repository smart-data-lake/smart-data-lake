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
package io.smartdatalake.debezium

import io.debezium.engine.DebeziumEngine
import io.smartdatalake.util.misc.SmartDataLakeLogger

import java.util.concurrent.ExecutorService


private[smartdatalake] class DebeziumCompletionCallback(executorService: ExecutorService) extends DebeziumEngine.CompletionCallback with SmartDataLakeLogger {

  var error: Option[Throwable] = None;

  override def handle(success: Boolean, message: String, error: Throwable): Unit = {
    if (success) logger.info(s"Debezium ended successfully with {$message}")
    else {
      logger.warn(s"Debezium failed with {$message}")
      this.error = Some(error)
    }


    this.executorService.shutdown()
  }
}

