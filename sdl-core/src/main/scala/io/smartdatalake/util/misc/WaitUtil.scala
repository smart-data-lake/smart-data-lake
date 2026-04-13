/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.util.misc

import java.util.concurrent.TimeoutException

object WaitUtil extends SmartDataLakeLogger {

  def sleepUntil(timeoutSec: Option[Int] = None, pollIntervalSec: Int = 1, logInfo: Option[String])(conditionFun: () => Boolean): Unit = {
    val ts = System.currentTimeMillis()
    while (!conditionFun()) {
      logger.debug(s"waiting${logInfo.map(s => s" for $s").getOrElse("")}")
      Thread.sleep(pollIntervalSec * 1000L)
      if (timeoutSec.isDefined && ts + timeoutSec.get * 1000L <= System.currentTimeMillis) throw new TimeoutException(s"Timeout waiting${logInfo.map(s => s" for $s").getOrElse("")} after $timeoutSec seconds")
    }
  }

}
