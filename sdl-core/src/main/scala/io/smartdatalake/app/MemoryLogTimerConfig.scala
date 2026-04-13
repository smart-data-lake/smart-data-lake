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
package io.smartdatalake.app

import io.smartdatalake.util.misc.MemoryUtils

/**
 * Configuration for periodic memory usage logging
 *
 * @param intervalSec interval in seconds between memory usage logs
 * @param logLinuxMem enable logging linux memory
 * @param logLinuxCGroupMem enable logging details about linux cgroup memory
 * @param logBuffers enable logging details about different jvm buffers
 */
case class MemoryLogTimerConfig(intervalSec: Int, logLinuxMem: Boolean = true, logLinuxCGroupMem: Boolean = false, logBuffers: Boolean = false ) {
  import MemoryLogTimerConfig._
  private[smartdatalake] def startTimer(): Unit = MemoryUtils.startMemoryLogger(intervalSec, logLinuxMem, logLinuxCGroupMem, logBuffers)
  private[smartdatalake] def getAsMap: Map[String, String] = Map(INTERVAL_SEC_OPTION -> intervalSec.toString, LOG_LINUX_MEM_OPTION -> logLinuxMem.toString, LOG_LINUX_CGROUP_MEM_OPTION -> logLinuxCGroupMem.toString, LOG_BUFFERS_OPTION -> logBuffers.toString)
}
private[smartdatalake] object MemoryLogTimerConfig{
  def from(conf: Map[String,String]): MemoryLogTimerConfig = MemoryLogTimerConfig(conf(INTERVAL_SEC_OPTION).toInt, conf(LOG_LINUX_MEM_OPTION).toBoolean, conf(LOG_LINUX_CGROUP_MEM_OPTION).toBoolean, conf(LOG_BUFFERS_OPTION).toBoolean)
  private val INTERVAL_SEC_OPTION = "spark.smartdatalake.memoryLog.intervalSec"
  private val LOG_LINUX_MEM_OPTION = "spark.smartdatalake.memoryLog.logLinuxMem"
  private val LOG_LINUX_CGROUP_MEM_OPTION = "spark.smartdatalake.memoryLog.logLinuxCGroupMem"
  private val LOG_BUFFERS_OPTION = "spark.smartdatalake.memoryLog.logBuffers"
}
