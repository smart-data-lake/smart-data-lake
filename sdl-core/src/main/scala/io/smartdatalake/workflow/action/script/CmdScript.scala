/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.script

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{EnvironmentUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.SparkSession

/**
 * Execute a command.
 * Command can be different for windows and linux operating systems, but it must be defined for at least one of them.
 * If return value is not zero an exception is thrown.
 * The last line of the scripts standard output is parsed as key-value and passed on as parameters in the output subfeed.
 * Key-value format: k1=v1 k2=v2
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param winCmd       Cmd to execute on windows operating systems - note that it is executed with "cmd /C" prefixed
 * @param linuxCmd     Cmd to execute on linux operating systems - note that it is executed with "sh -c" prefixed.
 */
case class CmdScript(override val name: String = "cmd", override val description: Option[String] = None, winCmd: Option[String] = None, linuxCmd: Option[String] = None) extends ParsableScriptDef with SmartDataLakeLogger {
  if (EnvironmentUtil.isWindowsOS) assert(winCmd.isDefined, s"($name) winCmd must be defined when running on Windows")
  if (!EnvironmentUtil.isWindowsOS) assert(linuxCmd.isDefined, s"($name) linuxCmd must be defined when running on Linux")

  override def execStdOut(actionId: ActionId, partitionValues: Seq[PartitionValues], parameters: Map[String,String])(implicit context: ActionPipelineContext): String = {
    import sys.process._
    val cmd = getCmd
    logger.info(s"($actionId) executing command: ${cmd.mkString(" ")}")
    cmd.!!
  }

  private def getCmd: Seq[String] = {
    if (EnvironmentUtil.isWindowsOS) Seq("cmd", "/C", winCmd.get)
    else Seq("sh", "-c", linuxCmd.get)
  }

  override def factory: FromConfigFactory[ParsableScriptDef] = CmdScript
}

object CmdScript extends FromConfigFactory[ParsableScriptDef] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CmdScript = {
    extract[CmdScript](config)
  }
}
