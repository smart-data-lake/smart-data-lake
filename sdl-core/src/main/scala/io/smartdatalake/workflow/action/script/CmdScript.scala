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
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConfigObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{EnvironmentUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Execute a command on the command line and get its std output
 * Command can be different for windows and linux operating systems, but it must be defined for at least one of them.
 *
 * If return value is not zero an exception is thrown.
 *
 * Note about internal implementation: on execution value of parameter map entries where key starts with
 * - 'param' will be added as parameter after the docker run command, sorted by key.
 * This allows to customize execution behaviour through Actions or DataObjects using CmdScript.
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param winCmd       Cmd to execute on windows operating systems - note that it is executed with "cmd /C" prefixed
 * @param linuxCmd     Cmd to execute on linux operating systems - note that it is executed with "sh -c" prefixed.
 */
case class CmdScript(override val name: String = "cmd", override val description: Option[String] = None, winCmd: Option[String] = None, linuxCmd: Option[String] = None) extends CmdScriptBase {
  if (EnvironmentUtil.isWindowsOS) assert(winCmd.isDefined, s"($name) winCmd must be defined when running on Windows")
  if (!EnvironmentUtil.isWindowsOS) assert(linuxCmd.isDefined, s"($name) linuxCmd must be defined when running on Linux")

  override private[smartdatalake] def getCmd(parameters: Map[String,String]): Seq[String] = {
    val cmd = if (EnvironmentUtil.isWindowsOS) Seq("cmd", "/C") ++ CmdScript.splitCmdParameters(winCmd.get) // parameters must be split for windows but not for linux
    else Seq("sh", "-c", linuxCmd.get)
    cmd ++ parameters.filterKeys(_.startsWith("param")).toSeq.sortBy(_._1).map(_._2)
  }

  override def factory: FromConfigFactory[ParsableScriptDef] = CmdScript

  override private[smartdatalake] def isWslCmd = EnvironmentUtil.isWindowsOS && winCmd.exists(_.startsWith("wsl"))
}

/**
 * Generic implementation of calling ProcessBuilder for different scripts executing command line
 * On execution value of parameter map entries where key starts with 'param' will be added as parameter to command.
 * If return value is not zero an exception is thrown.
 */
trait CmdScriptBase extends ParsableScriptDef with SmartDataLakeLogger {

  override def execStdOutString(configObjectId: ConfigObjectId, partitionValues: Seq[PartitionValues], parameters: Map[String,String], errors: mutable.Buffer[String] = mutable.Buffer())(implicit context: ActionPipelineContext): String = {
    import sys.process._
    val cmd = getCmd(parameters)
    val errLogger = ProcessLogger.apply { err =>
      logger.error(err)
      errors.append(err)
    }
    logger.info(s"($configObjectId) executing command: ${cmd.mkString(" ")}")
    cmd.!!(errLogger)
  }

  override def execStdOutStream(configObjectId: ConfigObjectId, partitionValues: Seq[PartitionValues], parameters: Map[String, String], errors: mutable.Buffer[String] = mutable.Buffer())(implicit context: ActionPipelineContext): Stream[String] = {
    import sys.process._
    val cmd = getCmd(parameters)
    val errLogger = ProcessLogger.apply { err =>
      logger.error(err)
      errors.append(err)
    }
    logger.info(s"($configObjectId) executing command: ${cmd.mkString(" ")}")
    cmd.lineStream(errLogger)
  }

  /**
   * Should return cmd to be executed by ProcessBuilder.
   * To be implemented by Subclasses.
   * @param parameters Map of parameters transmitted by the caller on execution time
   */
  private[smartdatalake] def getCmd(parameters: Map[String,String]): Seq[String]

  /**
   * Returns true if this is a Windows Linux Subsystem (Wsl) command.
   * This information is needed to prepare paths accordingly.
   * To be implemented by Subclasses.
   */
  private[smartdatalake] def isWslCmd: Boolean

  /**
   * Translates path if command is executed inside Windows Linux Subsystem (Wsl).
   * In that case a windows path is translated to it's corresponding path inside Wsl.
   */
  private[smartdatalake] def preparePath(path: String): String = {
    if (isWslCmd) translateWinToWslPath(path)
    else path
  }

  /**
   * Uses Wsl command to translate a windows path to it's corresponding path inside Wsl.
   */
  private def translateWinToWslPath(path: String): String = {
    assert(EnvironmentUtil.isWindowsOS)
    import sys.process._
    val wslPath = s"wsl wslpath '$path'".!!
    wslPath.linesIterator.find(_.nonEmpty).get.trim
  }
}

object CmdScript extends FromConfigFactory[ParsableScriptDef] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CmdScript = {
    extract[CmdScript](config)
  }

  /**
   * Splits command parameters respecting quotes.
   */
  private[smartdatalake] def splitCmdParameters(cmd: String): Seq[String] = {
    val quoteChars = "\"'"
    cmdParametersRegex.findAllMatchIn(cmd)
      .map(_.matched.dropWhile(quoteChars.contains(_)).reverse.dropWhile(quoteChars.contains(_)).reverse).toSeq
  }
  private val cmdParametersRegex = "[^\\s\"']+|\"[^\"]*\"|'[^']*'".r
}
