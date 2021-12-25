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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.EnvironmentUtil

import java.nio.file.{FileSystems, Paths}

/**
 * Run a docker image and get its std output.
 *
 * If return value is not zero an exception is thrown.
 *
 * Note about internal implementation: on execution value of parameter map entries where key starts with
 * - 'runParam' will be added as parameter after the docker run command, sorted by their key.
 * - 'dockerParam' will be added as parameter for the docker command, e.g. before the image name in the docker run command, sorted by their key.
 * This allows to customize execution behaviour through Actions or DataObjects using CmdScript.
 *
 * @param name                name of the transformer
 * @param description         Optional description of the transformer
 * @param image               Docker image to run
 * @param winDockerCmd        Cmd to execute docker on windows operating systems. Default is 'docker'.
 * @param linuxDockerCmd      Cmd to execute docker on linux operating systems. Default is 'docker'.
 * @param localDataDirToMount Optional directory that will be mounted as /mnt/data in the container. This is needed if your container wants to access files available in your local filesystem.
 */
case class DockerRunScript(override val name: String = "docker-run", override val description: Option[String] = None,
                           image: String,
                           winDockerCmd: String = "docker",
                           linuxDockerCmd: String = "docker",
                           localDataDirToMount: Option[String] = None
                          ) extends CmdScriptBase {

  final val containerDataDir = "/mnt/data"

  override private[smartdatalake] def getCmd(parameters: Map[String,String]): Seq[String] = {
    // prepare local data dir
    val workDir = FileSystems.getDefault.getPath("").toAbsolutePath
    val localDataDirToMountAbsolut = localDataDirToMount.map(d => if (!Paths.get(d).isAbsolute) s"$workDir/$d" else d)
    val localDataDirToMountParameters = localDataDirToMountAbsolut.map(d => Seq("-v", s"${preparePath(d)}:$containerDataDir")).getOrElse(Seq())
    // prepare parameters
    val dockerParameters = parameters.filterKeys(_.startsWith("dockerParam")).toSeq.sortBy(_._1).map(_._2) ++ localDataDirToMountParameters
    val runParameters = parameters.filterKeys(_.startsWith("runParam")).toSeq.sortBy(_._1).map(_._2)
    // cmd must be split for windows but not for linux
    val cmd = if (EnvironmentUtil.isWindowsOS) Seq("cmd", "/C") ++ CmdScript.splitCmdParameters(winDockerCmd) ++ dockerParameters ++ Seq(image) ++ runParameters
    else Seq("sh", "-c", linuxDockerCmd, (Seq(linuxDockerCmd) ++ dockerParameters :+ Seq(image) ++ runParameters).mkString(" "))
    cmd
  }

  override def factory: FromConfigFactory[ParsableScriptDef] = DockerRunScript

  override private[smartdatalake] def isWslCmd = EnvironmentUtil.isWindowsOS && winDockerCmd.startsWith("wsl")
}

object DockerRunScript extends FromConfigFactory[ParsableScriptDef] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DockerRunScript = {
    extract[DockerRunScript](config)
  }
}