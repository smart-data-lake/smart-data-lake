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
package io.smartdatalake.config.exporter

import io.smartdatalake.app.BackendClient
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionDAGRunState, HadoopFileActionDAGRunStateStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}

/**
 * Identifies the state of an attempt of a workflow run.
 */
case class StateRunId(runId: Int, attemptId: Int)

object StateRunId {
  implicit val ordering: Ordering[StateRunId] = Ordering.by(x => (x.runId, x.attemptId))
}

/**
 * Location the state of workflow runs can be read from and written to, e.g. to synchronize it between a
 * state directory and the UI backend.
 */
trait StateSyncEndpoint {

  /**
   * List the applications state is stored for.
   */
  def listApplications(): Seq[String]

  /**
   * List the runs and attempts stored for an application.
   */
  def listRuns(application: String): Seq[StateRunId]

  /**
   * The latest run and attempt stored for an application.
   */
  def latestRun(application: String): Option[StateRunId] = listRuns(application).maxOption

  /**
   * Read the state of a run and attempt as Json, see [[ActionDAGRunState.toJson]].
   */
  def readState(application: String, run: StateRunId): Option[String]

  /**
   * Write the state of a run and attempt given as Json. The application, run and attempt are part of the state.
   */
  def writeState(stateJson: String): Unit
}

object StateSyncEndpoint {

  /**
   * Create state endpoint depending on uri scheme: 'uiBackend' or a Hadoop path of a state directory, as given to
   * SmartDataLakeBuilder with `--state-path`. A 'localfile:' prefix is removed, so the same uris as for
   * [[ExportWriter]] can be used.
   */
  def apply(uri: String, configPaths: Seq[String] = Seq(), backendClient: Option[BackendClient] = None, hadoopConfig: Option[Configuration] = None): StateSyncEndpoint = {
    uri.takeWhile(_ != ':').toLowerCase match {
      case "uibackend" =>
        backendClient
          .orElse(if (configPaths.nonEmpty) Some(BackendClient(configPaths)) else None)
          .getOrElse(throw new IllegalArgumentException(s"cannot initialize BackendClient as configPaths and global.uiBackend are missing"))
      case _ => HadoopStateSyncEndpoint(uri.stripPrefix("localfile:"), hadoopConfig.getOrElse(new Configuration()))
    }
  }
}

/**
 * Reads and writes the state directory of [[HadoopFileActionDAGRunStateStore]].
 *
 * Reading does not use HadoopFileActionDAGRunStateStore, as it checks that the state directory is writable.
 */
case class HadoopStateSyncEndpoint(statePath: String, hadoopConf: Configuration) extends StateSyncEndpoint with SmartDataLakeLogger {
  private val hadoopStatePath = HdfsUtil.addHadoopDefaultSchemaAuthority(new HadoopPath(statePath))
  private implicit lazy val filesystem: FileSystem = HdfsUtil.getHadoopFsWithConf(hadoopStatePath)(hadoopConf)
  private val separator = HadoopFileActionDAGRunStateStore.fileNamePartSeparator
  private val filenameMatcher = s"(.+)\\$separator([0-9]+)\\$separator([0-9]+)\\.json".r

  override def listApplications(): Seq[String] = getFiles.map(_._1).distinct.sorted

  override def listRuns(application: String): Seq[StateRunId] = {
    getFiles.filter(_._1 == application).map(_._2).distinct.sorted
  }

  override def readState(application: String, run: StateRunId): Option[String] = {
    // if a state file exists in both directories for the same attempt, the one in 'succeeded' wins, see HadoopFileStateId
    getFiles.filter(f => f._1 == application && f._2 == run)
      .sortBy(_._3.getParent.getName == HadoopFileActionDAGRunStateStore.succeededDirName)
      .lastOption.map(f => HdfsUtil.readHadoopFile(f._3))
  }

  override def writeState(stateJson: String): Unit = {
    val state = ActionDAGRunState.fromJson(stateJson)
    HadoopFileActionDAGRunStateStore(statePath, state.appConfig.appName, hadoopConf).saveState(state)
  }

  private def getFiles: Seq[(String, StateRunId, HadoopPath)] = {
    val dirs = Seq(HadoopFileActionDAGRunStateStore.currentDirName, HadoopFileActionDAGRunStateStore.succeededDirName)
    dirs.flatMap { dir =>
      Option(filesystem.globStatus(new HadoopPath(new HadoopPath(hadoopStatePath, dir), "*.json"))).toSeq.flatten
        .filter(_.isFile)
        .flatMap(f => f.getPath.getName match {
          case filenameMatcher(appName, runId, attemptId) => Some((appName, StateRunId(runId.toInt, attemptId.toInt), f.getPath))
          case _ => None
        })
    }
  }
}
