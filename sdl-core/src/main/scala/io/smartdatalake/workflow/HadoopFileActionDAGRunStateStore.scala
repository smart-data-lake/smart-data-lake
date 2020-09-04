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

package io.smartdatalake.workflow

import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, PathFilter}

import scala.io.Codec

private[smartdatalake] case class HadoopFileActionDAGRunStateStore(statePath: String, appName: String) extends ActionDAGRunStateStore[HadoopFileStateId] with SmartDataLakeLogger {

  private val hadoopStatePath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(statePath))
  private val currentStatePath = new Path(hadoopStatePath, "current")
  private val succeededStatePath = new Path(hadoopStatePath, "succeeded")
  implicit private val filesystem: FileSystem = HdfsUtil.getHadoopFs(hadoopStatePath)
  if (!filesystem.exists(hadoopStatePath)) filesystem.mkdirs(hadoopStatePath)
  filesystem.setWriteChecksum(false) // disable writing CRC files

  /**
   * Save state to file
   */
  override def saveState(state: ActionDAGRunState): Unit = synchronized {
    val path = if (state.isSucceeded) succeededStatePath else currentStatePath
    // write state file
    val json = state.toJson
    val fileName = s"$appName${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}${state.runId}${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}${state.attemptId}.json"
    val file = new Path(path, fileName)
    val os = filesystem.create(file, true) // overwrite if exists
    os.write(json.getBytes("UTF-8"))
    os.close()
    logger.info(s"updated state into ${file.toUri}")
    // if succeeded:
    // - delete temporary state file from current directory
    // - move previous failed attempt files from current to succeeded directory
    if (state.isSucceeded) {
      filesystem.delete(new Path(currentStatePath, fileName), /*recursive*/ false)
      getFiles(Some(currentStatePath))
        .filter( stateFile => stateFile.runId==state.runId && stateFile.attemptId<state.attemptId)
        .foreach { stateFile =>
          logger.info(s"renamed ${stateFile.path}")
          FileUtil.copy(filesystem, stateFile.path, filesystem, succeededStatePath, /*deleteSource*/ true, filesystem.getConf)
        }
    }
  }

  /**
   * Get latest state
   * @param runId optional runId to search for latest state
   */
  override def getLatestState(runId: Option[Int] = None): HadoopFileStateId = {
    val latestStateFile = getFiles()
      .filter(x => runId.isEmpty || runId.contains(x.runId))
      .sortBy(_.getSortAttrs).lastOption
    require(latestStateFile.nonEmpty, s"No state file for application $appName and runId ${runId.getOrElse("latest")} found.")
    latestStateFile.get
  }

  /**
   * Get latest runId
   */
  override def getLatestRunId: Option[Int] = {
    val latestStateFile = getFiles()
      .sortBy(_.getSortAttrs).lastOption
    latestStateFile.map(_.runId)
  }

  /**
   * Search state directory for state files of this app
   */
  private def getFiles(path: Option[Path] = None): Seq[HadoopFileStateId] = {
    val filenameMatcher = s"([^_]+)\\${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}([0-9]+)\\${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}([0-9]+)\\.json".r
    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith(appName + HadoopFileActionDAGRunStateStore.fileNamePartSeparator)
    }
    val searchPath = path.getOrElse( new Path(hadoopStatePath, "*"))
    filesystem.globStatus(new Path(searchPath, "*.json"), pathFilter )
      .filter( x => x.isFile)
      .flatMap( x => x.getPath.getName match {
        case filenameMatcher(appName, runId, attemptId) =>
          Some(HadoopFileStateId(x.getPath, appName, runId.toInt, attemptId.toInt))
        case _ => None
      })
      .filter(_.appName == this.appName)
  }

  /**
   * recover previous run state
   */
  override def recoverRunState(stateId: HadoopFileStateId): ActionDAGRunState = {
    val stateFile = stateId.path
    require(filesystem.isFile(stateFile), s"Cannot recover previous run state. ${stateFile.toUri} doesn't exists or is not a file.")
    val is = filesystem.open(stateFile)
    val json = scala.io.Source.fromInputStream(is)(Codec.UTF8).mkString
    ActionDAGRunState.fromJson(json)
  }

}

case class HadoopFileStateId(path: Path, appName: String, runId: Int, attemptId: Int) extends StateId {
  def getSortAttrs: (Int, Int) = (runId, attemptId)
}

private[smartdatalake] object HadoopFileActionDAGRunStateStore {
  val fileNamePartSeparator = "."
}