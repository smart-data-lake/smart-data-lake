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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

import java.time.LocalDateTime

private[smartdatalake] case class HadoopFileActionDAGRunStateStore(statePath: String, appName: String, hadoopConf: Configuration) extends ActionDAGRunStateStore[HadoopFileStateId] with SmartDataLakeLogger {

  private val hadoopStatePath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(statePath))
  private val indexFile = new Path(hadoopStatePath, "index")
  val currentStatePath: Path = new Path(hadoopStatePath, "current")
  val succeededStatePath: Path = new Path(hadoopStatePath, "succeeded")
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithConf(hadoopStatePath)(hadoopConf)
  if (!filesystem.exists(hadoopStatePath)) filesystem.mkdirs(currentStatePath) // make sure current state directory exists
  filesystem.setWriteChecksum(false) // disable writing CRC files
  HdfsUtil.writeTest(hadoopStatePath) // make sure state directory is writable

  /**
   * Save state to file
   */
  override def saveState(state: ActionDAGRunState): Unit = synchronized {
    // write state file
    val (filename, filePath) = saveStateToFile(state)
    // if succeeded:
    // - delete temporary state file from current directory
    // - move previous failed attempt files from current to succeeded directory
    if (state.isSucceeded) {
      filesystem.delete(new Path(currentStatePath, filename), false)
      getFiles(Some(currentStatePath))
        .filter( stateFile => stateFile.runId==state.runId && stateFile.attemptId<state.attemptId)
        .foreach { stateFile =>
          val tgtFile = new Path(succeededStatePath, stateFile.path.getName)
          HdfsUtil.renamePath(stateFile.path, tgtFile)
          logger.info(s"renamed ${stateFile.path} -> $tgtFile")
        }
    }
    // if final, update index file if enabled
    if (state.isFinal && Environment.hadoopFileStateStoreIndexAppend) {
      val relativeFile = hadoopStatePath.toUri.relativize(filePath.toUri).toString
      val indexEntry = IndexEntry.from(state, relativeFile)
      val newContent = indexEntry.toJson + "\n"
      if (filesystem.exists(indexFile)) {
        try {
          HdfsUtil.appendHadoopFile(indexFile, newContent)
          logger.info("appended current run to index file")
        } catch {
          case _: UnsupportedOperationException =>
            var currentContent = HdfsUtil.readHadoopFile(indexFile)
            if (!currentContent.endsWith("\n")) currentContent = currentContent + "\n"
            HdfsUtil.writeHadoopFile(indexFile, currentContent + newContent)
            logger.info("recreated index file including current run (append not supported by filesystem implementation)")
        }
      } else {
        HdfsUtil.writeHadoopFile(indexFile, newContent)
        logger.info("created index file with current run")
      }
    }
  }

  def saveStateToFile(state: ActionDAGRunState): (String, Path) = {
    val path = if (state.isSucceeded) succeededStatePath else currentStatePath
    val json = state.toJson
    val fileName = s"$appName${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}${state.runId}${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}${state.attemptId}.json"
    val file = new Path(path, fileName)
    HdfsUtil.writeHadoopFile(file, json)
    logger.info(s"updated state into $file")
    // return
    (fileName, file)
  }

  /**
   * Get latest state
   * @param runId optional runId to search for latest state
   */
  override def getLatestStateId(runId: Option[Int] = None): Option[HadoopFileStateId] = {
    val latestStateFile = getFiles()
      .filter(x => runId.isEmpty || runId.contains(x.runId))
      .sortBy(_.getSortAttrs).lastOption
    if (latestStateFile.isEmpty) logger.info(s"No state file for application $appName and runId ${runId.getOrElse("latest")} found.")
    else logger.debug(s"got state from file ${latestStateFile}")
    latestStateFile
  }

  /**
   * Get latest runId
   */
  override def getLatestRunId: Option[Int] = {
    val latestStateFile = getFiles()
      .sortBy(_.getSortAttrs).lastOption
    logger.debug(s"latest state file is ${latestStateFile}")
    latestStateFile.map(_.runId)
  }

  /**
   * Search state directory for state files of this app
   */
  def getFiles(path: Option[Path] = None): Seq[HadoopFileStateId] = {
    val filenameMatcher = s"(.+)\\${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}([0-9]+)\\${HadoopFileActionDAGRunStateStore.fileNamePartSeparator}([0-9]+)\\.json".r
    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = path.getName.startsWith(appName + HadoopFileActionDAGRunStateStore.fileNamePartSeparator)
    }
    val searchPath = path.getOrElse( new Path(hadoopStatePath, "*"))
    logger.debug(s"searching path $searchPath for state")
    filesystem.globStatus(new Path(searchPath, "*.json"), pathFilter )
      .filter( x => x.isFile)
      .map{ x => logger.debug(s"found files ${x.getPath}"); x }
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
    val json = HdfsUtil.readHadoopFile(stateFile)
    ActionDAGRunState.fromJson(json)
  }
}

case class HadoopFileStateId(path: Path, appName: String, runId: Int, attemptId: Int) extends StateId {
  def getSortAttrs: (Int, Int) = (runId, attemptId)
}

private case class IndexEntry(name: String, runId: Int, attemptId: Int, feedSel: String,
                      runStartTime: LocalDateTime, attemptStartTime: LocalDateTime, runEndTime: Option[LocalDateTime],
                      status: RuntimeEventState, actionStatus: Map[RuntimeEventState,Int],
                      buildVersion: Option[String], appVersion: Option[String], path: String) {
  def toJson: String = {
    val str = ActionDAGRunState.toJson(this)
    assert(str.linesIterator.size == 1) // index entry should be serialized to one json line!
    str
  }
}
private object IndexEntry {
  def from(state: ActionDAGRunState, relativePath: String) = {
    implicit val localDateTimeOrdering: Ordering[LocalDateTime] = _ compareTo _
    val runEndTime = state.actionsState.values.flatMap(_.endTstmp).toSeq.sorted.lastOption
    val actionsStatus = state.actionsState.values.groupBy(_.state).mapValues(_.size)
    IndexEntry(
      state.appConfig.appName, state.runId, state.attemptId, state.appConfig.feedSel,
      state.runStartTime, state.attemptStartTime, runEndTime,
      state.finalState.get, actionsStatus, state.buildVersionInfo.map(_.version),
      state.appVersion, relativePath
    )
  }
}

private[smartdatalake] object HadoopFileActionDAGRunStateStore {
  val fileNamePartSeparator = "."
}