/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.communication.agent

import io.smartdatalake.app.{LocalStorageAgentSmartDataLakeBuilderConfig, SmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.communication.agent.StorageAgentServer.FileType.FileType
import io.smartdatalake.communication.agent.StorageAgentServer.{FileType, getFilename}
import io.smartdatalake.communication.message.{ActionLog, SDLMessage, SDLMessageType}
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.{SmartDataLakeLogger, WaitUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class StorageAgentServer(sdlb: SmartDataLakeBuilder) extends SmartDataLakeLogger {

  private val startTime = System.currentTimeMillis() / 1000

  private implicit val hadoopConfiguration: Configuration = new Configuration()

  def pollForInstructions(config: LocalStorageAgentSmartDataLakeBuilderConfig): Boolean = {
    val hadoopPath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(config.path))
    implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithConf(hadoopPath)
    filesystem.mkdirs(hadoopPath)

    logger.info(s"Polling for instructions in ${config.path}")
    if (config.stopAfterSec.exists(_ + startTime < System.currentTimeMillis() / 1000)) {
      logger.info(s"Agent is going to stop now, as it has been running for ${config.stopAfterSec.get} seconds")
      return false
    }
    WaitUtil.sleepUntil(pollIntervalSec = config.pollIntervalSec, logInfo = Some(s"checking storage for instructions")) {
      () => getInstructionFileIterator(hadoopPath).nonEmpty
    }

    // execute instructions
    val agentController: AgentServerController = AgentServerController(sdlb)
    getInstructionFileIterator(hadoopPath).foreach {
      instructionFile =>
        val instructionFilenamePattern = s"^(.+)-${FileType.Instruction}.json$$".r
        val instructionFilenamePattern(instructionId) = instructionFile.getName
        val resultFile = getFilename(hadoopPath, instructionId, FileType.Result)
        val instructionDoneFile = getFilename(hadoopPath, instructionId, FileType.InstructionDone)
        val logFile = getFilename(hadoopPath, instructionId, FileType.Log)
        logger.info(s"Processing instruction $instructionId from file $instructionFile")
        HdfsUtil.writeHadoopFile(logFile, LocalDateTime.now.format(DateTimeFormatter.ISO_DATE_TIME) + " Start processing")

        try {
          val instructionStr = HdfsUtil.readHadoopFile(instructionFile)
          val message = SDLMessage.fromJson(instructionStr)
          assert(message.agentInstruction.isDefined, s"Message must contain an agent instruction")

          // process instruction
          val sdlbConfig = SmartDataLakeBuilderConfig(config.feedSel, applicationName = config.applicationName, configuration = config.configuration,
            partitionValues = config.partitionValues,
            parallelism = config.parallelism, statePath = config.statePath,
            test = config.test, streaming = config.streaming)
          val resultMessage = agentController.handle(message, sdlbConfig)
            .getOrElse(throw new IllegalStateException("No result message received from instruction processing"))

          // write result
          HdfsUtil.writeHadoopFile(resultFile, resultMessage.toJson)
          logger.info(s"Finished processing instruction $instructionId, result written to $resultFile")
        } catch {
          case ex: Exception =>
            logger.error(s"Error processing instruction $instructionId: ${ex.getClass.getSimpleName}: ${ex.getMessage}", ex)
            HdfsUtil.appendHadoopFile(logFile, LocalDateTime.now.format(DateTimeFormatter.ISO_DATE_TIME) + s"Error processing instruction: ${ex.getClass.getSimpleName}: ${ex.getMessage}")
            val errorResult = SDLMessage(msgType = SDLMessageType.AgentResult, log = Some(ActionLog(level = "ERROR", timestamp = LocalDateTime.now(), message = s"Error processing instruction: ${ex.getClass.getSimpleName}: ${ex.getMessage}")))
            HdfsUtil.writeHadoopFile(resultFile, errorResult.toJson)
        } finally {
          // mark instruction as done
          HdfsUtil.renamePath(instructionFile, instructionDoneFile)
        }
    }
    true
  }

  def getInstructionFileIterator(hadoopPath: Path)(implicit filesystem: FileSystem): Iterator[Path] = {
    RemoteIteratorWrapper(filesystem.listStatusIterator(hadoopPath))
      .map(_.getPath)
      .filter(_.getName.endsWith(FileType.Instruction + ".json"))
  }
}

object StorageAgentServer {

  def getFilename(hadoopPath: Path, instructionId: String, fileType: FileType): Path = {
    new Path(hadoopPath, s"$instructionId-$fileType.json")
  }

  object FileType extends Enumeration {
    type FileType = Value
    val Instruction, Log, Result, InstructionDone = Value
  }
}
