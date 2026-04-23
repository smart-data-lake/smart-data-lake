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
package io.smartdatalake.communication.agent

import io.smartdatalake.app.{GlobalConfig, LocalStorageAgentSmartDataLakeBuilderConfig, SmartDataLakeBuilder}
import io.smartdatalake.communication.agent.StorageAgentServer.FileType.FileType
import io.smartdatalake.communication.agent.StorageAgentServer.{FileType, getFilename}
import io.smartdatalake.communication.message.{ActionLog, SDLMessage, SDLMessageType}
import io.smartdatalake.config.ConfigParser.{getConnectionConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.{SmartDataLakeLogger, WaitUtil}
import io.smartdatalake.workflow.connection.Connection
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class StorageAgentServer(sdlb: SmartDataLakeBuilder, agentConfig: LocalStorageAgentSmartDataLakeBuilderConfig) extends SmartDataLakeLogger {

  private val startTime = System.currentTimeMillis() / 1000

  private implicit val dummyInstanceRegistry: InstanceRegistry = new InstanceRegistry()
  private val localConfig = agentConfig.getHoconConfig(validateCompletness = false)(new Configuration())
  private val localConnections = getConnectionConfigMap(localConfig)
    .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }
  private val sdlbGlobalConfig = GlobalConfig.from(localConfig)
  implicit val hadoopConfiguration: Configuration = sdlbGlobalConfig.getHadoopConfiguration

  def pollForInstructions(): Boolean = {
    val hadoopPath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(agentConfig.path))
    implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithConf(hadoopPath)
    filesystem.mkdirs(hadoopPath)

    logger.info(s"Polling for instructions every ${agentConfig.pollIntervalSec} seconds in ${agentConfig.path}")
    val secondsPolled = (System.currentTimeMillis() / 1000 - startTime).toInt
    if (agentConfig.stopAfterSec.exists(secondsPolled > _)) {
      logger.info(s"Agent is going to stop now, as it has been running for $secondsPolled seconds")
      return false // dont poll again
    }
    WaitUtil.sleepUntil(timeoutSec = agentConfig.stopAfterSec.map(_ - secondsPolled), pollIntervalSec = agentConfig.pollIntervalSec, logInfo = Some(s"checking storage for instructions")) {
      () => getInstructionFileIterator(hadoopPath).nonEmpty
    }
    Thread.sleep(100L) // wait some time to ensure that the result file is completely written (unit tests might fail on github otherwise)

    // execute instructions
    val agentController = AgentServerController(sdlb, localConnections)
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
          logger.info(s"Instruction $instructionId: ${instructionStr.take(100)}")
          val message = SDLMessage.fromJson(instructionStr)
          assert(message.agentInstruction.isDefined, s"Message must contain an agent instruction")

          // process instruction
          val resultMessage = agentController.handle(message, agentConfig)
            .getOrElse(throw new IllegalStateException("No result message received from instruction processing"))

          // write result
          val resultStr = resultMessage.toJson
          HdfsUtil.writeHadoopFile(resultFile, resultMessage.toJson)
          logger.info(s"Finished processing instruction $instructionId, result written to $resultFile: ${resultStr.take(100)}")
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
    true // do poll again
  }

  def getInstructionFileIterator(hadoopPath: Path)(implicit filesystem: FileSystem): Iterator[Path] = {
    RemoteIteratorWrapper(filesystem.listStatusIterator(hadoopPath))
      .map(_.getPath)
      .filter(_.getName.endsWith(FileType.Instruction.toString + ".json"))
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
