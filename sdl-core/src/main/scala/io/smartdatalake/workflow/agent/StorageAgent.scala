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

package io.smartdatalake.workflow.agent

import com.typesafe.config.Config
import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.communication.agent.StorageAgentServer.{FileType, getFilename}
import io.smartdatalake.communication.message.SDLMessage
import io.smartdatalake.config.SdlConfigObject.AgentId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.{SmartDataLakeLogger, WaitUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.Connection
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * An SDLB Remote Agent that communicates through Hadoop storage (e.g. HDFS, S3, Azure Blob Storage, ...)
 *
 * @param path            Hadoop path where the agent reads instructions from and writes result information to
 * @param startTimeoutSec maximum time to wait for the start of the processing by the agent in seconds (default: 300s)
 * @param execTimeoutSec  maximum time to wait for the execution result in seconds (default: 300s)
 */
case class StorageAgent(override val id: AgentId, path: String, startTimeoutSec: Int = 300, execTimeoutSec: Int = 300, override val connections: Map[String, Connection])
  extends Agent with AgentClient with SmartDataLakeLogger {

  private val hadoopPath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(path))

  override def sendSDLMessage(message: SDLMessage)(implicit context: ActionPipelineContext): Option[SDLMessage] = {
    assert(message.agentInstruction.isDefined, s"($id) Message must contain an agent instruction")
    val instructionId = message.agentInstruction.get.instructionId
    val instructionFile = getFilename(hadoopPath, instructionId, FileType.Instruction)
    val resultFile = getFilename(hadoopPath, instructionId, FileType.Result)
    val logFile = getFilename(hadoopPath, instructionId, FileType.Log)

    logger.info(s"($id) Writing instruction $instructionId to $instructionFile")
    implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithConf(hadoopPath)(context.hadoopConf)
    HdfsUtil.writeHadoopFile(instructionFile, message.toJson)
    WaitUtil.sleepUntil(timeoutSec = Some(startTimeoutSec), logInfo = Some(s"to start $instructionId ($id)")) {
      () => filesystem.exists(logFile)
    }
    WaitUtil.sleepUntil(timeoutSec = Some(execTimeoutSec), logInfo = Some(s"to finish $instructionId ($id)")) {
      () => filesystem.exists(resultFile)
    }
    val resultStr = HdfsUtil.readHadoopFile(resultFile)
    logger.info(s"($id) Received result for $instructionId")
    Some(SDLMessage.fromJson(resultStr))
  }

  override def getClient: AgentClient = this

  override def factory: FromConfigFactory[Agent] = StorageAgent
}

object StorageAgent extends FromConfigFactory[Agent] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): StorageAgent = {
    extract[StorageAgent](config)
  }
}