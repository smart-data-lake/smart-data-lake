/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.workflow.agent

import com.typesafe.config.Config
import io.smartdatalake.communication.agent.AgentClient
import io.smartdatalake.communication.agent.StorageAgentServer.{FileType, getFilename}
import io.smartdatalake.communication.message.{AgentResult, SDLMessage}
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
case class StorageAgent(override val id: AgentId, path: String, startTimeoutSec: Int = 300, execTimeoutSec: Int = 300, override val connections: Map[String, Connection] = Map())
  extends Agent with AgentClient with SmartDataLakeLogger {

  private val hadoopPath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(path))

  override def sendSDLMessage(message: SDLMessage)(implicit context: ActionPipelineContext): AgentResult = {
    assert(message.agentInstruction.isDefined, s"($id) Message must contain an agent instruction")
    val instructionId = message.agentInstruction.get.instructionId
    val instructionFile = getFilename(hadoopPath, instructionId, FileType.Instruction)
    val resultFile = getFilename(hadoopPath, instructionId, FileType.Result)
    val logFile = getFilename(hadoopPath, instructionId, FileType.Log)

    logger.info(s"($id) Writing instruction $instructionId to $instructionFile: ${message.toJson.take(100)}")
    implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithConf(hadoopPath)(context.hadoopConf)
    // only one instruction at a time executed by an agent. Creating multiple instruction files at the same time might cause wait timeout exceptions.
    synchronized {
      HdfsUtil.writeHadoopFile(instructionFile, message.toJson)
      // wait for logfile, indicating that the agent has started processing the instruction
      WaitUtil.sleepUntil(timeoutSec = Some(startTimeoutSec), logInfo = Some(s"to start $instructionId ($id)")) {
        () => filesystem.exists(logFile)
      }
      // wait for result file
      WaitUtil.sleepUntil(timeoutSec = Some(execTimeoutSec), logInfo = Some(s"to finish $instructionId ($id)")) {
        () => filesystem.exists(resultFile)
      }
    }
    Thread.sleep(100L) // wait some time to ensure that the result file is completely written (unit tests might fail on github otherwise)
    val resultStr = HdfsUtil.readHadoopFile(resultFile)
    logger.info(s"($id) Received result for $instructionId: ${resultStr.take(100)}")
    val response = SDLMessage.fromJson(resultStr)
    assert(response.agentResult.isDefined, s"($id) Agent response must be a message of type AgentResult, but received ${response} for instruction $instructionId")
    response.agentResult.get
  }

  override def getClient: AgentClient = this

  override def factory: FromConfigFactory[Agent] = StorageAgent
}

object StorageAgent extends FromConfigFactory[Agent] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): StorageAgent = {
    extract[StorageAgent](config)
  }
}