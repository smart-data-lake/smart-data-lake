//
// ========================================================================
// Copyright (c) Mort Bay Consulting Pty Ltd and others.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License v. 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
// ========================================================================
//

package io.smartdatalake.communication.agent

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.communication.statusinfo.websocket.SDLMessageType.EndConnection
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.DataObject
import org.eclipse.jetty.websocket.api.{Session, StatusCode, WebSocketAdapter}

import java.time.LocalDateTime

class AgentServerSocket(config: AgentServerConfig, agentController: AgentController) extends WebSocketAdapter with SmartDataLakeLogger {

  override def onWebSocketConnect(sess: Session): Unit = {

    super.onWebSocketConnect(sess)
    logger.info(s"Socket $this Connected")
    sess.getRemote.sendString("Hello from " + this)
    sess.getPolicy.setMaxTextMessageBufferSize(1000000)
  }


  override def onWebSocketText(message: String): Unit = {
    super.onWebSocketText(message)
    logger.info("Received TEXT message: " + message)
    implicit val instanceRegistry: InstanceRegistry = agentController.instanceRegistry
    val agentConnectionIds = instanceRegistry.getConnections.map(_.id.id)

    val configFromString = ConfigFactory.parseString(message, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

    val connectionsToRegister: Map[ConnectionId, Connection] = getConnectionConfigMap(configFromString)
      //Connections defined by the agent should not get overwritten by the connections in the instructions
      .filterNot { case (id, _) => agentConnectionIds.contains(id) }
      .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }

    instanceRegistry.register(connectionsToRegister)

    val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
      .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }
    instanceRegistry.register(dataObjects)

    val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
      .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }

    instanceRegistry.register(actions)

    //TODO replace here config.sdlConfig.test with dry run for init phase
    agentController.sdlb.exec(config.sdlConfig, SDLExecutionId.executionId1, LocalDateTime.now(), LocalDateTime.now(), Map(), Seq(), Seq(), None, Seq(), simulation = false, globalConfig = GlobalConfig())(agentController.instanceRegistry)

    getSession.getRemote.sendString(EndConnection.toString)

    if (message.contains(EndConnection)) {
      logger.info(this + ": received EndConnection request, closing connection")
      getSession.close(StatusCode.NORMAL, "Connection closed by " + this)
    }
  }

  override def onWebSocketClose(statusCode: Int, reason: String): Unit = {
    super.onWebSocketClose(statusCode, reason)
    logger.info("Server says: Socket Closed: [" + statusCode + "] " + reason)

  }

  override def onWebSocketError(cause: Throwable): Unit = {
    super.onWebSocketError(cause)
    logger.error(s"Socket $this was closed with error ${cause.printStackTrace(System.err)}")
  }

  override def onWebSocketBinary(payload: Array[Byte], offset: Int, len: Int): Unit = {

  }
}
