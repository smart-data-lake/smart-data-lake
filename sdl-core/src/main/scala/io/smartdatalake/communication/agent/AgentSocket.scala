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

import io.smartdatalake.communication.agent.AgentStateEnum.{IDLE, READY}
import io.smartdatalake.communication.statusinfo.websocket.SDLMessageType.EndConnection
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionDAGRunState.formats
import io.smartdatalake.workflow.action.{Action, ActionSubFeedsImpl}
import io.smartdatalake.workflow.{ActionDAGRunState, FileSubFeed}
import org.eclipse.jetty.websocket.api.{Session, StatusCode, WebSocketAdapter}
import org.json4s.jackson.Serialization.read

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.Locale
import scala.reflect.ManifestFactory

class AgentSocket(agentController: AgentController.type) extends WebSocketAdapter with SmartDataLakeLogger {

  override def onWebSocketConnect(sess: Session): Unit = {

    if (agentController.state == IDLE) {
      super.onWebSocketConnect(sess)
      logger.info(s"Socket $this Connected")
      sess.getRemote.sendString("Hello from " + this)
      sess.getPolicy.setMaxBinaryMessageSize(1000000)
      agentController.state = READY
    } else {
      logger.error("State of agent was not idle while trying to connect, but was: " + agentController.state)
    }
  }


  override def onWebSocketText(message: String): Unit = {
    super.onWebSocketText(message)
    logger.info("Received TEXT message: " + message)

    import org.json4s.jackson.JsonMethods._

    val blub = parse(message).extract[ActionSubFeedsImpl[FileSubFeed]]

    val c = Class.forName("io.smartdatalake.workflow.action.Action")
    val manifest: Manifest[Action] = ManifestFactory.classType(c)
    val result = read[Action](message)(ActionDAGRunState.formats, manifest)

    if (message.toLowerCase(Locale.US).contains(EndConnection)) {
      getSession.close(StatusCode.NORMAL, "Connection closed by client")
    }
  }

  override def onWebSocketClose(statusCode: Int, reason: String): Unit = {
    super.onWebSocketClose(statusCode, reason)
    logger.info("Socket Closed: [" + statusCode + "] " + reason)

  }

  override def onWebSocketError(cause: Throwable): Unit = {
    super.onWebSocketError(cause)
    logger.error(s"Socket $this was closed with error ${cause.printStackTrace(System.err)}")
  }

  override def onWebSocketBinary(payload: Array[Byte], offset: Int, len: Int): Unit = {
    def deserialise(bytes: Array[Byte]): Any = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = ois.readObject
      ois.close()
      value
    }

    if (agentController.state == READY) {

      val instanceRegistryDes = deserialise(payload).asInstanceOf[InstanceRegistry]
      agentController.instanceRegistry = instanceRegistryDes
      println(agentController.instanceRegistry.getActions.toSet)
    }
    else {
      logger.error("State of agent was not READY while trying to receive, but was : " + agentController.state)
    }
  }
}
