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
package io.smartdatalake.util.jms

import java.util

import io.smartdatalake.util.misc.SmartDataLakeLogger
import javax.jms._
import javax.naming.{Context, InitialContext}

import scala.collection.JavaConverters._

/**
 * JMS Queue Consumer Factory
 *
 * @param contextFactory JNDI Context Factory
 * @param url JNDI Provider URL
 * @param user JNDI User
 * @param password JNDI Password
 * @param connectionFactoryName Name of JMS connection factory
 * @param queueName Name of JMS Queue
 */
private[smartdatalake] class JmsQueueConsumerFactory(private val contextFactory: String, private val url: String,
                             private val user: String, private val password: String,
                             private val connectionFactoryName: String, private val queueName: String)
extends MessageConsumerFactory with SmartDataLakeLogger
{

  private val properties = new util.Hashtable[String, String]
  properties.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory)
  properties.put(Context.PROVIDER_URL, url)
  properties.put(Context.SECURITY_PRINCIPAL, user)
  properties.put(Context.SECURITY_CREDENTIALS, password)

  /*
  Workaround to make it usable in util executors (circumvent serializable issues)
   */
  private object InitialContextHolder {
    def getInitialContext(properties: util.Hashtable[String, String]): InitialContext = {
      val ctx = new InitialContext(properties)
      ctx
    }
  }

  /**
   * @inheritdoc
   */
  override def makeConnection: Connection = {
    logger.debug(s"${this.getClass.getSimpleName}.makeConnection called.")
    val ctx = InitialContextHolder.getInitialContext(properties)
    val propsContent =
      properties.entrySet().asScala.map(x => (s"${x.getKey} -> ${x.getValue}")).mkString(",")
    logger.debug(s"InitialContext from properties: ${propsContent}")
    logger.debug(s"InitialContext.lookup(${connectionFactoryName})")
    val connectionFactory: ConnectionFactory =
      ctx.lookup(connectionFactoryName).asInstanceOf[ConnectionFactory]
    logger.debug(s"JMS ConnectionFactory fetched: ${connectionFactory}")
    val connection: Connection = connectionFactory.createConnection()
    logger.debug(s"JMS Connection fetched: ${connection}")
    connection
  }

  /**
   * @inheritdoc
   */
  override def makeConsumer(session: Session): MessageConsumer = {
    logger.debug(s"${this.getClass.getSimpleName}.makeConsumer called.")
    val ctx = InitialContextHolder.getInitialContext(properties)
    val queue: Queue = (ctx.lookup(queueName)).asInstanceOf[Queue]
    logger.debug(s"JMS Queue: ${queue}")
    val consumer = session.createConsumer(queue)
    logger.debug(s"JMS MessageConsumer: ${consumer}")
    consumer
  }
}



