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
package io.smartdatalake.workflow.dataobject

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.jms.{JmsQueueConsumerFactory, SynchronousJmsReceiver, TextMessageHandler}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.Duration

/**
 * [[DataObject]] of type JMS queue.
 * Provides details to an Action to access JMS queues.
 *
 * @param jndiContextFactory JNDI Context Factory
 * @param jndiProviderUrl JNDI Provider URL
 * @param authMode authentication information: for now BasicAuthMode is supported.
 * @param batchSize JMS batch size
 * @param connectionFactory JMS Connection Factory
 * @param queue Name of MQ Queue
 */
case class JmsDataObject(override val id: DataObjectId,
                         jndiContextFactory: String,
                         jndiProviderUrl: String,
                         override val schemaMin: Option[StructType],
                         authMode: AuthMode,
                         batchSize: Int,
                         maxWaitSec: Int,
                         maxBatchAgeSec: Int,
                         txBatchSize: Int,
                         connectionFactory: String,
                         queue: String,
                         override val metadata: Option[DataObjectMetadata] = None)
                        (implicit instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with SchemaValidation {

  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(supportedAuths.contains(authMode.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")
  val basicAuthMode = authMode.asInstanceOf[BasicAuthMode]

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {
    val consumerFactory = new JmsQueueConsumerFactory(jndiContextFactory, jndiProviderUrl, basicAuthMode.user, basicAuthMode.password, connectionFactory, queue)
    val receiver = new SynchronousJmsReceiver[String](consumerFactory,
      TextMessageHandler.convert2Text, batchSize, Duration(maxWaitSec, TimeUnit.SECONDS),
      Duration(maxBatchAgeSec, TimeUnit.SECONDS), txBatchSize, session)

    val df = receiver.receiveMessages().getOrElse(session.emptyDataFrame)
    validateSchemaMin(df, "read")
    df
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = JmsDataObject
}

object JmsDataObject extends FromConfigFactory[DataObject] {
  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JmsDataObject = {
    extract[JmsDataObject](config)
  }
}
