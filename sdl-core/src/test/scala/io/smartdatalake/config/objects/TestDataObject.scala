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
package io.smartdatalake.config.objects

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A dummy [[DataObject]] for unit tests.
 *
 * @param id The unique identified of this object.
 * @param arg1 some dummy argument
 * @param args more dummy arguments
 */
case class TestDataObject( id: DataObjectId,
                           override val schemaMin: Option[StructType] = None,
                           arg1: String,
                           args: Seq[String],
                           connectionId: Option[ConnectionId] = None,
                           override val metadata: Option[DataObjectMetadata] = None)
                         ( implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with TransactionalSparkTableDataObject {

  private val connection = connectionId.map( c => getConnection[TestConnection](c))

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = null

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false)
                             (implicit session: SparkSession, context: ActionPipelineContext): Unit = {}

  override var table: Table = Table(db=Some("testdb"), name="testtable")

  override def isDbExisting(implicit session: SparkSession): Boolean = true

  override def isTableExisting(implicit session: SparkSession): Boolean = true

  override def dropTable(implicit session: SparkSession): Unit = throw new NotImplementedError()

  override def factory: FromConfigFactory[DataObject] = TestDataObject
}

object TestDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): TestDataObject = {
    extract[TestDataObject](config)
  }
}