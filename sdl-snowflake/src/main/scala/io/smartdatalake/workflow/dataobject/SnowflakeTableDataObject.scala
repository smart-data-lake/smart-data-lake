/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SnowflakeTableConnection
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * [[DataObject]] of type SnowflakeTableDataObject.
 * Provides details to access Snowflake tables via an action
 *
 * @param id           unique name of this data object
 * @param schemaMin    An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param table        Snowflake table to be written by this output
 * @param saveMode     spark [[SaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.SnowflakeTableConnection]]
 * @param metadata     meta data
 */
case class SnowflakeTableDataObject(override val id: DataObjectId,
                                    override val schemaMin: Option[StructType] = None,
                                    override var table: Table,
                                    saveMode: SaveMode = SaveMode.Overwrite,
                                    connectionId: Option[ConnectionId] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject {

  /**
   * Connection defines connection string, credentials and database schema/name
   */
  private val connection = connectionId.map(c => getConnection[SnowflakeTableConnection](c))
  assert(connection.isDefined, "A SnowflakeTableDataObject needs to have an assigned SnowflakeTableConnection.")

  // prepare table
  table = table.overrideDb(connection.map(_.db))
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query", q)).getOrElse("dbtable" -> (connection.get.schema + "." + table.fullName)))
    val df = session
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.get.getSnowflakeOptions)
      .options(queryOrTable)
      .load()
    validateSchemaMin(df)
    df.colNamesLowercase
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false)
                             (implicit session: SparkSession): Unit = {
    validateSchemaMin(df)
    writeDataFrame(df, createTableOnly = false, partitionValues)
  }

  /**
   * Writes DataFrame to Snowflake
   * Snowflake does not support explicit partitions, so any passed partition values are ignored
   */
  def writeDataFrame(df: DataFrame, createTableOnly: Boolean, partitionValues: Seq[PartitionValues])
                    (implicit session: SparkSession): Unit = {
    val dfPrepared = if (createTableOnly) {
      DataFrameUtil.getEmptyDataFrame(df.schema)
    } else {
      df
    }

    dfPrepared.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.get.getSnowflakeOptions)
      .options(Map("dbtable" -> (connection.get.schema + "." + table.fullName)))
      .mode(saveMode)
      .save()
  }

  override def isDbExisting(implicit session: SparkSession): Boolean =
    connection.map(connection => {
      val sql = s"SHOW DATABASES LIKE '${connection.db}'"
      connection.execSnowflakeStatement(sql)
    }).exists(resultSet => resultSet.next())


  override def isTableExisting(implicit session: SparkSession): Boolean = {
    connection.map(connection => {
      val sql = s"SHOW TABLES LIKE '${table.name}' IN SCHEMA ${connection.schema}.${connection.db}"
      connection.execSnowflakeStatement(sql)
    }).exists(resultSet => resultSet.next())
  }

  override def dropTable(implicit session: SparkSession): Unit = throw new NotImplementedError()

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = SnowflakeTableDataObject
}

object SnowflakeTableDataObject extends FromConfigFactory[DataObject] {
  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SnowflakeTableDataObject = {
    extract[SnowflakeTableDataObject](config)
  }
}



