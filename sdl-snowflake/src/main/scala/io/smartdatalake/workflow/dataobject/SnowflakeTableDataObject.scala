/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import com.snowflake.snowpark.Session
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.smartdatalake.{SnowparkDataFrame, SnowparkStructType, SparkDataFrame, SparkStructType}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SnowflakeConnection
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

/**
 * [[DataObject]] of type SnowflakeTableDataObject.
 * Provides details to access Snowflake tables via an action
 * Can be used both for interacting with Snowflake through Spark with JDBC,
 * as well as for actions written in the Snowpark API that run directly on Snowflake
 *
 * @param id           unique name of this data object
 * @param table        Snowflake table to be written by this output
 * @param saveMode     spark [[SDLSaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId The SnowflakeTableConnection to use for the table
 * @param comment      An optional comment to add to the table after writing a DataFrame to it
 * @param metadata     meta data
 */
case class SnowflakeTableDataObject(override val id: DataObjectId,
                                    override var table: Table,
                                    override val schemaMin: Option[SparkStructType] = None,
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    connectionId: ConnectionId,
                                    comment: Option[String],
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject
    with CanCreateSnowparkDataFrame
    with CanWriteSnowparkDataFrame {

  private val connection = getConnection[SnowflakeConnection](connectionId)
  private var _snowparkSession: Option[Session] = None

  def session: Session = {
    if (_snowparkSession.isEmpty) {
      _snowparkSession = Some(connection.getSnowparkSession(table.db.get))
    }
    _snowparkSession.get
  }

  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) A SnowFlake schema name must be added as the 'db' parameter of a SnowflakeTableDataObject.")
  }

  // Get a Spark DataFrame with the table contents for Spark transformations
  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): SparkDataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query", q)).getOrElse("dbtable" -> (connection.database + "." + table.fullName)))
    val df = context.sparkSession
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(queryOrTable)
      .load()
    df.colNamesLowercase
  }

  // Write a Spark DataFrame to the Snowflake table
  override def writeDataFrame(df: SparkDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): Unit = {
    validateSchemaMin(df, role = "write")
    val finalSaveMode: SDLSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(Map("dbtable" -> (connection.database + "." + table.fullName)))
      .mode(finalSaveMode.asSparkSaveMode)
      .save()

    if (comment != null && comment.isDefined) {
      val sql = s"comment on table ${connection.database}.${table.fullName} is '$comment';"
      connection.execSnowflakeStatement(sql)
    }
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW DATABASES LIKE '${connection.database}'"
    connection.execSnowflakeStatement(sql).next()
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW TABLES LIKE '${table.name}' IN SCHEMA ${connection.database}.${table.db.get}"
    connection.execSnowflakeStatement(sql).next()
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = throw new NotImplementedError()

  override def factory: FromConfigFactory[DataObject] = SnowflakeTableDataObject

  // Read the contents of a table as a Snowpark DataFrame
  override def getSnowparkDataFrame()(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    this.session.table(table.fullName)
  }

  // Write a Snowpark DataFrame to Snowflake, used in Snowpark actions
  def writeSnowparkDataFrame(df: SnowparkDataFrame, isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                            (implicit context: ActionPipelineContext): Unit = {
    df.write.saveAsTable(table.fullName)
  }
}

object SnowflakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)
                         (implicit instanceRegistry: InstanceRegistry): SnowflakeTableDataObject = {
    extract[SnowflakeTableDataObject](config)
  }
}

private[smartdatalake] trait CanCreateSnowparkDataFrame {
  def getSnowparkDataFrame()(implicit context: ActionPipelineContext): SnowparkDataFrame
}

private[smartdatalake] trait CanWriteSnowparkDataFrame {
  def writeSnowparkDataFrame(df: SnowparkDataFrame,
                             isRecursiveInput: Boolean = false,
                             saveModeOptions: Option[SaveModeOptions] = None)
                            (implicit context: ActionPipelineContext): Unit
}
