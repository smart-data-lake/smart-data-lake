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

import java.sql.ResultSet

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.connection.JdbcTableConnection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * [[DataObject]] of type JDBC.
 * Provides details for an action to access tables in a database through JDBC.
 * @param id unique name of this data object
 * @param createSql DDL-statement to be executed in prepare phase
 * @param preSql SQL-statement to be executed before writing to table
 * @param postSql SQL-statement to be executed after writing to table
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param table The jdbc table to be read
 * @param jdbcFetchSize Number of rows to be fetched together by the Jdbc driver
 * @param connectionId Id of JdbcConnection configuration
 * @param jdbcOptions Any jdbc options according to [[https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html]].
 *                    Note that some options above set and override some of this options explicitly.
 */
case class JdbcTableDataObject(override val id: DataObjectId,
                               createSql: Option[String] = None,
                               preSql: Option[String] = None,
                               postSql: Option[String] = None,
                               override val schemaMin: Option[StructType] = None,
                               override var table: Table,
                               jdbcFetchSize: Int = 1000,
                               connectionId: ConnectionId,
                               jdbcOptions: Map[String, String] = Map(),
                               override val metadata: Option[DataObjectMetadata] = None
                              )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject {

  /**
   * Connection defines driver, url and db in central location
   */
  private val connection = getConnection[JdbcTableConnection](connectionId)

  // prepare final table
  table = table.overrideDb(connection.db)
  if(table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  override def prepare(implicit session: SparkSession): Unit = {
    if (!isTableExisting) {
      createSql.foreach{ sql =>
        logger.info(s"($id) createSQL is being executed")
        connection.execJdbcStatement(sql)
      }
      assert(isTableExisting, s"($id) Table ${table.fullName} doesn't exist. Define createSQL to create table automatically.")
    }
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query",q)).getOrElse(("dbtable"->table.fullName)))
    val df = session.read.format("jdbc")
      .options(jdbcOptions)
      .options(
        Map("url" -> connection.url,
          "driver" -> connection.driver,
          "fetchSize" -> jdbcFetchSize.toString))
      .options(connection.getAuthModeSparkOptions)
      .options(queryOrTable)
      .load()
    validateSchemaMin(df)
    df.colNamesLowercase
  }

  override def preWrite(implicit session: SparkSession): Unit = {
    super.preWrite
    preSql.foreach { sql =>
      logger.info(s"($id) preSQL is being executed")
      connection.execJdbcStatement(sql)
    }
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    require(table.query.isEmpty, s"($id) Cannot write to jdbc DataObject defined by a query.")
    validateSchemaMin(df)
    // write table
    // No need to define any partitions as parallelization will be defined according to the data frame's partitions
    df.write.mode(SaveMode.Append).format("jdbc")
      .options(jdbcOptions)
      .options(Map(
        "url" -> connection.url,
        "driver" -> connection.driver,
        "dbtable" -> s"${table.fullName}"
      ))
      .options(connection.getAuthModeSparkOptions)
      .save
  }

  override def postWrite(implicit session: SparkSession): Unit = {
    super.postWrite
    postSql.foreach{ sql =>
      logger.info(s"($id) postSQL is being executed")
      connection.execJdbcStatement(sql)
    }
  }

  override def isDbExisting(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog =
      if (Environment.enableJdbcCaseSensitivity)
        s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where TABLE_SCHEMA='${table.db.get}'"
      else
        s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where upper(TABLE_SCHEMA)=upper('${table.db.get}')"
    def evalTableExistsInCatalog( rs:ResultSet ) : Boolean = {
      rs.next
      rs.getInt(1) == 1
    }
    connection.execJdbcQuery(cntTableInCatalog, evalTableExistsInCatalog)
  }

  override def isTableExisting(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog =
      if (Environment.enableJdbcCaseSensitivity)
        s"select count(*) from INFORMATION_SCHEMA.TABLES where TABLE_NAME='${table.name}' and TABLE_SCHEMA='${table.db.get}'"
      else
        s"select count(*) from INFORMATION_SCHEMA.TABLES where upper(TABLE_NAME)=upper('${table.name}') and upper(TABLE_SCHEMA)=upper('${table.db.get}')"
    def evalTableExistsInCatalog( rs:ResultSet ) : Boolean = {
      rs.next
      rs.getInt(1)==1
    }
    connection.execJdbcQuery( cntTableInCatalog, evalTableExistsInCatalog )
  }

  override def dropTable(implicit session: SparkSession): Unit = {
    connection.execJdbcStatement(s"drop table if exists ${table.fullName}")
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = JdbcTableDataObject

}

object JdbcTableDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): JdbcTableDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[JdbcTableDataObject].value
  }
}
