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

import java.sql.{ResultSet, ResultSetMetaData}

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.misc.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.JdbcTableConnection
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * [[DataObject]] of type JDBC.
 * Provides details for an action to access tables in a database through JDBC.
 * @param id unique name of this data object
 * @param createSql DDL-statement to be executed in prepare phase, using output jdbc connection
 * @param preReadSql SQL-statement to be executed in exec phase before reading input table, using input jdbc connection.
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param postReadSql SQL-statement to be executed in exec phase after reading input table and before action is finished, using input jdbc connection
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param preWriteSql SQL-statement to be executed in exec phase before writing output table, using output jdbc connection
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param postWriteSql SQL-statement to be executed in exec phase after writing output table, using output jdbc connection
 *                   Use tokens with syntax %{<spark sql expression>} to substitute with values from [[DefaultExpressionData]].
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param saveMode [[SDLSaveMode]] to use when writing table, default is "Overwrite". Only "Append" and "Overwrite" supported.
 * @param table The jdbc table to be read
 * @param jdbcFetchSize Number of rows to be fetched together by the Jdbc driver
 * @param connectionId Id of JdbcConnection configuration
 * @param jdbcOptions Any jdbc options according to [[https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html]].
 *                    Note that some options above set and override some of this options explicitly.
 * @param virtualPartitions Virtual partition columns. Note that this doesn't need to be the same as the database partition
 *                   columns for this table. But it is important that there is an index on these columns to efficiently
 *                   list existing "partitions".
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 */
case class JdbcTableDataObject(override val id: DataObjectId,
                               createSql: Option[String] = None,
                               preReadSql: Option[String] = None,
                               postReadSql: Option[String] = None,
                               preWriteSql: Option[String] = None,
                               postWriteSql: Option[String] = None,
                               override val schemaMin: Option[StructType] = None,
                               override var table: Table,
                               jdbcFetchSize: Int = 1000,
                               saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                               connectionId: ConnectionId,
                               jdbcOptions: Map[String, String] = Map(),
                               virtualPartitions: Seq[String] = Seq(),
                               override val expectedPartitionsCondition: Option[String] = None,
                               override val metadata: Option[DataObjectMetadata] = None
                              )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject with CanHandlePartitions {

  /**
   * Connection defines driver, url and db in central location
   */
  @DeveloperApi
  val connection: JdbcTableConnection = getConnection[JdbcTableConnection](connectionId)

  // Define partition columns
  // Virtual partition column name might be quoted to force case sensitivity in database queries
  override val partitions: Seq[String] = virtualPartitions.map(prepareCaseSensitiveName).map(_.name)

  // prepare final table
  table = table.overrideDb(connection.db)
  if(table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  assert(saveMode==SDLSaveMode.Append || saveMode==SDLSaveMode.Overwrite, s"($id) Only saveMode Append and Overwrite are supported.")

  // jdbc column metadata
  lazy val columns = getJdbcColumnMetadata

  override def prepare(implicit session: SparkSession): Unit = {

    // test connection
    try {
      connection.test()
    } catch {
      case ex: Throwable => throw ConnectionTestException(s"($id) Can not connect. Error: ${ex.getMessage}", ex)
    }

    // test table existing
    if (!isTableExisting) {
      createSql.foreach{ sql =>
        logger.info(s"($id) createSQL is being executed")
        connection.execJdbcStatement(sql)
      }
      assert(isTableExisting, s"($id) Table ${table.fullName} doesn't exist. Define createSQL to create table automatically.")
    }

    // test partition columns exist
    if (virtualPartitions.nonEmpty) {
      val missingPartitionColumns = virtualPartitions.map(prepareCaseSensitiveName)
        .filterNot( partition => columns.exists(c => partition.nameEquals(c)))
        .map(_.name)
      assert(missingPartitionColumns.isEmpty, s"($id) Virtual partition columns ${missingPartitionColumns.mkString(",")} missing in table definition")
    }
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query",q)).getOrElse("dbtable"->table.fullName))
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

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false)(implicit session: SparkSession): Unit = {
    require(table.query.isEmpty, s"($id) Cannot write to jdbc DataObject defined by a query.")
    validateSchemaMin(df)

    // validate columns exists
    val dfWrite = if (isTableExisting) {
      val dfColumns = df.columns.map(c => JdbcColumn(c, isNameCaseSensitiv = false))
      val colsMissingInTable = dfColumns.filter(c => !columns.exists(c.nameEquals))
      assert(colsMissingInTable.isEmpty, s"Columns ${colsMissingInTable.mkString(", ")} missing in $id")
      val colsMissingInDataFrame = columns.filter(c => !dfColumns.exists(c.nameEquals))
      assert(colsMissingInTable.isEmpty, s"Columns ${colsMissingInDataFrame.mkString(", ")} exist in $id but not in DataFrame")

      // cleanup existing data if saveMode=overwrite
      if (saveMode == SDLSaveMode.Overwrite) {
        if (partitionValues.nonEmpty) deletePartitions(partitionValues)
        else deleteAllData
      }

      // order DataFrame columns according to table metadata
      df.select(columns.map(c => col(c.name)): _*)
    } else df

    // write table
    // No need to define any partitions as parallelization will be defined according to the data frame's partitions
    dfWrite.write.mode(SaveMode.Append).format("jdbc")
      .options(jdbcOptions)
      .options(Map(
        "url" -> connection.url,
        "driver" -> connection.driver,
        "dbtable" -> s"${table.fullName}"
      ))
      .options(connection.getAuthModeSparkOptions)
      .save
  }

  override def preRead(partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.preRead(partitionValues)
    preparedAndExecSql(preReadSql, Some("preReadSql"), partitionValues)
  }
  override def postRead(partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.postRead(partitionValues)
    preparedAndExecSql(postReadSql, Some("postReadSql"), partitionValues)
  }
  override def preWrite(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.preWrite
    preparedAndExecSql(preWriteSql, Some("preWriteSql"), Seq()) // no partition values here...
  }
  override def postWrite(partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.postWrite(partitionValues)
    preparedAndExecSql(postWriteSql, Some("postWriteSql"), partitionValues)
  }
  private def preparedAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    sqlOpt.foreach { sql =>
      val data = DefaultExpressionData.from(context, partitionValues)
      val preparedSql = SparkExpressionUtil.substitute(id, configName, sql, data)
      logger.info(s"($id) ${configName.getOrElse("SQL")} is being executed: $preparedSql")
      connection.execJdbcStatement(preparedSql, logging = false)
    }
  }

  override def isDbExisting(implicit session: SparkSession): Boolean = connection.catalog.isDbExisting(table.db.get)
  override def isTableExisting(implicit session: SparkSession): Boolean = connection.catalog.isTableExisting(table.db.get, table.name)

  def deleteAllData(implicit session: SparkSession): Unit = {
    connection.execJdbcStatement(s"delete from ${table.fullName}")
  }

  override def dropTable(implicit session: SparkSession): Unit = {
    connection.execJdbcStatement(s"drop table if exists ${table.fullName}")
  }

  override def factory: FromConfigFactory[DataObject] = JdbcTableDataObject

  /**
   * Listing virtual partitions by a "select distinct partition-columns" query
   */
  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = {
    if (partitions.nonEmpty) {
      val tableClause = table.query.map( q => s"($q)").getOrElse(table.fullName)
      val partitionListQuery = s"select distinct ${virtualPartitions.mkString(", ")} from $tableClause"
      val partitionsWithIdx = partitions.zipWithIndex
      def evalPartitions(rs: ResultSet): Seq[PartitionValues] = {
        Iterator.continually(rs.next).takeWhile(identity).map {
          _ => PartitionValues(partitionsWithIdx.map{ case (col,idx) => col -> rs.getObject(idx+1)}.toMap)
        }.toVector
      }
      connection.execJdbcQuery(partitionListQuery, evalPartitions)
    } else Seq()
  }

  /**
   * Delete virtual partitions by "delete from" statement
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    if (partitionValues.nonEmpty) {
      val partitionsColss = partitionValues.map(_.keys).distinct
      assert(partitionsColss.size == 1, "All partition values must have the same set of partition columns defined!")
      val partitionCols = partitionsColss.head
      val deletePartitionQuery = if (partitionCols.size == 1) {
        s"delete from ${table.fullName} where ${partitionCols.head} in ('${partitionValues.map(pv => pv(partitionCols.head)).mkString("','")}')"
      } else {
        val partitionValuesStr = partitionValues.map(pv => s"('${partitionCols.map(pv(_).toString).mkString("','")}')")
        s"delete from ${table.fullName} where (${partitionCols.mkString(",")}) in (${partitionValuesStr.mkString(",")})"
      }
      connection.execJdbcStatement(deletePartitionQuery)
    }
  }

  private def prepareCaseSensitiveName(name: String): JdbcColumn= {
    val caseSensitiveNamePattern = "^[\"'](.*)[\"']$".r
    name match {
      case caseSensitiveNamePattern(nameOnly) => JdbcColumn(nameOnly, isNameCaseSensitiv = true)
      case _ => JdbcColumn(name, isNameCaseSensitiv = false)
    }
  }

  def getJdbcColumnMetadata: Seq[JdbcColumn] = {
    val metadataQuery = table.query.getOrElse(s"select * from ${table.fullName}") + " where 1=0"
    def evalColumnNames(rs: ResultSet): Seq[JdbcColumn] = {
      (1 to rs.getMetaData.getColumnCount).map( i => JdbcColumn.from(rs.getMetaData, i))
    }
    connection.execJdbcQuery(metadataQuery, evalColumnNames)
  }
}

private[smartdatalake] case class JdbcColumn(name: String, isNameCaseSensitiv: Boolean, jdbcType: Option[Int] = None, dbTypeName: Option[String] = None, precision: Option[Int] = None, scale: Option[Int] = None) {
  def nameEquals(other: JdbcColumn): Boolean = {
    if (this.isNameCaseSensitiv || other.isNameCaseSensitiv) this.name.equals(other.name)
    else this.name.equalsIgnoreCase(other.name)
  }
}
private[smartdatalake] object JdbcColumn {
  def from(metadata: ResultSetMetaData, colIdx: Int): JdbcColumn = {
    val name = metadata.getColumnName(colIdx)
    val isNameCaseSensitiv = name != name.toUpperCase
    JdbcColumn(name, isNameCaseSensitiv, Option(metadata.getColumnType(colIdx)), Option(metadata.getColumnTypeName(colIdx)), Option(metadata.getPrecision(colIdx)), Option(metadata.getScale(colIdx)))
  }
}

object JdbcTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableDataObject = {
    extract[JdbcTableDataObject](config)
  }
}
