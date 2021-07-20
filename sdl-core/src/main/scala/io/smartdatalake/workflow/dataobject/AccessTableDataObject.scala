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

import java.io.File
import java.sql.Timestamp

import com.healthmarketscience.jackcess.DatabaseBuilder
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

/**
 * [[DataObject]] of type JDBC / Access.
 * Provides access to a Access DB to an Action. The functionality is handled seperately from [[JdbcTableDataObject]]
 * to avoid problems with net.ucanaccess.jdbc.UcanaccessDriver
 */
case class AccessTableDataObject(override val id: DataObjectId,
                                 path: String,
                                 override val schemaMin: Option[StructType] = None,
                                 override var table: Table,
                                 override val metadata: Option[DataObjectMetadata] = None
                                )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TableDataObject {

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext) : DataFrame = {

    // currently, only the schema is being inferred using [[net.ucanaccess.jdbc.UcanaccessDriver]]...
    val tableSchema = session.read
      .format("jdbc")
      .options(Map(
        "url" -> s"jdbc:ucanaccess://$path",
        "driver" -> "net.ucanaccess.jdbc.UcanaccessDriver",
        "dbtable" -> table.name))
      .load()
      .schema

    // ...the data itself is being read using [[com.healthmarketscience.jackcess.DatabaseBuilder]]
    // this shouldn't be necessary, but somehow Spark tries to read the header as a row as well when executing
    // [[JdbcUtils.resultSetToSparkInternalRows]] with UcanaccessDriver
    val db = openDb(session)
    val rows = db.getTable(table.name).iterator().asScala.toList.map(row => {
      val values = row.values().iterator().asScala.toSeq.map {
        case v: java.util.Date => new Timestamp(v.getTime) // Date/Time are mapped to Date by the UcanaccessDriver
        case v: java.lang.Float => v.toDouble // Floats are mapped to Doubles by the UcanaccessDriver
        case default => default
      }
      Row.fromSeq(values)
    })
    val df = session.createDataFrame(session.sparkContext.makeRDD(rows), tableSchema)
    db.close()

    validateSchemaMin(df, "read")
    //return
    df
  }

  private def openDb(session: SparkSession) = {
    val hadoopPath = new Path(path)
    val inputStream = FileSystem.get(hadoopPath.toUri, session.sparkContext.hadoopConfiguration).open(hadoopPath)
    val tempFile = File.createTempFile("temp", "accdb")
    tempFile.deleteOnExit()
    FileUtils.copyInputStreamToFile(inputStream, tempFile)
    DatabaseBuilder.open(tempFile)
  }

  // FIXME for dev purposes only, to visualize to current problem with the [[net.ucanaccess.jdbc.UcanaccessDriver]]
  def getDataFrameByFramework(doPersist:Boolean)(implicit session: SparkSession) : DataFrame = {
    val df = session.read
      .format("jdbc")
      .options(Map("url" -> s"jdbc:ucanaccess://$path",
        "driver" -> "net.ucanaccess.jdbc.UcanaccessDriver",
        "dbtable" -> table.name))
      .load()
    DataFrameUtil.persistDfIfPossible(df, doPersist)
  }

  override def isDbExisting(implicit session: SparkSession): Boolean = {
    // if we can open the db, it is existing
    val db = openDb(session)
    db.close()
    //return
    true
  }

  override def isTableExisting(implicit session: SparkSession): Boolean = {
    val db = openDb(session)
    val tableExisting = db.getTableNames.contains(table.name)
    db.close()
    //return
    tableExisting
  }

  override def dropTable(implicit session: SparkSession): Unit = throw new NotImplementedError

  override def factory: FromConfigFactory[DataObject] = AccessTableDataObject
}

object AccessTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AccessTableDataObject = {
    extract[AccessTableDataObject](config)
  }
}
