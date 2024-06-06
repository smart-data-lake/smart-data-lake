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

import com.healthmarketscience.jackcess.DatabaseBuilder
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.sql.Timestamp
import scala.jdk.CollectionConverters._

/**
 * [[DataObject]] of type Microsoft Access.
 * Can read a table from a Microsoft Access DB.
 * @param path: the path to the access database file
 * @param table: the Access table to be read
 */
case class AccessTableDataObject(override val id: DataObjectId,
                                 path: String,
                                 override val schemaMin: Option[GenericSchema] = None,
                                 override var table: Table,
                                 override val metadata: Option[DataObjectMetadata] = None
                                )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TableDataObject with CanCreateSparkDataFrame {

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext) : DataFrame = {
    val session = context.sparkSession

    val db = openDb(session)
    val accessTable = db.getTable(table.name)

    // derive Spark Schema
    val tableSchema = StructType(
      accessTable.getColumns.asScala.map(
        col => StructField(col.getName, getCatalystType(col.getSQLType, col.getPrecision, col.getScale))
      ).toList
    )

    // create DataFrame from rows
    val rows = accessTable.iterator().asScala.toList.map(row => {
      val values = row.values().iterator().asScala.toSeq.map {
        case v: java.util.Date => new Timestamp(v.getTime)
        case v: java.time.LocalDateTime => Timestamp.valueOf(v)
        case default => default
      }
      Row.fromSeq(values)
    })
    val df = session.createDataFrame(session.sparkContext.makeRDD(rows), tableSchema)
    db.close()

    validateSchemaMin(SparkSchema(df.schema), "read")
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

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    // if we can open the db, it is existing
    val db = openDb(context.sparkSession)
    db.close()
    //return
    true
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    val db = openDb(context.sparkSession)
    val tableExisting = db.getTableNames.contains(table.name)
    db.close()
    //return
    tableExisting
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = throw new NotImplementedError

  /**
   * Mapping from Java SQL Types to Spark Types
   * Copied and adapted from Spark:JdbcUtils
   */
  def getCatalystType( sqlType: Int, precision: Int, scale: Int, signed: Boolean = true): DataType = {
    sqlType match {
      case java.sql.Types.BIGINT => if (signed) LongType else DecimalType(20, 0)
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) IntegerType else LongType
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => StringType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
    }
  }

  override def factory: FromConfigFactory[DataObject] = AccessTableDataObject
}

object AccessTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AccessTableDataObject = {
    extract[AccessTableDataObject](config)
  }
}
