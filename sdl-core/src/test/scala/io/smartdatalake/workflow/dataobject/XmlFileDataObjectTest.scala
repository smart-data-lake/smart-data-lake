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

import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.nio.file.Files
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import org.apache.hadoop.fs.Path


class XmlFileDataObjectTest extends DataObjectTestSuite with SparkFileDataObjectSchemaBehavior {

  import session.implicits._

  testsFor(readNonExistingSources(createDataObject, ".xml"))
  testsFor(readEmptySources(createDataObject, ".xml"))
  testsFor(validateSchemaMinOnWrite(createDataObjectWithSchemaMin, ".xml"))

  // note that spark-xml doesn't support reading and writing partitioned xml data.
  // SDL implements custom logic to read partitioned xml data
  test("XML files partitioned") {
    val tempDir = Files.createTempDirectory("xml")

    val data1 = Seq(("A", "1", "-"), ("B", "2", null))
    val df1 = data1.toDF("h1", "h2", "h3")
    val pv1 = Seq(PartitionValues(Map("h1"->"A")), PartitionValues(Map("h1"->"B")))

    // Partitions have to be written manually as spark-xml doesn't support writing partitions
    val dataObj = XmlFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = Some(df1.schema), filenameColumn = Some("_filename"))
    dataObj.writeDataFrameToPath(df1.where($"h1"==="A"), new Path(dataObj.hadoopPath, "h1=A"), SDLSaveMode.Overwrite)
    dataObj.writeDataFrameToPath(df1.where($"h1"==="B"), new Path(dataObj.hadoopPath, "h1=B"), SDLSaveMode.Overwrite)

    val dataObjPartitioned = dataObj.copy(partitions = Seq("h1"))

    // read with list of partition values
    val dfResult1 = dataObjPartitioned.getDataFrame(pv1).cache
    assert(dfResult1.columns.toSet == Set("h1", "h2", "h3", "_filename"))
    assert(dfResult1.drop("_filename").isEqual(df1))
    assert(dfResult1.where($"_filename".isNull).isEmpty)

    // read all
    val dfResult2 = dataObjPartitioned.getDataFrame().cache
    assert(dfResult2.columns.toSet == Set("h1", "h2", "h3", "_filename"))
    assert(dfResult2.drop("_filename").isEqual(df1))
    assert(dfResult2.where($"_filename".isNull).isEmpty)
  }

  private def createDataObject(path: String, schemaOpt: Option[StructType]): XmlFileDataObject = {
    val dataObj = XmlFileDataObject(id = "schemaTestXmlDO", path = path, schema = schemaOpt)
    instanceRegistry.register(dataObj)
    dataObj
  }

  private def createDataObjectWithSchemaMin(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): XmlFileDataObject = {
    val dataObj = XmlFileDataObject(id = "schemaTestXmlDO", path = path, schema = schemaOpt, schemaMin = schemaMinOpt)
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("com.databricks.spark.xml").save(path)
  }
}
