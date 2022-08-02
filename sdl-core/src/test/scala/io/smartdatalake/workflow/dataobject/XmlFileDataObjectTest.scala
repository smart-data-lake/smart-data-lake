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
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.util.xml.XsdSchemaConverter
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, functions}

import java.nio.file.Files
import scala.io.Source


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
    val dataObj = XmlFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = Some(SparkSchema(df1.schema)), filenameColumn = Some("_filename"))
    dataObj.writeDataFrameToPath(SparkDataFrame(df1.where($"h1"==="A")), new Path(dataObj.hadoopPath, "h1=A"), SDLSaveMode.Overwrite)
    dataObj.writeDataFrameToPath(SparkDataFrame(df1.where($"h1"==="B")), new Path(dataObj.hadoopPath, "h1=B"), SDLSaveMode.Overwrite)

    val dataObjPartitioned = dataObj.copy(partitions = Seq("h1"))

    // read with list of partition values
    val dfResult1 = dataObjPartitioned.getSparkDataFrame(pv1).cache
    assert(dfResult1.columns.toSet == Set("h1", "h2", "h3", "_filename"))
    assert(dfResult1.drop("_filename").isEqual(df1))
    assert(dfResult1.where($"_filename".isNull).isEmpty)

    // read all
    val dfResult2 = dataObjPartitioned.getSparkDataFrame().cache
    assert(dfResult2.columns.toSet == Set("h1", "h2", "h3", "_filename"))
    assert(dfResult2.drop("_filename").isEqual(df1))
    assert(dfResult2.where($"_filename".isNull).isEmpty)
  }

  test("Simple XML file") {
    val tempDir = Files.createTempDirectory("xml")

    // prepare schema
    val xsdContent = Source.fromResource("xmlSchema/basket.xsd").mkString
    val rootSchema = XsdSchemaConverter.read(xsdContent, 10)
    rootSchema.printTreeString()
    val schema = SchemaUtil.extractRowTag(rootSchema, "basket/entry")
      .add(StructField("_corrupt_record", StringType))

    // prepare data
    val xmlResourceFile = "xmlSchema/basket.xml"
    val xmlFile = tempDir.resolve(xmlResourceFile.split("/").last).toFile
    TestUtil.copyResourceToFile(xmlResourceFile, xmlFile)

    /*
    val df = session.read
      .format("xml")
      .option("rowTag", "entry")
      .option("path", tempDir.toFile.getPath)
      .load()
    df.show
    */

    val dataObj = XmlFileDataObject(id = "test1", path = tempDir.toFile.getPath,
      schema = Some(SparkSchema(schema)),
      rowTag = Some("entry"),
    )

    // read
    val dfResult2 = dataObj.getSparkDataFrame()
    dfResult2.show
  }

  test("Complex XML file") {
    // TODO
    // - commented items are still read by spark-xml!

    val tempDir = Files.createTempDirectory("xml")

    // prepare schema
    val xsdContent = Source.fromResource("xmlSchema/TMS_TMS2TMS_Tariff_1300.xsd").mkString
    val rootSchema = XsdSchemaConverter.read(xsdContent, 10)
    //    val schema = SchemaUtil.extractRowTag(rootSchema, "TMSData/update/tariffNodes/modified/tariffNode")
    //    val schema = SchemaUtil.extractRowTag(rootSchema, "TMSData/initialLoad/tariffNodes/tariffNode")
    val schema = SchemaUtil.extractRowTag(rootSchema, "TMSData/initialLoad/tariffRoot")
      .add(StructField("_corrupt_record", StringType))
    //schema.printTreeString()

    // prepare data
    val xmlResourceFile = "xmlSchema/tns_minimal_init_test.xml"
    val xmlFile = tempDir.resolve(xmlResourceFile.split("/").last).toFile
    TestUtil.copyResourceToFile(xmlResourceFile, xmlFile)

    val dataObj = XmlFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath),
      schema = Some(SparkSchema(schema)),
      rowTag = Some("tariffRoot")
    )

    // read
    val dfResultL0 = dataObj.getSparkDataFrame().cache
      .withColumn("cntDesc", functions.size($"descriptions"))
    //.withColumn("cntChildren", functions.size($"tariffNodes"))
    dfResultL0.show
    /*
    val dfResultL1 = dfResultL0
      .withColumn("tariffNodes", explode($"tariffNodes"))
      .select($"tariffNodes.tariffNode.*")
      .withColumn("cntChildren", functions.size($"tariffNodes"))
    dfResultL1.show
    val dfResultL2 = dfResultL1
      .withColumn("tariffNodes", explode($"tariffNodes"))
      .select($"tariffNodes.tariffNode.*")
      .withColumn("cntChildren", functions.size($"tariffNodes"))
    dfResultL2.show
    val dfResultL3 = dfResultL2
      .withColumn("tariffNodes", explode($"tariffNodes"))
      .select($"tariffNodes.tariffNode.*")
      .withColumn("cntChildren", functions.size($"tariffNodes"))
    dfResultL3.show
    val dfResultL4 = dfResultL3
      .withColumn("tariffNodes", explode($"tariffNodes"))
      .select($"tariffNodes.tariffNode.*")
      .withColumn("cntChildren", functions.size($"tariffNodes"))
    dfResultL4.show
    val dfResultL5 = dfResultL4
      .withColumn("tariffNodes", explode($"tariffNodes"))
      .select($"tariffNodes.tariffNode.*")
      .withColumn("cntChildren", functions.size($"tariffNodes"))
    dfResultL5.show
    */
    //https://pig.apache.org/docs/r0.17.0/api/org/apache/pig/piggybank/storage/XMLLoader.XMLRecordReader.html
  }

  test("XML with nested lists") {

    val tempDir = Files.createTempDirectory("xml")

    // prepare schema
    val xsdContent = Source.fromResource("xmlSchema/lists.xsd").mkString
    val rootSchema = XsdSchemaConverter.read(xsdContent, 10)
    //rootSchema.printTreeString()
    val schema = SchemaUtil.extractRowTag(rootSchema, "tree/nodes/node")
    //schema.printTreeString()

    // prepare data and config
    val xmlResourceFile = "xmlSchema/lists.xml"
    val xmlFile = tempDir.resolve(xmlResourceFile.split("/").last).toFile
    TestUtil.copyResourceToFile(xmlResourceFile, xmlFile)
    val dataObj = XmlFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath),
      schema = Some(SparkSchema(schema)),
      rowTag = Some("node")
    )

    // read and check
    val dfResult = dataObj.getSparkDataFrame().cache
    val cntDescriptions = dfResult.select(functions.size($"descriptions.description")).as[Int].collect.toSeq
    assert(cntDescriptions == Seq(2))
  }

  private def createDataObject(path: String, schemaOpt: Option[StructType]): XmlFileDataObject = {
    val dataObj = XmlFileDataObject(id = "schemaTestXmlDO", path = path, schema = schemaOpt.map(SparkSchema))
    instanceRegistry.register(dataObj)
    dataObj
  }

  private def createDataObjectWithSchemaMin(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): XmlFileDataObject = {
    val dataObj = XmlFileDataObject(id = "schemaTestXmlDO", path = path, schema = schemaOpt.map(SparkSchema), schemaMin = schemaMinOpt.map(SparkSchema))
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("com.databricks.spark.xml").save(path)
  }
}
