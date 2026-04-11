/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.typesafe.config.ConfigFactory
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.PushPredicateThroughTolerantCollectMetricsRuleObject.pushDownTolerantMetricsMarker
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.connection.HadoopFileConnection
import io.smartdatalake.workflow.dataframe.spark.{SparkObservation, SparkSchema, SparkSubFeed}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class ParquetFileDataObjectTest extends DataObjectTestSuite with SparkFileDataObjectSchemaBehavior with SmartDataLakeLogger {

  private val tempDir = createTempDir
  private val tempPath = tempDir.toAbsolutePath.toString

  import session.implicits._

  private val testDf = Seq(
    ("string1",1,Seq(1,2,3)),
    ("string2",2,Seq(2,4,6)),
    ("string3",3,Seq(3,6,9))
  ).toDF("str","number","list")

  test("write and read parquet file with observation of processed files") {

    val config = ConfigFactory.parseString(s"""
                                               | id = src1
                                               | path = "${escapedFilePath(tempPath)}"
                                               | filenameColumn = _filename
         """.stripMargin)

    val doSrc1 = ParquetFileDataObject.fromConfig(config)
    doSrc1.writeSparkDataFrame(testDf, Seq())
    val filesObserver = doSrc1.setupFilesObserver("test")
    val result = doSrc1.getSparkDataFrame()(contextExec)
    assert(result.count() == 3)
    assert(filesObserver.getFilesProcessed.nonEmpty)
  }

  // See also notes on [[CollectSetDeterministic]]
  test("make sure observation of processed files does not crash if there are no files to process") {

    val config = ConfigFactory.parseString(s"""
                                              | id = src1
                                              | path = "${escapedFilePath(tempPath)}"
                                              | filenameColumn = _filename
                                              | schema = "a int, b string"
         """.stripMargin)

    val doSrc1 = ParquetFileDataObject.fromConfig(config)
    doSrc1.deleteAll
    val filesObserver = doSrc1.setupFilesObserver("test")
    val df = doSrc1.getSparkDataFrame()(contextInit) // ok in init phase
    intercept[NoDataToProcessWarning](doSrc1.getSparkDataFrame()(contextExec))
  }

  test("write and read parquet file with connection") {

    // write without connection
    val configTgt1 = ConfigFactory.parseString(s"""
                                                   | id = tgt1
                                                   | path = "${escapedFilePath(tempPath)}/test"
         """.stripMargin)
    val doTgt1 = ParquetFileDataObject.fromConfig(configTgt1)
    doTgt1.writeSparkDataFrame(testDf, Seq())

    // read with connection
    val configSrc1 = ConfigFactory.parseString(s"""
                                               | id = src1
                                               | connectionId = con1
                                               | path = "test"
         """.stripMargin)
    instanceRegistry.register(HadoopFileConnection("con1", s"file:///${escapedFilePath(tempPath)}"))
    val doSrc1 = ParquetFileDataObject.fromConfig(configSrc1)

    val result = doSrc1.getSparkDataFrame()
    assert(result.count() == 3)
  }


  test("read parquet file check push-down filter with input count observation") {

    val config = ConfigFactory.parseString(s"""
                                              | id = src1
                                              | path = "${escapedFilePath(tempPath)}"
                                              | filenameColumn = _filename
         """.stripMargin)

    val doSrc1 = ParquetFileDataObject.fromConfig(config)
    doSrc1.writeSparkDataFrame(testDf, Seq())
    val (df, observation) = doSrc1.getDataFrame(Seq(), SparkSubFeed.subFeedType)(contextExec)
      .observe("test#input"+pushDownTolerantMetricsMarker, Seq(SparkSubFeed.count(SparkSubFeed.lit("*")).as("count")), isExecPhase = true)
      .where(SparkSubFeed.col("str")===SparkSubFeed.lit("test"))
      .setupObservation("final", Seq(SparkSubFeed.count(SparkSubFeed.lit("*")).as("count")), isExecPhase = true)

    assert(df.count == 0) // no results expected

    observation.asInstanceOf[SparkObservation].setOtherObservationsPrefix("test#")
    val metrics = observation.waitFor()
    assert(metrics("count") == 0) // this is the final count
    assert(metrics("count#input") == 0) // input count 0 is expected (if it fails, filter push down through observation doesnt work anymore)
  }

  test("read parquet files mixed with other files in same directory") {
    val parquetDO = ParquetFileDataObject(id = "src1", path = escapedFilePath(tempPath), filenameColumn = Some("_filename"))
    val jsonDO = JsonFileDataObject(id = "src2", path = escapedFilePath(tempPath), filenameColumn = Some("_filename"), saveMode = SDLSaveMode.Append, jsonOptions = Some(Map("multiline" -> "false")))
    parquetDO.writeSparkDataFrame(testDf, Seq())
    jsonDO.writeSparkDataFrame(testDf, Seq())
    val resultParquet = parquetDO.getSparkDataFrame()(contextExec)
    resultParquet.show(false)
    assert(resultParquet.select($"_filename").as[String].collect.map(_.split('.').last).toSeq == Seq("parquet", "parquet", "parquet"))
    val resultJson = jsonDO.getSparkDataFrame()(contextExec)
    resultJson.show(false)
    assert(resultJson.select($"_filename").as[String].collect.map(_.split('.').last).toSeq == Seq("json", "json", "json"))
  }

  test("incremental output mode") {

    // create data object
    val dataObject1 = ParquetFileDataObject( "incDO1", path = tempPath+"/incremental1", saveMode = SDLSaveMode.Append)

    // write test data 1
    val df1 = Seq((1,"A",1),(2,"A",2),(3,"B",3),(4,"B",4)).toDF("id", "p", "value")
    dataObject1.prepare
    dataObject1.initSparkDataFrame(df1, Seq())
    dataObject1.writeSparkDataFrame(df1)

    // test 1
    dataObject1.setState(None) // initialize incremental output with empty state
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 4
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 4 // can be queried multiple times with same state
    val newState1 = dataObject1.getState // triggers moving forward incremental state to the latest, so that next query only returns new data

    // append test data 2
    val df2 = Seq((5,"B",5)).toDF("id", "p", "value")
    dataObject1.writeSparkDataFrame(df2)

    // test 2
    dataObject1.setState(newState1)
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 1
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 1 // can be queried multiple times with same state
    val newState2 = dataObject1.getState
    assert(newState1.get < newState2.get)
    intercept[NoDataToProcessWarning](dataObject1.getSparkDataFrame()(contextExec)) // no new data since last state

    // append test data 3
    val df3 = Seq((6,"B",6)).toDF("id", "p", "value")
    dataObject1.writeSparkDataFrame(df2)

    // test 3
    //dataObject1.setState(newState1) // setting state not necessary, as state is should move forward automatically through getState called above
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 1
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 1 // can be queried multiple times with same state
    val newState3 = dataObject1.getState
    assert(newState2.get < newState3.get)
    intercept[NoDataToProcessWarning](dataObject1.getSparkDataFrame()(contextExec)) // no new data since last state

    // disable incremental output and query all data
    dataObject1.setState(None)
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 6
    dataObject1.getSparkDataFrame()(contextExec).count() shouldEqual 6 // can be queried multiple times with same state
  }

  testsFor(readNonExistingSources(createDataObject, fileExtension = ".parquet"))
  testsFor(readEmptySourcesWithEmbeddedSchema(createDataObject, fileExtension = ".parquet"))
  testsFor(validateSchemaMinOnWrite(createDataObjectWithSchemaMin, fileExtension = ".parquet"))
  testsFor(validateSchemaMinOnRead(createDataObjectWithSchemaMin, fileExtension = ".parquet"))

  def createDataObject(path: String, schemaOpt: Option[StructType]): ParquetFileDataObject = {
    val dataObj = ParquetFileDataObject(id = "schemaTestParquetDO", path = path, schema = schemaOpt.map(SparkSchema.apply))
    instanceRegistry.register(dataObj)
    dataObj
  }

  def createDataObjectWithSchemaMin(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): ParquetFileDataObject = {
    val dataObj = ParquetFileDataObject(id = "schemaTestParquetDO", path = path, schema = schemaOpt.map(SparkSchema.apply), schemaMin = schemaMinOpt.map(SparkSchema.apply))
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("parquet").save(path)
  }
}