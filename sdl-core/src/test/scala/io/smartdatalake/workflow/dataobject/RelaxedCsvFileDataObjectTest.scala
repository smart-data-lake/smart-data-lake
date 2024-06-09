package io.smartdatalake.workflow.dataobject

import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.util.hdfs.PartitionValues
import org.apache.spark.SparkException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructType}

import java.nio.file.Files

class RelaxedCsvFileDataObjectTest extends DataObjectTestSuite {

  import session.implicits._

  test("CSV files with missing and superfluous column") {
    val tempDir = Files.createTempDirectory("csv")

    // exact schema
    val data1 = Seq(("A", "1", "-"), ("B", "2", null))
    val df1 = data1.toDF("h1", "h2", "h3")
    df1.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    // schema missing column h3
    val data2 = Seq(("C", "1"), ("D", "2"))
    val df2 = data2.toDF("h1", "h2")
    df2.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    // schema has superfluous column h4
    val data3 = Seq(("C", "1", "-", "x"), ("D", "2", "-", "x"))
    val df3= data3.toDF("h1", "h2", "h3", "h4")
    df3.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = Some(SparkSchema(df1.schema)))

    val dfResult = dataObj.getSparkDataFrame()

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3"))
    val expectedResult = data1 ++ data2.map(x => (x._1, x._2, null)) ++ data3.map(x => (x._1, x._2, x._3))
    assert(dfResult.as[(String,String,String)].collect.toSet == expectedResult.toSet )
  }

  test("CSV files with missing and superfluous column treated as error") {
    val tempDir = Files.createTempDirectory("csv")

    // exact schema
    val data1 = Seq(("A", "1", "-"), ("A", "2", null))
    val df1 = data1.toDF("h1", "h2", "h3")
    df1.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    // schema missing column h3
    val data2 = Seq(("B", "1"), ("B", "2"))
    val df2 = data2.toDF("h1", "h2")
    df2.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    // schema has superfluous column h4
    val data3 = Seq(("C", "1", "-", "x"), ("C", "2", "-", "x"))
    val df3= data3.toDF("h1", "h2", "h3", "h4")
    df3.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    val schema = Some(df1.schema.add("_filename", StringType).add("_corrupt_record", StringType).add("_corrupt_record_msg", StringType))
    val dataObj = RelaxedCsvFileDataObject( id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = schema.map(SparkSchema), filenameColumn = Some("_filename")
                                          , treatMissingColumnsAsCorrupt = true, treatSuperfluousColumnsAsCorrupt = true)

    val dfResult = dataObj.getSparkDataFrame().cache

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3", "_corrupt_record", "_corrupt_record_msg", "_filename"))
    assert(dfResult.where($"h1"==="A" and $"_corrupt_record".isNull and $"_corrupt_record_msg".isNull).count == 2)
    assert(dfResult.where($"h1"==="B" and $"_corrupt_record".isNotNull and $"_corrupt_record_msg".isNotNull).count == 2)
    assert(dfResult.where($"h1"==="C" and $"_corrupt_record".isNotNull and $"_corrupt_record_msg".isNotNull).count == 2)
  }

  test("CSV files with different column order") {
    val tempDir = Files.createTempDirectory("csv")

    // column order 1
    val data1 = Seq(("A", "1", "-"), ("B", "2", null))
    val df1 = data1.toDF("h1", "h2", "h3")
    df1.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    // column order 2
    val data2 = Seq(("1","-","C"), ("2","-","D"))
    val df2 = data2.toDF("h2", "h3", "h1")
    df2.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = Some(SparkSchema(df1.schema)))

    val dfResult = dataObj.getSparkDataFrame()

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3"))
    val expectedResult = data1 ++ data2.map(x => (x._3, x._1, x._2))
    assert(dfResult.as[(String,String,String)].collect.toSet == expectedResult.toSet )
  }

  test("CSV files with filename col") {
    val tempDir = Files.createTempDirectory("csv")

    val data1 = Seq(("A", "1", "-"), ("B", "2", null))
    val df1 = data1.toDF("h1", "h2", "h3")
    df1.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    val data2 = Seq(("1","-","C"), ("2","-","D"))
    val df2 = data2.toDF("h2", "h3", "h1")
    df2.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)

    val schema = Some(df1.schema.add("_filename", StringType))
    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = schema.map(SparkSchema), filenameColumn = Some("_filename"))

    val dfResult = dataObj.getSparkDataFrame().cache

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3", "_filename"))
    val expectedResult = data1 ++ data2.map(x => (x._3, x._1, x._2))
    assert(dfResult.select($"h1", $"h2", $"h3").as[(String,String,String)].collect.toSet == expectedResult.toSet )
    assert(dfResult.select($"_filename").distinct.count > 1 )
  }

  test("CSV files partitioned") {
    val tempDir = Files.createTempDirectory("csv")

    val data1 = Seq(("A", "1", "-"), ("B", "2", null))
    val df1 = data1.toDF("h1", "h2", "h3")
    val pv1 = Seq(PartitionValues(Map("h1"->"A")), PartitionValues(Map("h1"->"B")))

    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), partitions = Seq("h1"), schema = Some(SparkSchema(df1.schema)), filenameColumn = Some("_filename"))
    dataObj.writeSparkDataFrame(df1, pv1)

    val dfResult = dataObj.getSparkDataFrame(pv1).cache

    assert(dfResult.columns.toSeq == Seq("h2", "h3", "h1", "_filename"))
    assert(dfResult.select($"h1", $"h2", $"h3").as[(String,String,String)].collect.toSet == data1.toSet)
    assert(dfResult.where($"_filename".isNull).isEmpty)
  }

  test("Read CSV file with header only") {
    val tempDir = Files.createTempDirectory("csv")

    // File with header only
    val data1 = Seq[(String,String,String)]()
    val df1 = data1.toDF("h1", "h2", "h3")
    Environment._enableSparkPlanNoDataCheck = Some(false)
    df1.write.mode(SaveMode.Append).option("header", true).csv(tempDir.toFile.getPath)
    Environment._enableSparkPlanNoDataCheck = Some(true)

    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = Some(SparkSchema(df1.schema)))

    val dfResult = dataObj.getSparkDataFrame()

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3"))
    assert(dfResult.isEmpty)
  }

  test("Read empty file (no header, no data)") {
    val tempDir = Files.createTempDirectory("csv")

    // File with header only
    val data1 = Seq[(String,String,String)]()
    val df1 = data1.toDF("h1", "h2", "h3")
    Environment._enableSparkPlanNoDataCheck = Some(false)
    df1.write.mode(SaveMode.Append).option("header", false).csv(tempDir.toFile.getPath)
    Environment._enableSparkPlanNoDataCheck = Some(true)

    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = Some(SparkSchema(df1.schema)))

    val dfResult = dataObj.getSparkDataFrame()

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3"))
    assert(dfResult.isEmpty)
  }

  test("Bad CSV File with PermissiveMode and _corrupt_record col") {
    val tempDir = Files.createTempDirectory("csv")
    val badCsvContent = """
        |h1,h2,h3
        |A,1
        |""".stripMargin
    Files.write(tempDir.resolve("bad.csv"), badCsvContent.getBytes)

    val options = Map("mode" -> "permissive")
    val schema = Some(StructType.fromDDL("h1 string, h2 string, h3 string, _corrupt_record string"))
    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = schema.map(SparkSchema), csvOptions = options)
    val dfResult = dataObj.getSparkDataFrame().cache

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3", "_corrupt_record"))
    val expectedResult = Seq[(String,String,String)](("A","1",null))
    assert(dfResult.select($"h1", $"h2", $"h3").as[(String,String,String)].collect.toSet == expectedResult.toSet )
    assert(dfResult.where($"_corrupt_record".isNotNull).count == 1)
  }

  test("Bad CSV File with FailFastMode") {
    val tempDir = Files.createTempDirectory("csv")
    val badCsvContent = """
                          |h1,h2,h3
                          |A,1
                          |""".stripMargin
    Files.write(tempDir.resolve("bad.csv"), badCsvContent.getBytes)

    val options = Map("mode" -> "failfast")
    val schema = Some(StructType.fromDDL("h1 string, h2 string, h3 string"))
    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = schema.map(SparkSchema), csvOptions = options)
    intercept[SparkException](dataObj.getSparkDataFrame().count)
  }

  test("Bad CSV File with DropMalformedMode") {
    val tempDir = Files.createTempDirectory("csv")
    val badCsvContent = """
                          |h1,h2,h3
                          |A,1
                          |""".stripMargin
    Files.write(tempDir.resolve("bad.csv"), badCsvContent.getBytes)

    val options = Map("mode" -> "dropmalformed")
    val schema = Some(StructType.fromDDL("h1 string, h2 string, h3 string"))
    val dataObj = RelaxedCsvFileDataObject(id = "test1", path = escapedFilePath(tempDir.toFile.getPath), schema = schema.map(SparkSchema), csvOptions = options)
    val dfResult = dataObj.getSparkDataFrame()

    assert(dfResult.columns.toSeq == Seq("h1", "h2", "h3"))
    assert(dfResult.count == 0)
  }
}
