/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.workflow.action.file

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.{CustomFileAction, NoDataToProcessWarning}
import io.smartdatalake.workflow.action.executionMode.PartitionDiffMode
import io.smartdatalake.workflow.action.generic.customlogic.{CustomFileTransformer, OutputStreamFactory}
import io.smartdatalake.workflow.action.generic.transformer.ScalaClassFileTransformer
import io.smartdatalake.workflow.dataobject.CsvFileDataObject
import io.smartdatalake.workflow.dataobject.file.FileRef
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, FileSubFeed, InitSubFeed}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.{InputStream, OutputStream, PrintWriter}
import java.nio.file.{Files, Path => NioPath}
import scala.io.Source
import scala.util.Using

class CustomFileActionTest extends AnyFunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = SparkTestUtil.session

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  private var tempDir: NioPath = _
  private var tempPath: String = _

  before {
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    tempDir = Files.createTempDirectory("test")
    tempPath = tempDir.toAbsolutePath.toString
  }

  after {
    FileUtils.deleteDirectory(tempDir.toFile)
  }

  test("custom csv-file transformation") {

    val feed = "filetransfer"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    SparkTestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestFileTransformer].getName, options = Map("test" -> "true"))
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    // check if file is present
    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)

    // read src with util and count
    val dfSrc = srcDO.getSparkDataFrame()
    assert(dfSrc.columns.length > 2)
    val srcCount = dfSrc.count()

    // read tgt with util and count
    val dfTgt = tgtDO.getSparkDataFrame()
    assert(dfTgt.columns.length == 2)
    val tgtCount = dfTgt.count()
    assert(srcCount == tgtCount)
  }

  test("custom csv-file transformation with partition diff execution mode") {

    val feed = "filetransfer"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    val srcPartitionValues = Seq(PartitionValues(Map("p" -> "test")))
    SparkTestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve("p=test/" + resourceFile).toFile)

    // setup DataObjects
    val srcDO = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), partitions = Seq("p"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), partitions = Seq("p"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestFileTransformer].getName, options = Map("test" -> "true"))
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1, executionMode = Some(PartitionDiffMode()))
    val srcSubFeed = InitSubFeed("src1", srcPartitionValues) // InitSubFeed needed to test initExecutionMode!
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)
    assert(tgtSubFeed.partitionValues.toSet == srcPartitionValues.toSet)
    assert(tgtDO.listPartitions.toSet == srcPartitionValues.toSet)

    // check if file is present
    assert(tgtDO.getFileRefs(Seq()).map(_.fileName) == Seq(resourceFile))
    assert(tgtDO.getFileRefs(srcPartitionValues).map(_.fileName) == Seq(resourceFile))

    // check counts
    val dfSrc = srcDO.getSparkDataFrame()
    val dfTgt = tgtDO.getSparkDataFrame()
    assert(dfSrc.count() == dfTgt.count())
  }


  test("custom file transformation with partition value transformation") {

    val resourceFile = "AB_NYC_2019.csv"
    val srcPath = tempDir.resolve("testSrc")
    val tgtPath = tempDir.resolve("testTgt")
    SparkTestUtil.copyResourceToFile(resourceFile, srcPath.resolve("year=2002/month=12/" + resourceFile).toFile)

    // setup DataObjects: year/month partitions are standardized to dt partition
    val srcDO = CsvFileDataObject("src1", srcPath.toString.replace('\\', '/'), partitions = Seq("year", "month"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    val tgtDO = CsvFileDataObject("tgt1", tgtPath.toString.replace('\\', '/'), partitions = Seq("dt"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // start load
    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestDtPartitionFileTransformer].getName)
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head.asInstanceOf[FileSubFeed]

    // check partitions and files
    val expectedPartitionValues = Seq(PartitionValues(Map("dt" -> "200212")))
    assert(tgtSubFeed.partitionValues == expectedPartitionValues)
    assert(tgtSubFeed.fileRefs.get.map(_.partitionValues) == expectedPartitionValues)
    assert(tgtDO.listPartitions == expectedPartitionValues)
    assert(tgtPath.resolve("dt=200212").resolve(resourceFile).toFile.exists)
    assert(srcDO.getSparkDataFrame().count() == tgtDO.getSparkDataFrame().count())
  }

  test("custom file transformation with partition value transformation fails if output partition is missing") {

    val resourceFile = "AB_NYC_2019.csv"
    SparkTestUtil.copyResourceToFile(resourceFile, tempDir.resolve("testSrc").resolve("year=2002/month=12/" + resourceFile).toFile)
    val srcDO = CsvFileDataObject("src1", tempDir.resolve("testSrc").toString.replace('\\', '/'), partitions = Seq("year", "month"))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve("testTgt").toString.replace('\\', '/'), partitions = Seq("dt"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // TestFileTransformer does not create partition dt
    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestFileTransformer].getName, options = Map("test" -> "true"))
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1)
    val ex = intercept[Exception](action1.exec(Seq(FileSubFeed(None, "src1", partitionValues = Seq())))(contextExec))
    val exMessages = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).toSeq
    assert(exMessages.exists(_.contains("Partition columns dt of DataObject~tgt1 not found")), exMessages.mkString(" / "))
  }

  test("custom file transformation with partition value transformation and PartitionDiffMode") {

    val resourceFile = "AB_NYC_2019.csv"
    val srcPath = tempDir.resolve("testSrc")
    val tgtPath = tempDir.resolve("testTgt")
    SparkTestUtil.copyResourceToFile(resourceFile, srcPath.resolve("year=2002/month=12/" + resourceFile).toFile)
    SparkTestUtil.copyResourceToFile(resourceFile, srcPath.resolve("year=2003/month=01/" + resourceFile).toFile)

    val srcDO = CsvFileDataObject("src1", srcPath.toString.replace('\\', '/'), partitions = Seq("year", "month"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    val tgtDO = CsvFileDataObject("tgt1", tgtPath.toString.replace('\\', '/'), partitions = Seq("dt"), csvOptions = Map("header" -> "true", "delimiter" -> CustomFileActionTest.delimiter))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestDtPartitionFileTransformer].getName)
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1, executionMode = Some(PartitionDiffMode(applyPartitionValuesTransform = true)))

    // first run processes all partitions
    val srcSubFeed = InitSubFeed("src1", Seq()) // InitSubFeed needed to test initExecutionMode!
    action1.preInit(Seq(srcSubFeed), Seq())
    action1.init(Seq(srcSubFeed))
    action1.preExec(Seq(srcSubFeed))(contextExec)
    val tgtSubFeed = action1.exec(Seq(srcSubFeed))(contextExec).head
    val expectedPartitionValues = Seq(PartitionValues(Map("dt" -> "200212")), PartitionValues(Map("dt" -> "200301")))
    assert(tgtSubFeed.partitionValues.toSet == expectedPartitionValues.toSet)
    assert(tgtDO.listPartitions.toSet == expectedPartitionValues.toSet)
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed))(contextExec)

    // second run has no data to process, as transformed partition values of input exist in output
    action1.reset
    action1.preInit(Seq(srcSubFeed), Seq())
    action1.init(Seq(srcSubFeed))
    action1.preExec(Seq(srcSubFeed))(contextExec)
    intercept[NoDataToProcessWarning](action1.exec(Seq(srcSubFeed))(contextExec))
  }

  test("custom file transformation with multiple output files") {

    val srcPath = tempDir.resolve("testSrc")
    val tgtPath = tempDir.resolve("testTgt")
    Files.createDirectories(srcPath.resolve("year=2002/month=12"))
    Files.writeString(srcPath.resolve("year=2002/month=12/data.csv"), "id\n1\n2\n3\n")
    val srcDO = CsvFileDataObject("src1", srcPath.toString.replace('\\', '/'), partitions = Seq("year", "month"), csvOptions = Map("header" -> "true"))
    val tgtDO = CsvFileDataObject("tgt1", tgtPath.toString.replace('\\', '/'), partitions = Seq("dt"), csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // split each file into one file per data row, and map partitions to dt
    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestSplitRowsFileTransformer].getName)
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1)
    val tgtSubFeed = action1.exec(Seq(FileSubFeed(None, "src1", partitionValues = Seq())))(contextExec).head.asInstanceOf[FileSubFeed]

    // output files are created in the transformed partition, with the file name extension of the output DataObject
    val expectedFileNames = Seq("data-0.csv", "data-1.csv", "data-2.csv")
    assert(tgtSubFeed.fileRefs.get.map(_.fileName).sorted == expectedFileNames)
    assert(tgtSubFeed.fileRefMapping.get.map(_.src.fileName).distinct == Seq("data.csv"))
    assert(tgtSubFeed.metrics.get("files_written") == 3)
    assert(tgtDO.getFileRefs(Seq()).map(_.fileName).sorted == expectedFileNames)
    assert(tgtDO.listPartitions == Seq(PartitionValues(Map("dt" -> "200212"))))
    assert(tgtDO.getSparkDataFrame().count() == 3)
  }

  test("init phase skips sample file if predicted input file does not exist, and uses first output file otherwise") {

    val srcPath = tempDir.resolve("testSrc")
    val tgtPath = tempDir.resolve("testTgt")
    Files.createDirectories(srcPath)
    val srcDO = CsvFileDataObject("src1", srcPath.toString.replace('\\', '/'), csvOptions = Map("header" -> "true"))
    val tgtDO = CsvFileDataObject("tgt1", tgtPath.toString.replace('\\', '/'), csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)
    val sampleFile = tgtPath.resolve(".sample/sampleData.csv")

    // file reference predicted by a previous file action in init phase, which does not exist yet
    val srcFile = srcPath.resolve("data.csv")
    val srcSubFeed = FileSubFeed(Some(Seq(FileRef(srcFile.toString.replace('\\', '/'), "data.csv", PartitionValues(Map())))), "src1", partitionValues = Seq())
    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestSplitRowsFileTransformer].getName)
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1)
    action1.preInit(Seq(srcSubFeed), Seq())
    action1.init(Seq(srcSubFeed))
    assert(!sampleFile.toFile.exists)

    // with existing input file the sample file contains the first output file of the transformation
    Files.writeString(srcFile, "id\n1\n2\n")
    action1.reset
    action1.preInit(Seq(srcSubFeed), Seq())
    action1.init(Seq(srcSubFeed))
    assert(Files.readString(sampleFile) == "id\n1\n")
  }

  test("custom file transformation fails after all files are processed if a file returned an error") {

    val resourceFile = "AB_NYC_2019.csv"
    val srcPath = tempDir.resolve("testSrc")
    val tgtPath = tempDir.resolve("testTgt")
    SparkTestUtil.copyResourceToFile(resourceFile, srcPath.resolve(resourceFile).toFile)
    Files.createDirectories(srcPath)
    Files.writeString(srcPath.resolve("fail.csv"), "fail")
    val srcDO = CsvFileDataObject("src1", srcPath.toString.replace('\\', '/'))
    val tgtDO = CsvFileDataObject("tgt1", tgtPath.toString.replace('\\', '/'))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // one file per Spark partition, the transformer returns an error for fail.csv
    val fileTransformer = ScalaClassFileTransformer(className = classOf[TestFailOnContentFileTransformer].getName)
    val action1 = CustomFileAction(id = "cfa", srcDO.id, tgtDO.id, fileTransformer, 1)
    val ex = intercept[Exception](action1.exec(Seq(FileSubFeed(None, "src1", partitionValues = Seq())))(contextExec))
    val exMessages = Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).map(_.getMessage).toSeq
    assert(exMessages.exists(m => m.contains("transformation failed for 1 of 2 files") && m.contains("fail.csv")), exMessages.mkString(" / "))

    // the other file is processed nevertheless
    assert(tgtPath.resolve(resourceFile).toFile.exists)
  }

}

object CustomFileActionTest {
  val delimiter = ","
}

class TestFileTransformer extends CustomFileTransformer {
  override def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] = {
    assert(options("test") == "true")
    Using.resource(Source.fromInputStream(input)) { src =>
      Using.resource(new PrintWriter(output)) { os =>
        src.getLines().foreach { l =>
          // reduce to 2 cols
          val transformedLine = l.split(CustomFileActionTest.delimiter).take(2).mkString(CustomFileActionTest.delimiter)
          os.println(transformedLine)
        }
      }
      None
    }
  }
}
class TestDtPartitionFileTransformer extends CustomFileTransformer {
  override def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] = {
    input.transferTo(output)
    None
  }
  override def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] =
    Some(partitionValues.map(pv => (pv, PartitionValues(Map("dt" -> (pv("year").toString + pv("month").toString))))).toMap)
}

class TestFailOnContentFileTransformer extends CustomFileTransformer {
  override def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] = {
    val content = input.readAllBytes()
    if (new String(content).startsWith("fail")) Some(new IllegalArgumentException("test failure"))
    else {
      output.write(content)
      None
    }
  }
}

/**
 * Creates one output file per data row, repeating the header line, and maps partitions year/month to dt if partitioned.
 */
class TestSplitRowsFileTransformer extends CustomFileTransformer {
  override def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception] = {
    val header +: rows = new String(input.readAllBytes()).split("\n").toSeq
    val baseName = fileName.stripSuffix(".csv")
    rows.zipWithIndex.foreach { case (row, idx) =>
      // output file name without extension, it is appended according to the output DataObjects fileName
      outputs.create(s"$baseName-$idx").write(s"$header\n$row\n".getBytes)
    }
    None
  }
  override def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] =
    Some(partitionValues.map(pv => (pv, if (pv.isEmpty) pv else PartitionValues(Map("dt" -> (pv("year").toString + pv("month").toString))))).toMap)
}
