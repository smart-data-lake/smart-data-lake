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

package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.customlogic.{CustomFileTransformer, OutputStreamFactory}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.charset.StandardCharsets

class GenericFileTransformerTest extends AnyFunSuite {

  private implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  private implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
  private val actionId = ActionId("a1")

  /**
   * Run a transformer with in-memory output streams, and a file name adaption appending ".csv" like a DataObject with fileName=*.csv
   * @return created output files with their content, and the error
   */
  private def run(transformer: GenericFileTransformer, input: String, options: Map[String, String] = Map()): (Seq[(String, String)], Option[Exception]) = {
    val outputs = collection.mutable.LinkedHashMap[String, ByteArrayOutputStream]()
    val (fileNames, error) = GenericFileTransformer.transformToFiles(transformer, options, new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), "data.csv",
      fileName => if (fileName.endsWith(".csv")) fileName else fileName + ".csv",
      fileName => outputs.getOrElseUpdate(fileName, new ByteArrayOutputStream()))
    assert(fileNames == outputs.keys.toSeq)
    (outputs.toSeq.map { case (name, os) => (name, os.toString(StandardCharsets.UTF_8)) }, error)
  }

  test("1:1 transformer writes to default file name") {
    val (outputs, error) = run(ScalaClassFileTransformer(className = classOf[TestAppendFileTransformer].getName), "x", Map("suffix" -> "-a"))
    assert(error.isEmpty)
    assert(outputs == Seq("data.csv" -> "x-a"))
  }

  test("1:n transformer creates multiple files with adapted names") {
    val (outputs, error) = run(ScalaClassFileTransformer(className = classOf[TestSplitFileTransformer].getName), "a\nb\nc")
    assert(error.isEmpty)
    assert(outputs == Seq("part0.csv" -> "a", "part1.csv" -> "b", "part2.csv" -> "c"))
  }

  test("returned or thrown exceptions are returned as error") {
    val (_, returnedError) = run(ScalaClassFileTransformer(className = classOf[TestFailFileTransformer].getName), "x")
    assert(returnedError.map(_.getMessage).contains("test failure"))
    val (_, thrownError) = run(ScalaClassFileTransformer(className = classOf[TestThrowFileTransformer].getName), "x")
    assert(thrownError.map(_.getMessage).contains("test exception"))
  }

  test("creating no output file is an error") {
    val (outputs, error) = run(ScalaClassFileTransformer(className = classOf[TestSplitFileTransformer].getName), "")
    assert(outputs.isEmpty)
    assert(error.exists(_.getMessage.contains("created no output file")))
  }

  test("invalid and duplicate output file names are an error") {
    val (_, invalidError) = run(ScalaClassFileTransformer(className = classOf[TestCreateFilesTransformer].getName), "", Map("files" -> "sub/x"))
    assert(invalidError.exists(_.getMessage.contains("must not be empty or contain a directory")))
    val (_, duplicateError) = run(ScalaClassFileTransformer(className = classOf[TestCreateFilesTransformer].getName), "", Map("files" -> "x,x.csv"))
    assert(duplicateError.exists(_.getMessage.contains("output file x.csv was already created")))
  }

  test("output streams are closed exactly once") {
    val closeCounts = collection.mutable.Map[String, Int]()
    val transformer = ScalaClassFileTransformer(className = classOf[TestCreateFilesTransformer].getName)
    GenericFileTransformer.transformToFiles(transformer, Map("files" -> "x,y", "close" -> "true"), new ByteArrayInputStream(Array()), "data.csv", identity,
      fileName => new ByteArrayOutputStream() {
        override def close(): Unit = closeCounts.update(fileName, closeCounts.getOrElse(fileName, 0) + 1)
      })
    assert(closeCounts == Map("x" -> 1, "y" -> 1))
  }

  test("sample file gets only the first output file") {
    val sample = new ByteArrayOutputStream()
    val error = GenericFileTransformer.transformToSampleFile(ScalaClassFileTransformer(className = classOf[TestSplitFileTransformer].getName), Map(),
      new ByteArrayInputStream("a\nb".getBytes(StandardCharsets.UTF_8)), "data.csv", () => sample)
    assert(error.isEmpty)
    assert(sample.toString(StandardCharsets.UTF_8) == "a")
  }

  test("failed files are listed in exception") {
    GenericFileTransformer.throwIfTransformationsFailed(Seq(), 2)
    val ex = intercept[IllegalStateException](GenericFileTransformer.throwIfTransformationsFailed(Seq("f1" -> "error1"), 2))
    assert(ex.getMessage.contains("file transformation failed for 1 of 2 files") && ex.getMessage.contains("f1: error1"))
  }

  test("options include evaluated runtimeOptions and executionModeResultOptions") {
    val transformer = ScalaClassFileTransformer(className = classOf[TestAppendFileTransformer].getName, options = Map("suffix" -> "-a"), runtimeOptions = Map("phase" -> "executionPhase"))
    val options = transformer.prepareOptions(actionId, Seq(), Map("emOption" -> "x"))
    assert(options == Map("suffix" -> "-a", "phase" -> context.phase.toString, "emOption" -> "x"))
  }

  test("transform partition values") {
    val transformer = ScalaClassFileTransformer(className = classOf[TestAppendFileTransformer].getName, options = Map("suffix" -> "-a"))
    val pv = PartitionValues(Map("year" -> "2002", "month" -> "12"))
    assert(transformer.transformPartitionValues(actionId, Seq(pv), Map()) == Some(Map(pv -> PartitionValues(Map("dt" -> "200212")))))
  }

  test("ScalaCodeFileTransformer with function") {
    val transformer = ScalaCodeFileTransformer(code = Some(
      """
        |import java.io.{InputStream, OutputStream}
        |(options: Map[String,String], input: InputStream, output: OutputStream) => {
        |  output.write(new String(input.readAllBytes()).toUpperCase.getBytes)
        |  None
        |}
        |""".stripMargin))
    transformer.prepare(actionId)
    val (outputs, error) = run(transformer, "x")
    assert(error.isEmpty)
    assert(outputs == Seq("data.csv" -> "X"))
    assert(transformer.transformPartitionValues(actionId, Seq(PartitionValues(Map("dt" -> "1"))), Map()).isEmpty)
  }

  test("ScalaCodeFileTransformer with CustomFileTransformer implementation") {
    val transformer = ScalaCodeFileTransformer(code = Some(
      """
        |import io.smartdatalake.util.hdfs.PartitionValues
        |import io.smartdatalake.workflow.action.generic.customlogic.{CustomFileTransformer, OutputStreamFactory}
        |import java.io.InputStream
        |new CustomFileTransformer {
        |  override def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception] = {
        |    val content = input.readAllBytes()
        |    outputs.create("a").write(content)
        |    outputs.create("b").write(content)
        |    None
        |  }
        |  override def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] =
        |    Some(partitionValues.map(pv => (pv, PartitionValues(Map("dt" -> "x")))).toMap)
        |}
        |""".stripMargin))
    val (outputs, error) = run(transformer, "x")
    assert(error.isEmpty)
    assert(outputs == Seq("a.csv" -> "x", "b.csv" -> "x"))
    val pv = PartitionValues(Map("dt" -> "1"))
    assert(transformer.transformPartitionValues(actionId, Seq(pv), Map()) == Some(Map(pv -> PartitionValues(Map("dt" -> "x")))))
  }
}

class TestAppendFileTransformer extends CustomFileTransformer {
  override def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] = {
    input.transferTo(output)
    output.write(options("suffix").getBytes(StandardCharsets.UTF_8))
    None
  }
  override def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] =
    Some(partitionValues.map(pv => (pv, PartitionValues(Map("dt" -> (pv("year").toString + pv("month").toString))))).toMap)
}

/**
 * Creates one output file per line of the input, named part<lineNb>.
 */
class TestSplitFileTransformer extends CustomFileTransformer {
  override def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception] = {
    val content = new String(input.readAllBytes(), StandardCharsets.UTF_8)
    if (content.nonEmpty) content.split("\n").zipWithIndex.foreach {
      case (line, idx) => outputs.create(s"part$idx").write(line.getBytes(StandardCharsets.UTF_8))
    }
    None
  }
}

/**
 * Creates the files given by option "files", and closes them if option "close" is set.
 */
class TestCreateFilesTransformer extends CustomFileTransformer {
  override def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception] = {
    options("files").split(",").foreach { name =>
      val os = outputs.create(name)
      if (options.get("close").contains("true")) os.close()
    }
    None
  }
}

class TestFailFileTransformer extends CustomFileTransformer {
  override def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] =
    Some(new IllegalStateException("test failure"))
}

class TestThrowFileTransformer extends CustomFileTransformer {
  override def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] =
    throw new IllegalArgumentException("test exception")
}
