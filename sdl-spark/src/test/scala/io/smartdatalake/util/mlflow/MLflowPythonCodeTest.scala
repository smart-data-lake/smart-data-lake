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
package io.smartdatalake.util.mlflow

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.sys.process._
import scala.util.Try

/**
 * Tests the python code talking to MLflow, see [[MLflowPythonCode]].
 *
 * The interesting part runs a python script which extracts the python code from the Scala source file, checks that
 * every snippet compiles and executes the helper functions against a fake mlflow module. It needs a python
 * interpreter but no mlflow installation, and is skipped if no interpreter is available, as a python environment is
 * not needed to build SDLB.
 */
class MLflowPythonCodeTest extends AnyFunSuite {

  private val scalaSourceFile = "src/main/scala/io/smartdatalake/util/mlflow/MLflowPythonCode.scala"
  private val pythonTestFile = "src/test/python/test_mlflow_python.py"

  test("no configuration value is interpolated into the python code") {
    // the code must stay static so that it is testable and values like a model name containing a quote can not
    // break it. Everything is read from the options dict instead, see MLflowPythonCode.
    val allCode = Seq(MLflowPythonCode.preludeCode, MLflowPythonCode.getOrCreateExperimentCode,
      MLflowPythonCode.trainPreludeCode, MLflowPythonCode.trainPostludeCode, MLflowPythonCode.predictCode,
      MLflowPythonCode.getLatestRunInfoCode)
    val source = scala.io.Source.fromFile(scalaSourceFile)
    val sourceText = try source.mkString finally source.close()
    assert(!sourceText.contains("s\"\"\""), "MLflowPythonCode must not use interpolated string literals")
    // every run info key used in the python code must be known to MLflowRunInfo
    val usedKeys = "'(experimentId|experimentName|modelName|runId|runName|duration|date|artifactPath|modelUri|estimatorName)'".r
      .findAllMatchIn(allCode.mkString).map(_.group(1)).toSet
    assert(usedKeys.diff(MLflowRunInfo.fields.toSet).isEmpty)
    assert(MLflowRunInfo.fields.toSet.diff(usedKeys).isEmpty, "MLflowRunInfo declares keys the python code never sets")
  }

  test("the python code compiles and its helpers behave as expected") {
    val pythonCmd = Seq("python3", "python").find(cmd => Try(Seq(cmd, "--version").! == 0).getOrElse(false))
    assume(pythonCmd.isDefined, "no Python interpreter found")
    // the working directory of the test is the module directory
    assume(new File(scalaSourceFile).exists(), s"$scalaSourceFile not found, working directory is ${new File(".").getAbsolutePath}")
    val output = new StringBuilder
    val logger = ProcessLogger(line => output.append(line).append(System.lineSeparator))
    val returnCode = Seq(pythonCmd.get, pythonTestFile, scalaSourceFile) ! logger
    assert(returnCode == 0, s"Python test failed:${System.lineSeparator}$output")
  }
}
