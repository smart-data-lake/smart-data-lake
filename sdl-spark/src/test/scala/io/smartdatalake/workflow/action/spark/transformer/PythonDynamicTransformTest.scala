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
package io.smartdatalake.workflow.action.spark.transformer

import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.sys.process._
import scala.util.Try

/**
 * Tests the Python code which maps the parameters of a Python transform function dynamically, see
 * [[PythonDynamicTransform]].
 *
 * The test executes a Python script which extracts the Python code from the Scala source file and runs it against
 * fake DataFrames. It is skipped if no Python interpreter is available, as a Python environment is not needed to
 * build SDLB.
 */
class PythonDynamicTransformTest extends AnyFunSuite {

  private val scalaSourceFile = "src/main/scala/io/smartdatalake/workflow/action/spark/transformer/PythonDynamicTransform.scala"
  private val pythonTestFile = "src/test/python/test_python_dynamic_transform.py"

  test("Python transform function parameters are mapped dynamically") {
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
