/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.fasterxml.jackson.core.JsonParseException
import ScalaNotebookDfTransformer.{compileCode, downloadNotebook, parseNotebook, prepareFunction}
import org.scalatest.FunSuite
import scalaj.http.Http

import java.net.URLConnection
import scala.io.Source

class ScalaNotebookDfTransformerTest extends FunSuite {

  val testNotebookContent = """
    {
      "metadata" : {
        "config" : {
          "dependencies" : {},
          "exclusions" : [],
          "sparkConfig" : {
            "spark.master" : "local[*]"
          },
          "env" : {}
        },
        "language_info" : {
          "name" : "scala"
        }
      },
      "nbformat" : 4,
      "nbformat_minor" : 0,
      "cells" : [
        {
          "cell_type" : "code",
          "execution_count" : 0,
          "metadata" : {
            "cell.metadata.exec_info" : {
              "startTs" : 1632858122174,
              "endTs" : 1632858122561
            },
            "language" : "scala"
          },
          "language" : "scala",
          "source" : [
            "val t1 = \"test\""
          ],
          "outputs" : [
          ]
        },
        {
          "cell_type" : "code",
          "language" : "scala",
          "source" : [
            "//!IGNORE",
            "val t2 = \"test2\""
          ],
          "outputs" : [
          ]
        },
        {
          "cell_type" : "markdown",
          "source" : [
            "## Test Title"
          ],
          "outputs" : [
          ]
        },
        {
          "cell_type" : "code",
          "execution_count" : 7,
          "language" : "scala",
          "source" : [
            "val t3 = \"test3\""
          ],
          "outputs" : [
          ]
        }
      ]
    }
    """

  val testNotebookContentToCompile = """
    {
      "nbformat" : 4,
      "nbformat_minor" : 0,
      "cells" : [
        {
          "cell_type" : "code",
          "source" : [
            "val t = \"test\""
          ]
        },
        {
          "cell_type" : "code",
          "source" : [
            "def testTransform(spark: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {\r\n",
            "  df\r\n",
            "}"
          ]
        }
      ]
    }
    """

  test("parse notebook") {
    val notebookCode = ScalaNotebookDfTransformer.parseNotebook(testNotebookContent)
    val expectedNotebookCode = """
        |val t1 = "test"
        |val t3 = "test3"
        |""".stripMargin.trim
    assert(notebookCode == expectedNotebookCode)
  }

  test("parse notebook fails if not json") {
    intercept[JsonParseException](ScalaNotebookDfTransformer.parseNotebook("<html></html>"))
  }

  test("prepare function fails if function name is not found in content") {
    intercept[IllegalArgumentException](ScalaNotebookDfTransformer.prepareFunction(ScalaNotebookDfTransformer.parseNotebook(testNotebookContent), "abc"))
  }

  test("compile transform function") {
    val notebookCode = prepareFunction(parseNotebook(testNotebookContentToCompile), "testTransform")
    compileCode(notebookCode)
  }

  ignore("load notebook") {
    // test loading from Polynote installation
    // note that java has some problems to connect to Polynote running in WSL2 over localhost/127.0.0.1
    ScalaNotebookDfTransformer(url = "http://172.17.125.205:8192/notebook/Test.ipynb?download=true", functionName = "testTransform")
  }
}