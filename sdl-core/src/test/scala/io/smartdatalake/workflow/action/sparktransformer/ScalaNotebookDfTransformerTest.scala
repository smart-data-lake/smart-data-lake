/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.sparktransformer

import com.fasterxml.jackson.core.JsonParseException
import org.scalatest.FunSuite
import scalaj.http.{Http, HttpOptions}

import java.io.InputStream

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

  test("load notebook") {
    //ScalaNotebookDfTransformer(url = "http://localhost:8192/notebook/Test.ipynb?download=true", functionName = "test")
    import sun.util.logging.PlatformLogger
    val logger = PlatformLogger.getLogger("sun.net.www.protocol.http.HttpURLConnection")
    logger.setLevel(PlatformLogger.Level.FINEST)
    val request = Http("http://localhost:8192/notebook/Test.ipynb?download=true")
      .option(HttpOptions.followRedirects(true))
      .header("Accept", "application/x-ipynb+json")
      .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36 Edg/94.0.992.37")
      .header("Accept-Encoding", "gzip, deflate, br")
      .header("Accept-Language", "en-US,en;q=0.9")
    println(request.asString)
  }

}
