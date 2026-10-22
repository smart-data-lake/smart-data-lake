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
package io.smartdatalake.app

import io.smartdatalake.testutils.sql.SQLTestUtil
import org.scalatest.Outcome
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/**
 * End-to-end test of a feed mixing the Spark and the SQL engine, see `mixedEngines/application.conf`:
 * Spark loads a csv file into a DuckDB table, the SQL engine creates a view on it, and Spark exports the view.
 *
 * It lives in sdl-sql, as sdl-sql has sdl-spark as test dependency. It can not be placed in sdl-lang, which has
 * sdl-spark and sdl-sparkconnect on the classpath.
 */
class MixedEnginesTest extends AnyFunSuite {

  override def withFixture(test: NoArgTest): Outcome = {
    val reason = SQLTestUtil.pythonUnavailableReason
    assume(reason.isEmpty, reason.getOrElse(""))
    super.withFixture(test)
  }

  private def writeCsv(dir: Path, lines: String*): Unit = {
    Files.createDirectories(dir)
    Files.write(dir.resolve("customers.csv"), ("id,name,city" +: lines).asJava, StandardCharsets.UTF_8)
  }

  private def readCsv(dir: Path): Seq[String] = dir.toFile.listFiles().toSeq
    .filter(f => f.getName.startsWith("part-") && f.getName.endsWith(".csv"))
    .flatMap(f => Files.readAllLines(f.toPath, StandardCharsets.UTF_8).asScala)
    .filterNot(_ == "id,name").sorted

  private def config(tempDir: Path, name: String, overwrite: Map[String, String] = Map()): SmartDataLakeBuilderConfig =
    SmartDataLakeBuilderConfig(feedSel = "mixed", configuration = Seq("cp:/mixedEngines/application.conf"),
      configurationValueOverwrite = Map("env.tempDir" -> tempDir.toString, "env.duckdbUrl" -> SQLTestUtil.createDuckDbUrl(name)) ++ overwrite)

  test("Spark loads a table, the SQL engine creates a view on it, and Spark reads the view") {
    val tempDir = Files.createTempDirectory("mixed-engines")
    writeCsv(tempDir.resolve("src"), "1,bob,Bern", "2,ann,Basel", "3,joe,Bern")
    val sdlConfig = config(tempDir, "MixedEnginesTest")

    // first run: neither the table nor the view exist. The schema is passed on from the SQL engine to Spark in init phase.
    new SmartDataLakeBuilder {}.run(sdlConfig)
    assert(readCsv(tempDir.resolve("export")) == Seq("1,BOB", "3,JOE"))

    // second run with new data: the table is overwritten, and the view is replaced
    writeCsv(tempDir.resolve("src"), "1,bob,Bern", "4,kim,Bern")
    new SmartDataLakeBuilder {}.run(sdlConfig)
    assert(readCsv(tempDir.resolve("export")) == Seq("1,BOB", "4,KIM"))
  }

  test("schemaMin given as Spark schema is validated for a DataFrame of the SQL engine") {
    val tempDir = Files.createTempDirectory("mixed-engines")
    writeCsv(tempDir.resolve("src"), "1,bob,Bern")
    new SmartDataLakeBuilder {}.run(config(tempDir, "MixedEnginesTestSchemaMin",
      Map("dataObjects.btl-customers-bern.schemaMin" -> "id int, name string")))
    assert(readCsv(tempDir.resolve("export")) == Seq("1,BOB"))
    // a missing column
    val ex = intercept[Exception](new SmartDataLakeBuilder {}.run(config(tempDir, "MixedEnginesTestSchemaMinMissing",
      Map("dataObjects.btl-customers-bern.schemaMin" -> "id int, name string, zip string"))))
    assert(Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).exists(_.getMessage.contains("missingCols=zip")))
  }
}
