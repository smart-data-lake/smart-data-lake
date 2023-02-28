/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.lab

import io.smartdatalake.util.misc.TryWithRessource
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}
import scala.io.Source

class LabCatalogGeneratorTest extends FunSuite {
  test("generate catalog") {
    val srcDir = "target/generatedSrc"
    val packageName = "ch.smartdatalake.generated"
    val className = "Catalog"
    val config = LabCatalogGeneratorConfig(Seq(getClass.getResource("/dagexporter/dagexporterTest.conf").getPath), srcDir, packageName, className)
    LabCatalogGenerator.generateDataObjectCatalog(config)
    val path = Paths.get(s"$srcDir/${packageName.split('.').mkString("/")}/$className.scala")
    assert(Files.exists(path))
    assert(TryWithRessource.execSource(Source.fromFile(path.toFile)) {
      x => x.getLines.exists(_.contains("dataObjectParquet12"))
    })
  }
}
