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

import io.smartdatalake.util.misc.{CustomCodeUtil, ScalaUtil}
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.Using

class LabCatalogGeneratorTest extends FunSuite {
  test("generate catalog") {
    val srcDir = "target/generatedSrc"
    val packageName = "ch.smartdatalake.generated"
    val dataObjectCatalogClassName = "MyDataObjectCatalog"
    val actionCatalogClassName = "MyActionCatalog"
    val config = LabCatalogGeneratorConfig(Seq(getClass.getResource("/dagexporter/dagexporterTest.conf").getPath), srcDir, packageName, dataObjectCatalogClassName, actionCatalogClassName)
    LabCatalogGenerator.generateCatalogs(config)

    // test DataObjectCatalog
    {
      val path = Paths.get(s"$srcDir/${packageName.split('.').mkString("/")}/$dataObjectCatalogClassName.scala")
      assert(Files.exists(path))
      val catalogCode = Using.resource(Source.fromFile(path.toFile)) {
        x => x.getLines()
          .dropWhile(!_.contains("import")) // remove 'package' statement for compilation below
          .mkString(System.lineSeparator())
      }
      assert(catalogCode.contains("dataObjectParquet12"))
      // check compilation
      CustomCodeUtil.compileCode[Class[Product]](s"""
        $catalogCode
        classOf[$dataObjectCatalogClassName]
      """)
    }

    // test ActionCatalog
    {
      val path = Paths.get(s"$srcDir/${packageName.split('.').mkString("/")}/$actionCatalogClassName.scala")
      assert(Files.exists(path))
      val catalogCode = Using.resource(Source.fromFile(path.toFile)) {
        x => x.getLines()
          .dropWhile(!_.contains("import")) // remove 'package' statement for compilation below
          .mkString(System.lineSeparator())
      }
      assert(catalogCode.contains("actionId1"))
      // check compilation
      CustomCodeUtil.compileCode[Class[Product]](s"""
        $catalogCode
        classOf[$actionCatalogClassName]
      """)
    }
  }
}
