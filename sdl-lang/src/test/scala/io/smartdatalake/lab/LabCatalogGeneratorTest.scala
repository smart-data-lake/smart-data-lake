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
package io.smartdatalake.lab

import io.smartdatalake.util.misc.{CustomCodeUtil, SmartDataLakeLogger}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}
import _root_.scala.util.{Failure, Success, Try, Using}
import scala.io.Source

class LabCatalogGeneratorTest extends AnyFunSuite with SmartDataLakeLogger {

  def getCodeFromFile(f: File): String = Using.resource(Source.fromFile(f)) {
    x =>
      x.getLines().dropWhile(!_.contains("import")) // remove 'package' statement for compilation below
        .mkString(System.lineSeparator())
  }

  test("generate catalog") {
    val srcDir = "target/generatedSrc"
    val packageName = "ch.smartdatalake.generated"
    val dataObjectCatalogClassName = "MyDataObjectCatalog"
    val actionCatalogClassName = "MyActionCatalog"
    val config = LabCatalogGeneratorConfig(Seq(getClass.getResource("/dagexporter/dagexporterTest.conf").getPath), srcDir, packageName,
      dataObjectCatalogClassName, actionCatalogClassName)
    LabCatalogGenerator.generateCatalogs(config)

    val pathCatalog = Paths.get(s"$srcDir/${packageName.split('.').mkString("/")}/$dataObjectCatalogClassName.scala")
    val catalogCodeDOcatalog = getCodeFromFile(pathCatalog.toFile)
    val codeCatalog = s"""
        $catalogCodeDOcatalog
        classOf[$dataObjectCatalogClassName]
      """

    logger.debug("test DataObjectCatalog")
    Try {
      assert(Files.exists(pathCatalog))
      assert(catalogCodeDOcatalog.contains("dataObjectParquet12"))
      logger.debug("check compilation")
      CustomCodeUtil.compileCode[Class[Product]](codeCatalog)(logger)
    } match {
      case Success(_) => logger.debug("check compilation succeeded")
      case Failure(e) =>
        println()
        logger.error("!!! testing DataObjectCatalog FAILED !!!")
        logger.error(s"srcDir = $srcDir , packageName = $packageName , pathCatalog = $pathCatalog ," +
          s" dataObjectCatalogClassName = $dataObjectCatalogClassName , actionCatalogClassName = $actionCatalogClassName")
        logger.error(s"config : ${config.toDebugString}")
        logger.error(s"codeCatalog : $codeCatalog")
        throw e
    }

    val pathAction = Paths.get(s"$srcDir/${packageName.split('.').mkString("/")}/$actionCatalogClassName.scala")
    val catalogCodeAction = getCodeFromFile(pathAction.toFile)
    val codeAction = s"""
        $catalogCodeAction
        classOf[$actionCatalogClassName]
      """
    logger.debug(s"test ActionCatalog: pathAction = $pathAction")
    Try {
      assert(Files.exists(pathAction))
      assert(codeAction.contains("actionId1"))
      logger.debug("check compilation")
      CustomCodeUtil.compileCode[Class[Product]](codeAction)(logger)
    } match {
      case Success(_) => logger.debug("check compilation succeeded")
      case Failure(e) =>
        println()
        logger.error("!!! testing ActionCatalog FAILED !!!")
        logger.error(s"srcDir = $srcDir , packageName = $packageName , pathAction = $pathAction ," +
          s" dataObjectCatalogClassName = $dataObjectCatalogClassName , actionCatalogClassName = $actionCatalogClassName")
        logger.error(s"config : ${config.toDebugString}")
        logger.error(s"codeAction : $codeAction")
        throw e
    }
  }
}
