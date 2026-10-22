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
package io.smartdatalake.workflow.connection.jdbc

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigParser, InstanceRegistry}
import io.smartdatalake.workflow.dataobject.JdbcTableDataObject
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.reflect.runtime.universe._

/**
 * [[JdbcConnection]] was called [[JdbcTableConnection]] until version 3.0.0. The deprecated name is kept as a separate
 * case class, so that existing configurations keep working and the type stays visible in the schema viewer.
 *
 * Both case classes must offer exactly the same parameters. This test guards the duplication: whenever a parameter
 * is added to, removed from or changed in [[JdbcConnection]], it has to be applied to [[JdbcTableConnection]] as well.
 */
@scala.annotation.nowarn("cat=deprecation")
class DeprecatedJdbcTableConnectionTest extends AnyFunSuite with Matchers {

  private def constructorParams(tpe: Type): Seq[(String, String, Boolean)] = {
    val params = tpe.decls.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor => m.paramLists.head
    }.getOrElse(fail(s"no primary constructor found for $tpe"))
    params.map(p => (p.name.toString, p.typeSignature.toString, p.asTerm.isParamWithDefault))
  }

  test("JdbcTableConnection has the same parameters as JdbcConnection") {
    constructorParams(typeOf[JdbcTableConnection]) shouldBe constructorParams(typeOf[JdbcConnection])
  }

  test("JdbcTableConnection is deprecated and implements the same logic as JdbcConnection") {
    // the Java annotation is the one which is kept at runtime and read by the json schema exporter
    typeOf[JdbcTableConnection].typeSymbol.annotations.exists(_.tree.tpe =:= typeOf[Deprecated]) shouldBe true
    typeOf[JdbcTableConnection] <:< typeOf[JdbcConnectionImpl] shouldBe true
    typeOf[JdbcConnection] <:< typeOf[JdbcConnectionImpl] shouldBe true
  }

  test("JdbcTableConnection is still parsable and usable by a JdbcTableDataObject") {
    val config = ConfigFactory.parseString(
      """
        |connections.jdbc1 {
        |  type = JdbcTableConnection
        |  url = "jdbc:hsqldb:mem:DeprecatedJdbcTableConnectionTest"
        |  driver = org.hsqldb.jdbcDriver
        |  db = test
        |}
        |dataObjects.tbl1 {
        |  type = JdbcTableDataObject
        |  connectionId = jdbc1
        |  table = { name = tbl1 }
        |}
        |""".stripMargin).resolve
    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.get[JdbcTableConnection](ConnectionId("jdbc1")).url shouldBe "jdbc:hsqldb:mem:DeprecatedJdbcTableConnectionTest"
    registry.get[JdbcTableDataObject](DataObjectId("tbl1")).connection shouldBe a[JdbcTableConnection]
  }
}
