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

package io.smartdatalake.util.xml

import org.scalatest.FunSuite

import scala.io.Source

class XsdSchemaConverterTest extends FunSuite {

  test("read basket schema") {
    val xsdContent = Source.fromResource("xmlSchema/basket.xsd").mkString
    val schema = XsdSchemaConverter.read(xsdContent, 10)
    println(schema)
  }

  test("read complex schema with recursion") {
    val xsdContent = Source.fromResource("xmlSchema/complex.xsd").mkString
    val schema = XsdSchemaConverter.read(xsdContent, 10)
    schema.printTreeString()
  }
}
