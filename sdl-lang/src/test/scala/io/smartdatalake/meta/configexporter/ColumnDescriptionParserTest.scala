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
package io.smartdatalake.meta.configexporter

import org.scalatest.funsuite.AnyFunSuite

class ColumnDescriptionParserTest extends AnyFunSuite {

  test("parse column descriptions of a markdown file") {
    val content =
      """# Some DataObject
        |
        |Some free text which is not a column description.
        |
        |## Columns
        |@column a  Description of a
        |@column "b" Description of b,
        |continued on the next line
        |
        |@column `c.c1` Description of a nested column
        |
        |# Next header closes the last description
        |This text is ignored.
        |""".stripMargin
    val descriptions = ColumnDescriptionParser.parseContent(content)
    assert(descriptions("a") == "Description of a")
    assert(descriptions("b") == s"Description of b,${System.lineSeparator()}continued on the next line")
    assert(descriptions("c.c1") == "Description of a nested column")
    assert(descriptions.size == 3)
  }

  test("column names are converted to column paths, dropping array markers") {
    assert(ColumnDescriptionParser.toColumnPath("a") == Seq("a"))
    assert(ColumnDescriptionParser.toColumnPath("c.c1") == Seq("c", "c1"))
    assert(ColumnDescriptionParser.toColumnPath("b.[].b1") == Seq("b", "b1"))
  }
}
