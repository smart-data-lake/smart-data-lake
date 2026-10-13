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
package io.smartdatalake.util.misc

import org.scalatest.funsuite.AnyFunSuite

class ScaladocUtilTest extends AnyFunSuite {

  private def format(doc: String): String = ScaladocUtil.formatScaladocWithTags(scaladoc.Scaladoc.fromString(doc).toOption.get)

  test("inline code keeps line breaks and adds no spaces") {
    val doc =
      """/**
        | * Options passed by default:
        | *   - isExec: defined as `context.isExecPhase`
        | *   - dataObjectId: id of the [[DataObject]]s input, e.g. `x`.
        | */""".stripMargin
    assert(format(doc) == "Options passed by default:\n- isExec: defined as `context.isExecPhase`\n- dataObjectId: id of the `DataObject`s input, e.g. `x`.")
  }

  test("text before a code block starts a new paragraph") {
    val doc =
      """/**
        | * Use `transform`.
        | *
        | * Example:
        | * {{{
        | * a = {
        | *   b = c
        | * }
        | * }}}
        | */""".stripMargin
    assert(format(doc) == "Use `transform`.\n\nExample:\n```\na = {\n  b = c\n}\n```\n")
  }

  test("parameter with link on a new line") {
    val doc =
      """/**
        | * @param runtimeOptions evaluated against an instance of
        | *                       [[DefaultExpressionData]].
        | */""".stripMargin
    val param = scaladoc.Scaladoc.fromString(doc).toOption.get.tags.collectFirst { case p: scaladoc.Tag.Param => p }.get
    assert(ScaladocUtil.formatScaladocMarkup(param.markup) == "evaluated against an instance of\n`DefaultExpressionData`.")
  }
}
