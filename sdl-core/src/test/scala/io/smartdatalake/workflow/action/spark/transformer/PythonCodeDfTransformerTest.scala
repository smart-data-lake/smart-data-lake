/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.workflow.action.spark.transformer.PythonCodeDfTransformer.dedent
import org.scalatest.FunSuite

class PythonCodeDfTransformerTest extends FunSuite {

	// To ensure consistent comparisons across different environments, we normalize line endings.
	def normalizeLineEndings(text: String): String =
		text.replace("\r\n", "\n")

	test("dedent maintaining relative indentation") {
		val input = """
			def foo():
				print("hi")
				if True:
					print("again")
			"""
		val output = dedent(input)
		val expected = """
			|def foo():
			|	print("hi")
			|	if True:
			|		print("again")
			|""".stripMargin
		assert(normalizeLineEndings(output) == normalizeLineEndings(expected))
	}

	test("ignore empty lines") {
		val input = """

			def foo():


				print("hi")

				print("again")

			"""
		val output = dedent(input)
		val expected = """
			|
			|def foo():
			|
			|
			|	print("hi")
			|
			|	print("again")
			|
			|""".stripMargin
		assert(normalizeLineEndings(output) == normalizeLineEndings(expected))
	}

	test("handle scala margin indicator") {
		val input = """
			|		print("test")
			|"""

		val output = dedent(input)
		val expected = """
			|print("test")""".stripMargin
		assert(normalizeLineEndings(output) == normalizeLineEndings(expected))
	}
}

