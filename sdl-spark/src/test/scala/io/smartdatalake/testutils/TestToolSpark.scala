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
package io.smartdatalake.testutils

import org.apache.spark.sql.Dataset
import org.slf4j.Logger

trait TestToolSpark extends GenericTestTool {

  /**
   * testArgumentExpectedMapWithComment overrides GenericTestTool.testArgumentExpectedMapWithComment
   * to add case Dataset to pattern matching in logFailureObject
   *
   * @param experiendum
   *   map you want to test
   * @param argExpMapComm
   *   map of (comment, input) -> expected output of provided map
   * @param logger
   *   to write nice messages
   * @tparam K
   *   type of input values of map to test
   * @tparam V
   *   type of output values of map to test
   * @return
   *   booleans which indicate whether tests were successful
   */
  override def testArgumentExpectedMapWithComment[K, V](
      experiendum: K => V,
      argExpMapComm: Map[(String, K), V]
  )(implicit logger: Logger): Map[(String, K), Boolean] = {

    def logFailureObject(argName: String, x: Any): Unit = {
      val printPrefix = s"   ${argName.padTo(8, " ").mkString("")} = "
      x match {
        case df: Dataset[_] =>
          logger.error(printPrefix)
          df.show(false)
        case x: Array[_] => logger.error(s"$printPrefix${x.mkString(", ")}")
        case x: Seq[_]   => logger.error(s"$printPrefix${x.mkString(", ")}")
        // case x: scala.collection.GenSeq[_] => logger.error(s"$printPrefix${x.mkString(", ")}")
        case _ => logger.error(s"$printPrefix${x.toString}")
      }
    }

    def logFailure(argument: K, actual: V, expected: V, comment: String): Unit = {
      logger.error("Test case failed !")
      logFailureObject("argument", argument)
      logFailureObject("actual", actual)
      logFailureObject("expected", expected)
      if (comment.nonEmpty) logFailureObject("comment", comment)
    }

    def checkKey(x: (String, K)): Boolean = x match {
      case (comment, argument) =>
        val actual = experiendum(argument)
        val expected = argExpMapComm(x)
        val resultat = anyEqual(actual, expected)
        if (!resultat) logFailure(argument, actual, expected, comment)
        resultat
      case _ => throw new Exception(s"Something went wrong: checkKey called with parameter x=$x")
    }

    argExpMapComm.map { case (ck, _) => (ck, checkKey(ck)) }
  }

}
