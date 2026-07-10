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
package io.smartdatalake.util

import io.smartdatalake.testutils.GenericTestTool
import io.smartdatalake.util.Constants.{epsilonDouble, halfDouble}
import io.smartdatalake.util.OriginTag.{FromLeft, FromRight, LeftAndRight, OriginTag}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

class CompareTest extends AnyFlatSpec with Matchers with GenericTestTool {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  "anyEqual" should "check equality properly" in {
    val argExpMap: Map[(String, (Any, Any)), Boolean] = Map(
      // comparing 0 with 1
      ("0=1",                                            (0, 0))                                               -> true,
      ("0=1",                                            (0, 1))                                               -> false,
      ("a double does not equal an integer",             (0d, 0))                                              -> false,
      ("a double does not equal a float",                (0d, 0f))                                             -> false,
      ("2 equal strings",                                ("abc", "abc"))                                       -> true,
      ("control characters ignored",                     ("", (0 to 31).map(_.toChar).mkString))               -> true,
      ("control characters ignored but not space",       ("", (0 to 32).map(_.toChar).mkString))               -> false,
      ("2 equal strings with different line seperators", (s"ab${13.toChar}${10.toChar}c", s"ab${13.toChar}c")) -> true,
      ("2 equal arrays",                                 (Array(2), Array(2)))                                 -> true,
      ("2 equal options",                                (Some(2), Some(2)))                                   -> true,
      ("2 equal sequences",                              (Seq(2), Seq(2)))                                     -> true,
      ("2 equal pairs",                                  ((1, 2), (1, 2)))                                     -> true,
      ("a map is not a string",                          (Map(4 -> 6), "this is not a map"))                   -> false,
      ("null equals null, contrary to SQL !",            (null, null))                                         -> true
    )
    val testFun: ((Any, Any)) => Boolean = {
      case (x, y) => anyEqual(x = x, y = y)
    }
    testArgumentExpectedMapWithComment[(Any, Any), Boolean](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "Given twice -half, almostEqual" should "return true" in {
    almostEqual(epsilonDouble, 0d - halfDouble, 0d - halfDouble) shouldEqual true
  }

  "Given 100000d and 99999d relative almostEqual" should "return true" in {
    almostEqual(epsilonDouble, 100000d, 99999d, relative = true) shouldEqual true
  }

  "Given 100000d and 99999d irrelative almostEqual" should "return true" in {
    almostEqual(epsilonDouble, 100000d, 99999d) shouldEqual false
  }

  "isOrdered" should "check the order properly" in {
    val argExpMap: Map[(String, (Boolean, Boolean, Int, Int)), Boolean] = Map(
      // comparing 0 with 1
      ("0≥1", (false, false, 0, 1)) -> false,
      ("0>1", (false, true, 0, 1))  -> false,
      ("0≤1", (true, false, 0, 1))  -> true,
      ("0<1", (true, true, 0, 1))   -> true,
      // comparing 2 with itself
      ("2≥2", (false, false, 2, 2)) -> true,
      ("2>2", (false, true, 2, 2))  -> false,
      ("2≤2", (true, false, 2, 2))  -> true,
      ("2<2", (true, true, 2, 2))   -> false,
      // comparing 4 with 3
      ("4≥3", (false, false, 4, 3)) -> true,
      ("4>3", (false, true, 4, 3))  -> true,
      ("4≤3", (true, false, 4, 3))  -> false,
      ("4<3", (true, true, 4, 3))   -> false
    )
    val testFun: ((Boolean, Boolean, Int, Int)) => Boolean = {
      case (i, s, x, xn) => isOrdered[Int](increasing = i, strict = s)(x = x, xNext = xn)
    }
    testArgumentExpectedMapWithComment[(Boolean, Boolean, Int, Int), Boolean](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "originMap" should "return a tagged union of two sets" in {
    val emptySet: Set[Double] = Set.empty
    val emptyDiff: Map[Double, OriginTag] = Map.empty
    val argExpMap = Map(
      ("union of empty with itself",            (emptySet, emptySet)) -> emptyDiff,
      ("union of an inhabited set with itself",
        (Set(0d, 1d), Set(0d, 1d))) -> Map(0d -> LeftAndRight, 1d -> LeftAndRight),
      ("union of empty and an inhabited set with itself",
        (emptySet, Set(0d, 1d))) -> Map(0d -> FromRight, 1d -> FromRight),
      ("union of two inhabited sets",
        (Set(0d, 1d), Set(0d, 2d))) -> Map(0d -> LeftAndRight, 1d -> FromLeft, 2d -> FromRight)
    )
    val testFun: ((Set[Double], Set[Double])) => Map[Double, OriginTag] = {
      case (sL, sR) => originMap[Double](sL, sR)
    }
    testArgumentExpectedMapWithComment[(Set[Double], Set[Double]), Map[Double, OriginTag]](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

  "mapAlmostSymDiff" should "return symmetric difference of 2 maps with respect to almost equal" in {
    type mapType = Map[Double, Double]
    type diffType = Map[Double, (OriginTag, Option[Double], Option[Double])]
    val emptyMap: mapType = Map.empty
    val emptyDiff: diffType = Map.empty
    val argExpMap: Map[(String, (mapType, mapType)), diffType] = Map(
      ("comparing empty Map with itself",     (emptyMap, emptyMap))           -> emptyDiff,
      ("comparing squareRootMap with itself", (squareRootMap, squareRootMap)) -> emptyDiff,
      ("comparing squareRootMap with empty",  (squareRootMap, emptyMap))      ->
        squareRootMap.map { case (k, v) => (k, (FromLeft, Some(v), None)) },
      ("comparing 2 simple maps with different value at 0",
        (Map(0d -> 0d, 1d -> 1d, 42d -> 42d),
          Map(0d -> epsilonDouble, 2d -> 2d, 42d -> 42d))) ->
        Map(0d -> (LeftAndRight, Some(0d), Some(epsilonDouble)),
          1d   -> (FromLeft, Some(1d), None),
          2d   -> (FromRight, None, Some(2d)))
    )
    val testFun: ((mapType, mapType)) => diffType = {
      case (f, g) => mapAlmostSymDiff[Double, Double](epsilonDouble)(f, g)
    }
    testArgumentExpectedMapWithComment[(mapType, mapType), diffType](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

}
