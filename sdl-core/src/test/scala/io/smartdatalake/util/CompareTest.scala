package io.smartdatalake.util

import io.smartdatalake.testutils.TestTool
import io.smartdatalake.util.Constants.{epsilonDouble, halfDouble}
import io.smartdatalake.util.OriginTag.{FromLeft, FromRight, LeftAndRight, OriginTag}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

class CompareTest extends AnyFlatSpec with Matchers with TestTool {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)

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
      ("0≥1", (false, false, 0, 1)) -> false, ("0>1", (false, true, 0, 1)) -> false,
      ("0≤1", (true, false, 0, 1)) -> true, ("0<1", (true, true, 0, 1)) -> true,
      // comparing 2 with itself
      ("2≥2", (false, false, 2, 2)) -> true, ("2>2", (false, true, 2, 2)) -> false,
      ("2≤2", (true, false, 2, 2)) -> true, ("2<2", (true, true, 2, 2)) -> false,
      // comparing 4 with 3
      ("4≥3", (false, false, 4, 3)) -> true, ("4>3", (false, true, 4, 3)) -> true,
      ("4≤3", (true, false, 4, 3)) -> false, ("4<3", (true, true, 4, 3)) -> false)
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
      ("union of empty with itself", (emptySet, emptySet)) -> emptyDiff,
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
      ("comparing empty Map with itself", (emptyMap, emptyMap)) -> emptyDiff,
      ("comparing squareRootMap with itself", (squareRootMap, squareRootMap)) -> emptyDiff,
      ("comparing squareRootMap with empty", (squareRootMap, emptyMap)) ->
        squareRootMap.map { case (k, v) => (k, (FromLeft, Some(v), None)) },
      ("comparing 2 simple maps with different value at 0",
        (Map(0d -> 0d, 1d -> 1d, 42d -> 42d),
          Map(0d -> epsilonDouble, 2d -> 2d, 42d -> 42d))) ->
        Map(0d -> (LeftAndRight, Some(0d), Some(epsilonDouble)),
          1d -> (FromLeft, Some(1d), None),
          2d -> (FromRight, None, Some(2d)))
    )
    val testFun: ((mapType, mapType)) => diffType = {
      case (f, g) => mapAlmostSymDiff[Double, Double](epsilonDouble)(f, g)
    }
    testArgumentExpectedMapWithComment[(mapType, mapType), diffType](testFun, argExpMap)
      .values.forall(identity[Boolean]) shouldBe true
  }

}
