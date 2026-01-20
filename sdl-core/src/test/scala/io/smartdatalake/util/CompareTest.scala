package io.smartdatalake.util

import io.smartdatalake.util.Constants.{epsilonDouble, halfDouble}
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
    val argExpMap = Map(
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

}
