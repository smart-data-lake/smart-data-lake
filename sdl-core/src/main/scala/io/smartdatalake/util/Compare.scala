/*
 * sdl-core - Build your data lake the smart way.
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

import io.smartdatalake.util.Constants.{epsilonDouble, epsilonFloat}

/**
 * Trait Compare provides methods to compare numeric values, sets and maps
 */
trait Compare extends Serializable {

  import OriginTag._

  /**
   * originMap returns the union of two sets where the elements are tagged with their origin
   * However the method here returns each element once only using the third flag LeftAndRight for
   * elements of the intersestion
   * originMap is needed to compare to maps, cf. mapAlmostSymDiff
   *
   * @param sL left set of type A
   * @param sR right set of type A
   * @tparam A type of elements
   * @return map tagging the elements of sl union sR with their origin: LeftAndRight, Left or Right
   */
  final def originMap[A](sL: Set[A], sR: Set[A]): Map[A, OriginTag] = sL.intersect(sR)
    .map(x => (x, LeftAndRight)).toMap ++
    sL.diff(sR).map(x => (x, FromLeft)).toMap ++ sR.diff(sL).map(x => (x, FromRight)).toMap

  /**
   * checks whether 2 values are almost equal
   *
   * @param epsilon  maximal difference to be considered for equality
   * @param x        first value
   * @param y        second value
   * @param relative if true then comparison is done with respect to value of the first argument
   * @param num      implicit proof that type A is numeric
   * @tparam A type of values x and y
   * @return boolean telling you whether x and y are considered to equal
   */
  final def almostEqual[A](epsilon: A, x: A, y: A, relative: Boolean = false)
                          (implicit num: Numeric[A]): Boolean = {
    val maxDiff = if (relative) num.times(num.abs(x), epsilon) else epsilon
    num.lt(num.abs(num.minus(x, y)), maxDiff)
  }


  /**
   * checks whether the two elements of a value pair are almost equal
   *
   * @param epsilon  maximal difference to be considered for equality
   * @param xy       pair of values
   * @param relative if true then comparison is done with respect to the first element
   * @param num      implicit proof that type A is numeric
   * @tparam A type of elements x and y
   * @return boolean telling you whether the two elements are considered to equal
   */
  final def almostEqual[A](epsilon: A, relative: Boolean)
                          (xy: (A, A))
                          (implicit num: Numeric[A]): Boolean = almostEqual[A](epsilon, xy._1, xy._2, relative)(num)

  final def anyEqual(x: Any, y: Any): Boolean = x match {
    // Numeric values
    case x: Double => almostEqual(epsilonDouble, x, y.asInstanceOf[Double]) ||
      (x.isNaN && y.asInstanceOf[Double].isNaN) ||
      (x.isPosInfinity && y.asInstanceOf[Double].isPosInfinity) ||
      (x.isNegInfinity && y.asInstanceOf[Double].isNegInfinity)
    case x: Float => almostEqual(epsilonFloat, x, y.asInstanceOf[Float]) ||
      (x.isNaN && y.asInstanceOf[Float].isNaN) ||
      (x.isPosInfinity && y.asInstanceOf[Float].isPosInfinity) ||
      (x.isNegInfinity && y.asInstanceOf[Float].isNegInfinity)
    case x: Array[_] => y match {
      case y: Array[_] => (x.length == y.length) && x.zip(y).forall(xy => anyEqual(xy._1, xy._2))
      case _ => false
    }
    // optional values
    case x: Option[_] => y match {
      case y: Option[_] => (x.isEmpty && y.isEmpty) || x.zip(y).exists(xy => anyEqual(xy._1, xy._2))
      case _ => false
    }
    // collections
    case x: Seq[_] => y match {
      case y: Seq[_] => (x.size == y.size) && x.zip(y).forall(xy => anyEqual(xy._1, xy._2))
      case _ => false
    }
    case x: (_, _) => y match {
      case y: (_, _) => anyEqual(x._1, y._1) && anyEqual(x._2, y._2)
      case _ => false
    }
    case _ => x == y
  }


  /**
   * mapAlmostSymDiff compares two maps. Two maps differ when a key belongs to one keySet only,
   * or if it belongs to both keySets but its values differ.
   *
   * @param epsilon : threshold up to two values are considered almost equal
   * @param f       : left function to compare with g
   * @param g       : right function to compare with f
   * @param num     : proof that V is a numeric type
   * @tparam K : type of the keys
   * @tparam V : type of the values
   * @return map containing the keys and both optional values where f and g differ
   */
  final def mapAlmostSymDiff[K, V](epsilon: V)
                                  (f: Map[K, V], g: Map[K, V])
                                  (implicit num: Numeric[V]): Map[K, (OriginTag, Option[V], Option[V])] = originMap(f.keySet, g.keySet)
    .map { case (vK, vFound) => (vK, (vFound, f.get(vK), g.get(vK))) }
    .filterNot { case (_, (vFound, vF, vG)) => vFound == LeftAndRight && almostEqual(epsilon, vF.get, vG.get) }

  /**
   * checks wheter 2 values of type A are ordered as desired
   * note that ∀ p,q:Bool . ¬(p=¬q) = (p=q) (used here in case of ¬strict)
   *
   * @param increasing whether x is supposed to be smaller (or equal) xNext
   * @param strict     whether equality is good
   * @param x          first value
   * @param xNext      second value
   * @param ord        which ordering to use
   * @tparam A type of values
   * @return boolean value indicating whether they are ordered
   */
  final def isOrdered[A](increasing: Boolean, strict: Boolean)(x: A, xNext: A)(implicit ord: Ordering[A]): Boolean = strict ==
    (x != xNext && ord.lt(x, xNext) == (increasing == strict))

  final def isOrdered[A](increasing: Boolean, strict: Boolean, valuePair: (A, A))(implicit ord: Ordering[A]): Boolean = isOrdered(increasing, strict)(valuePair._1, valuePair._2)(ord)
}
