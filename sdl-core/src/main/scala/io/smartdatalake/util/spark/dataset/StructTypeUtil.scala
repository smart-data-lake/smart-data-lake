/*
 * Smart Data Lake - Build your data lake the smart way.
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

package io.smartdatalake.util.spark.dataset

import io.smartdatalake.util.LogUtils
import org.apache.spark.sql.types._
import org.slf4j.Logger

trait StructTypeUtil {

  implicit class StructSDLB(st: StructType) {

    /**
     *
     * Returns whether two schemata equal
     *
     * @param that              the right schema
     * @param ignoreColumnOrder whether or not to ignore the order of columns
     * @param ignoreNullability whether or not to ignore nullability of fields
     * @param showDiff          whether or not to log a diff
     * @return whether or not the schemata are equal
     */
    final def equal(that: StructType,
                    ignoreColumnOrder: Boolean = true, ignoreNullability: Boolean = true,
                    showDiff: Boolean = true)(implicit logger: Logger): Boolean = {
      LogUtils.debugLog(s"schemataEqual: st  =  ${st.catalogString}")
      LogUtils.debugLog(s"schemataEqual: rightSchema = ${that.catalogString}")
      LogUtils.debugLog(s"schemataEqual: ignoreColumnOrder = $ignoreColumnOrder , ignoreNullability = $ignoreNullability , showDiff = $showDiff")

      def fieldOrder(f1: StructField, f2: StructField): Boolean = f1.name < f2.name

      def makeNullableIfIgnored(sf: StructField): StructField = StructField(sf.name, sf.dataType, ignoreNullability || sf.nullable, sf.metadata)

      val lSch = st.map(makeNullableIfIgnored)
      val rSch = that.map(makeNullableIfIgnored)
      val result = lSch == rSch || (ignoreColumnOrder && lSch.sortWith(fieldOrder) == rSch.sortWith(fieldOrder))

      if (!result && showDiff) {
        logger.info("schemataEqual: schemata differ !")
        logger.info(s"ignoreColumnOrder = $ignoreColumnOrder")
        logger.info(s"ignoreNullability = $ignoreNullability")
        logger.info(s"st  = ${st.mkString(", ")}")
        st.printTreeString
        logger.info(s"rightSchema = ${that.mkString(", ")}")
        that.printTreeString
        logger.info(s"st minus rightSchema = ${st.diff(that).mkString(", ")}")
        logger.info(s"rightSchema minus st = ${that.diff(st).mkString(", ")}")
      }
      result
    }


    /**
     * checks whether schema is subschema of given [[StructType]].
     *
     * @param scm to test
     * @return result whether this is subset of provided schema
     */
    final def isSubSetOf(scm: StructType): Boolean = st.toSet.subsetOf(scm.toSet)

    /**
     * checks whether schema is superschema of given [[StructType]].
     *
     * @param scm to test
     * @return result whether this is superset of provided schema
     */
    final def isSuperSetOf(scm: StructType): Boolean = scm.toSet.subsetOf(st.toSet)

  }
  
}
