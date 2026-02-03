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

  def structField2String(sf: StructField): String = s"StructField(name=${sf.name}, dataType=${sf.dataType}," +
    s" nullable=${sf.nullable}, metadata=${sf.metadata})"

  /**
   * creates Struct
   *
   * @param fields : fields as triple (name, data type, is nullable)
   * @return StructType
   */
  def createStruct(fields: Array[(String, DataType, Boolean)]): StructType = StructType(
    fields.map(x => StructField(name = x._1, dataType = x._2: DataType, nullable = x._3))
  )

  /**
   * creates Struct with nullable fields
   *
   * @param fields : nullable fields as pair (name, data type)
   * @return StructType
   */
  def createStruct(fields: Array[(String, DataType)]): StructType = createStruct(
    fields.map { case (fldName, dTyp) => (fldName, dTyp, true) }
  )

  /**
   * creates Struct with one field
   *
   * @param fieldName : name of field
   * @param fieldType : data type of field
   * @param nullable  : is field nullable ?
   * @return StructType
   */
  def createStruct(fieldName: String, fieldType: DataType, nullable: Boolean = true): StructType = createStruct(Array((fieldName, fieldType, nullable)))

  implicit class StructSDLB(st: StructType) {

    val niceString: String = s"StructType(\n ${st.map(structField2String).mkString("\n ")} )"

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
                    ignoreColnameCase: Boolean = true, ignoreMetadata: Boolean = true,
                    showDiff: Boolean = true)(implicit logger: Logger): Boolean = {
      LogUtils.debugLog(s"StructSDLB.equal: this = $niceString")
      LogUtils.debugLog(s"StructSDLB.equal: that = ${that.niceString}")
      LogUtils.debugLog(s"StructSDLB.equal: ignoreColumnOrder = $ignoreColumnOrder , ignoreNullability = $ignoreNullability ," +
        s" ignoreColnameCase = $ignoreColnameCase , showDiff = $showDiff")

      def fieldOrder(f1: StructField, f2: StructField): Boolean = f1.name < f2.name

      def prepare(sf: StructField): StructField = StructField(dataType = sf.dataType,
        name = if (ignoreColnameCase) sf.name.toLowerCase else sf.name,
        nullable = ignoreNullability || sf.nullable,
        metadata = if (ignoreMetadata) Metadata.empty else sf.metadata
      )

      val lSch: Seq[StructField] = st.map(prepare)
      val rSch: Seq[StructField] = that.map(prepare)
      val result = lSch == rSch || (ignoreColumnOrder && lSch.sortWith(fieldOrder) == rSch.sortWith(fieldOrder))

      //if (!result && showDiff) {
      if (true) {
        logger.info("StructSDLB.equal: schemata differ !")
        logger.info(s"ignoreColumnOrder = $ignoreColumnOrder")
        logger.info(s"ignoreNullability = $ignoreNullability")
        logger.info(s"this = $niceString")
        st.printTreeString
        logger.info(s"that = ${that.niceString}")
        that.printTreeString
        logger.info(s"this minus that = ${st.diff(that).map(structField2String).mkString(", ")}")
        logger.info(s"that minus this = ${that.diff(st).map(structField2String).mkString(", ")}")
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
