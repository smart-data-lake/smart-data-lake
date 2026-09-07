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
package io.smartdatalake.workflow.dataframe.plainScala

/**
 * Provenance of a column of a [[ScalaDataFrame]]: which columns its value is calculated from, and how.
 * This is what [[ScalaColumnLineageUtil]] follows to extract the column level lineage of a DataFrame.
 *
 * The plain-Scala engine has no logical plan the lineage could be read from afterwards, as it evaluates every
 * expression immediately: a [[ScalaColumn]] holds data, not an expression tree. The provenance is therefore
 * recorded while a column is created, and it is carried by the [[ScalaColumnDefinition]], which is the part of
 * a column that survives the operations rebuilding a DataFrame from its rows, e.g. filter, distinct or join.
 *
 * A provenance is compared by reference and is therefore the identity of the column carrying it, taking the
 * role of Sparks expression id. Copying a column definition, e.g. to add a DataFrame alias, keeps that
 * identity, as it keeps the same provenance instance.
 *
 * @param references  provenance of the columns this columns value is calculated from.
 * @param isIdentity  true if the value of the referenced column is taken over unchanged, e.g. by a rename.
 * @param description description of the expression calculating the column, if there is one.
 * @param isRoot      true if this is a column of a DataFrame created from data, e.g. read from a DataObject.
 *                    Such a column has no references because its provenance is unknown, whereas a calculated
 *                    column without references is known to read no column at all, e.g. a constant.
 */
class ScalaColumnProvenance(val references: Seq[ScalaColumnProvenance],
                            val isIdentity: Boolean,
                            val description: Option[String],
                            val isRoot: Boolean) {

  // the references are not printed, as the provenance of a column can reference a large part of the DataFrames history
  override def toString: String = s"ScalaColumnProvenance(${references.size} references, isIdentity=$isIdentity, isRoot=$isRoot, description=$description)"
}

object ScalaColumnProvenance {

  /**
   * Provenance of a column of a DataFrame created from data, whose own provenance is unknown.
   * Every call returns a new instance, as a provenance is the identity of the column carrying it.
   */
  def root(): ScalaColumnProvenance = new ScalaColumnProvenance(Seq(), isIdentity = false, None, isRoot = true)

  /**
   * Provenance of a column calculated by an expression from the given columns.
   */
  def calculated(references: Seq[ScalaColumnProvenance], isIdentity: Boolean, description: Option[String]): ScalaColumnProvenance = {
    new ScalaColumnProvenance(references, isIdentity, description, isRoot = false)
  }

  /**
   * Provenance of a column combining the values of the given columns of several DataFrames, as a union does.
   * The value of each of them is taken over unchanged, which makes this an identity.
   */
  def union(references: Seq[ScalaColumnProvenance]): ScalaColumnProvenance = {
    references.distinct match {
      // the very same column on both sides of the union, e.g. a DataFrame united with itself - keep its identity
      case Seq(reference) => reference
      case distinctReferences => new ScalaColumnProvenance(distinctReferences, isIdentity = true, None, isRoot = false)
    }
  }
}
