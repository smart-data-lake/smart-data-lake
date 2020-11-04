/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FakeV2SessionCatalog, FunctionRegistry, SimpleAnalyzer, SimpleFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class ExpressionEvaluator[T<:Product:TypeTag,R:TypeTag](exprCol: Column)(implicit classTagR: ClassTag[R]) {

  // prepare evaluator (this is Spark internal API)
  private val encoder = Encoders.product[T].asInstanceOf[ExpressionEncoder[T]]
  private val rowSerializer = encoder.createSerializer()
  private val expr = {
    val attributes = encoder.schema.toAttributes
    val localRelation = LocalRelation(attributes)
    val rawPlan = Project(Seq(exprCol.alias("test").named),localRelation)
    val resolvedPlan = ExpressionEvaluator.MySimpleAnalyzer.execute(rawPlan).asInstanceOf[Project]
    val resolvedExpr = resolvedPlan.projectList.head
    BindReferences.bindReference(resolvedExpr, attributes)
  }

  // check if resulting datatype matches
  if (classTagR.runtimeClass != classOf[Any]) {
    val resultDataType = ExpressionEncoder[R].schema.head.dataType
    require(expr.dataType == resultDataType, s"expression result data type ${expr.dataType} does not match requested datatype $resultDataType")
  }

  // evaluate expression on object
  def apply(v: T): R = {
    val row = rowSerializer.apply(v)
    expr.eval(row).asInstanceOf[R]
  }
}

object ExpressionEvaluator {
  // create a simple catalyst analyzer supporting builtin functions
  private val sqlConf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true)
  private val simpleCatalog = new SessionCatalog( new InMemoryCatalog, FunctionRegistry.builtin, sqlConf) {
    override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {}
  }
  object MySimpleAnalyzer extends Analyzer(new CatalogManager(sqlConf, FakeV2SessionCatalog, simpleCatalog), sqlConf)
}