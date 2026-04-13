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
package io.smartdatalake.testutils.spark.dataset

import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import org.apache.spark.sql.Dataset
import org.slf4j.Logger

trait TestToolDataset extends io.smartdatalake.util.spark.dataset.Equality {

  def printFailedTestResult[T](testName: String, arguments: Seq[Dataset[T]] = Nil)
                              (actual: Dataset[T])(expected: Dataset[T])
                              (implicit logger: Logger): Unit = {
    def printDf(df: Dataset[T]): Unit = {
      logger.error(df.schema.simpleString)
      df.printSchema()
      df.orderBy(df.columns.head, df.columns.tail: _*).show(false)
    }

    logger.error(s"!!!! Test $testName Failed !!!")
    logger.error("   Arguments ")
    arguments.foreach(printDf)
    logger.error("   Actual ")
    printDf(actual)
    logger.error("   Expected ")
    printDf(expected)
    logger.error(s"  Do schemata equal? ${actual.schema.fields.toSet == expected.schema.fields.toSet}")
    logger.error(s"  Do cardinalities equal? ${actual.count() == expected.count()}")
    logger.error("   symmetric Difference ")
    actual.getSymmetricDifference(expected).show(false)
  }

  def printFailedTestResultGeneric(testName: String, arguments: Seq[GenericDataFrame] = Seq())
                                  (actual: GenericDataFrame)(expected: GenericDataFrame)
                                  (implicit logger: Logger): Unit = {
    (actual, expected) match {
      case (actual: SparkDataFrame, expected: SparkDataFrame) =>
        assert(arguments.forall(_.isInstanceOf[SparkDataFrame]))
        printFailedTestResult(testName, arguments.map(_.asInstanceOf[SparkDataFrame].inner))(actual.inner)(expected.inner)
    }
  }

}
