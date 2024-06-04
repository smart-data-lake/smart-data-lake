/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException

/**
 * AnalysisException with reduced logical plan output
 * Output of logical plan is reduced to max 5 lines
 */
class SimplifiedAnalysisException (analysisException: ExtendedAnalysisException)
  extends Exception(analysisException.message, analysisException.cause.orNull) with Serializable {
  setStackTrace(analysisException.getStackTrace)
  private val logicalPlanMaxLines = 5
  override def getMessage: String = {
    val planAnnotation = Option(analysisException.plan).map(p => limitLines(s";\n$p", logicalPlanMaxLines)).getOrElse("")
    analysisException.getSimpleMessage + planAnnotation
  }
  private def limitLines(s: String, lines: Int) = {
    val lines = s.linesIterator
    val limitedLines = lines.take(logicalPlanMaxLines) ++ (if(lines.hasNext) Seq("... logical plan is truncated to 5 lines. See Environment.simplifyFinalExceptionLog to disable this.") else Seq())
    limitedLines.mkString(System.lineSeparator())
  }
}
