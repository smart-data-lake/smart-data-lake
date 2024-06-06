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

package io.smartdatalake.util.dag

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.dag.DAGHelper.NodeId
import io.smartdatalake.util.misc.LogUtil
import io.smartdatalake.workflow.{SimplifiedAnalysisException, SubFeed}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException

private[smartdatalake] abstract class DAGException(msg: String, cause: Throwable = null) extends Exception(msg, cause) {
  def severity: ExceptionSeverity.ExceptionSeverity
  def getDAGRootExceptions: Seq[DAGException]
}

private[smartdatalake] case class TaskFailedException(id: NodeId, msg: String, cause: Throwable, override val severity: ExceptionSeverity.ExceptionSeverity, results: Option[Seq[SubFeed]] = None) extends DAGException(msg, cause) {
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}
private[smartdatalake] object TaskFailedException {
  def apply(id: NodeId, cause: Throwable, results: Option[Seq[SubFeed]]): TaskFailedException = {
    // get root cause to show create message of this exception
    val rootCause = getRootCause(cause)
    // create message including first line of cause message
    val rootCauseFirstLine = LogUtil.splitLines(rootCause.getMessage).headOption
    val msg = s"Task $id failed. Root cause is '${rootCause.getClass.getSimpleName}${rootCauseFirstLine.map(": "+_).getOrElse("")}'"
    // create exception
    val ex = cause match {
      case ex: DAGException => TaskFailedException(id, msg, cause, ex.severity, results)
      case ex: ExtendedAnalysisException if Environment.simplifyFinalExceptionLog =>
        // reduce logical plan output to 5 lines
        TaskFailedException(id, msg, new SimplifiedAnalysisException(ex), ExceptionSeverity.FAILED, results)
      case _ => TaskFailedException(id, msg, cause, ExceptionSeverity.FAILED, results)
    }
    // remove stacktrace: avoid many lines of DAG, Monix and Java stacktrace and show directly the real exception that made the task fail
    ex.setStackTrace(Array())
    ex
  }
  // recursively get root cause of exception
  def getRootCause(cause: Throwable): Throwable = {
    Option(cause.getCause).map(getRootCause).getOrElse(cause)
  }
}

private[smartdatalake] case class TaskCancelledException(id: NodeId) extends DAGException(s"Task $id cancelled") {
  override val severity: ExceptionSeverity.ExceptionSeverity = ExceptionSeverity.CANCELLED
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}

// this is no case class as it should be extended by child classes
private[smartdatalake] class TaskSkippedWarning(id: NodeId, msg: String) extends DAGException(msg) {
  override val severity: ExceptionSeverity.ExceptionSeverity = ExceptionSeverity.SKIPPED
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}

private[smartdatalake] class TaskSkippedDontStopWarning[R <: DAGResult](id: NodeId, msg: String, results: Option[Seq[R]]) extends TaskSkippedWarning(id, msg) {
  override val severity: ExceptionSeverity.ExceptionSeverity = ExceptionSeverity.SKIPPED_DONT_STOP
  def getResults: Option[Seq[R]] = results
}

private[smartdatalake] case class TaskPredecessorFailureWarning(id: NodeId, cause: DAGException, allCauses: Seq[DAGException]) extends DAGException(s"Task $id failed because predecessor failed", cause) {
  override val severity: ExceptionSeverity.ExceptionSeverity = cause.severity
  override def getDAGRootExceptions: Seq[DAGException] = allCauses.flatMap(_.getDAGRootExceptions)
}

private[smartdatalake] object ExceptionSeverity extends Enumeration {
  type ExceptionSeverity = Value
  val FAILED, // Execution of action failed, dependent actions are not executed, next phase is not started
      CANCELLED, // Execution of action got cancelled, dependent actions are not executed, next phase is not started
      FAILED_DONT_STOP, // Execution of action failed, dependent actions are not executed, but next phase is started
      SKIPPED, // Execution of action is skipped because of ExecutionMode, dependent actions are not executed, next phase is started
      SKIPPED_DONT_STOP // Execution of action is skipped because of ExecutionMode, dependent actions are executed, next phase is started
      = Value
}