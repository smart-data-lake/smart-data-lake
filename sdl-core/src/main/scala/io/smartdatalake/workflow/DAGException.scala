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

package io.smartdatalake.workflow

import io.smartdatalake.workflow.DAGHelper.NodeId

private[smartdatalake] abstract class DAGException(msg: String, cause: Throwable = null) extends Exception(msg, cause) {
  def severity: ExceptionSeverity.ExceptionSeverity
  def getDAGRootExceptions: Seq[DAGException]
  def getMessageWithCause: String = msg + Option(getDAGRootExceptions.head.getCause).map(t => s": ${t.getClass.getSimpleName}: ${t.getMessage}").getOrElse("")
}

private[smartdatalake] case class TaskFailedException(id: NodeId, cause: Throwable, override val severity: ExceptionSeverity.ExceptionSeverity) extends DAGException(id, cause) {
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}
private[smartdatalake] object TaskFailedException {
  def apply(id: NodeId, cause: Throwable): TaskFailedException = cause match {
    case ex: DAGException => TaskFailedException(id, cause, ex.severity)
    case _ => TaskFailedException(id, cause, ExceptionSeverity.FAILED)
  }
}

private[smartdatalake] case class TaskCancelledException(id: NodeId) extends DAGException(id) {
  override val severity: ExceptionSeverity.ExceptionSeverity = ExceptionSeverity.CANCELLED
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}

// this is no case class as it should be extended by child classes
private[smartdatalake] class TaskSkippedWarning(id: NodeId, msg: String) extends DAGException(msg) {
  override val severity: ExceptionSeverity.ExceptionSeverity = ExceptionSeverity.SKIPPED
  override def getDAGRootExceptions: Seq[DAGException] = Seq(this)
}

private[smartdatalake] class TaskSkippedDontStopWarning(id: NodeId, msg: String) extends TaskSkippedWarning(id, msg) {
  override val severity: ExceptionSeverity.ExceptionSeverity = ExceptionSeverity.SKIPPED_DONT_STOP
}

private[smartdatalake] case class TaskPredecessorFailureWarning(id: NodeId, cause: DAGException, allCauses: Seq[DAGException]) extends DAGException(id, cause) {
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