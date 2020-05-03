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
package io.smartdatalake.workflow.action

import java.time.{Duration, LocalDateTime}

import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry, ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DAGNode, SparkSubFeed, SubFeed}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * An action defines a [[DAGNode]], that is, a transformation from input [[DataObject]]s to output [[DataObject]]s in
 * the DAG of actions.
 */
private[smartdatalake] trait Action extends SdlConfigObject with ParsableFromConfig[Action] with DAGNode with SmartDataLakeLogger {

  /**
   * A unique identifier for this instance.
   */
  override val id: ActionObjectId

  /**
   * Additional metadata for the Action
   */
  def metadata: Option[ActionMetadata]

  /**
   * Input [[DataObject]]s
   * To be implemented by subclasses
   */
  def inputs: Seq[DataObject]

  /**
   * Output [[DataObject]]s
   * To be implemented by subclasses
   */
  def outputs: Seq[DataObject]

  /**
   * Prepare DataObjects prerequisites.
   * In this step preconditions are prepared & tested:
   * - directories exists or can be created
   * - connections can be created
   *
   * This runs during the "prepare" operation of the DAG.
   */
  def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    inputs.foreach(_.prepare)
    outputs.foreach(_.prepare)

    // Make sure that data object names are still unique when replacing special characters with underscore
    // Requirement from SQL transformations because temp view names can not contain special characters
    val duplicateNames = context.instanceRegistry.getDataObjects.map {
      dataObj => ActionHelper.replaceSpecialCharactersWithUnderscore(dataObj.id.id)
    }.groupBy(identity).collect { case (x, List(_,_,_*)) => x }.toList

    require(duplicateNames.size==0, s"The names of your DataObjects are not unique when replacing special characters with underscore. Duplicates: ${duplicateNames.mkString(",")}")

  }

  /**
   * Initialize Action with [[SubFeed]]'s to be processed.
   * If successful
   * - the DAG can be built
   * - Spark DataFrame lineage can be built
   *
   * @param subFeeds [[SparkSubFeed]]'s to be processed
   * @return processed [[SparkSubFeed]]'s
   */
  def init(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed]

  /**
   * Executes operations needed before executing an action.
   * In this step any operation on Input- or Output-DataObjects needed before the main task is executed,
   * e.g. JdbcTableDataObjects preSql
   */
  def preExec(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    inputs.foreach(_.preRead)
    outputs.foreach(_.preWrite)
  }

  /**
   * Executes the main task of an action.
   * In this step the data of the SubFeed's is moved from Input- to Output-DataObjects.
   *
   * @param subFeeds [[SparkSubFeed]]'s to be processed
   * @return processed [[SparkSubFeed]]'s
   */
  def exec(subFeeds: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Seq[SubFeed]

  /**
   * Executes operations needed after executing an action.
   * In this step any operation on Input- or Output-DataObjects needed after the main task is executed,
   * e.g. JdbcTableDataObjects postSql or CopyActions deleteInputData.
   */
  def postExec(inputSubFeed: Seq[SubFeed], outputSubFeed: Seq[SubFeed])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    inputs.foreach(_.postRead)
    outputs.foreach(_.postWrite)
  }

  /**
   * provide an implementation of the DAG node id
   */
  def nodeId: String = id.id

  /**
   * Sets the util job description for better traceability in the Spark UI
   *
   * @param operation operation description (be short...)
   * @param session util session
   */
  def setSparkJobDescription(operation: String)(implicit session: SparkSession) : Unit = {
    session.sparkContext.setJobDescription(s"${this.getClass.getSimpleName}.$id: $operation")
  }

  /**
   * Handle class cast exception when getting objects from instance registry
   */
  private def getDataObject[T <: DataObject](dataObjectId: DataObjectId, role: String)(implicit registry: InstanceRegistry, ct: ClassTag[T], tt: TypeTag[T]): T = {
    val dataObject = registry.get[T](dataObjectId)
    try {
      // force class cast on generic type (otherwise the ClassCastException is thrown later)
      ct.runtimeClass.cast(dataObject).asInstanceOf[T]
    } catch {
      case e: ClassCastException =>
        val objClass = dataObject.getClass.getSimpleName
        val expectedClass = tt.tpe.toString.replaceAll(classOf[DataObject].getPackage.getName+".", "")
        throw ConfigurationException(s"$toStringShort needs $expectedClass as $role but $dataObjectId is of type $objClass")
    }
  }
  protected def getInputDataObject[T <: DataObject: ClassTag: TypeTag](id: DataObjectId)(implicit registry: InstanceRegistry): T = getDataObject[T](id, "input")
  protected def getOutputDataObject[T <: DataObject: ClassTag: TypeTag](id: DataObjectId)(implicit registry: InstanceRegistry): T = getDataObject[T](id, "output")

  /**
   * A buffer to collect Action events
   */
  private val runtimeEvents = mutable.Buffer[RuntimeEvent]()

  /**
   * Adds an action event
   */
  def addRuntimeEvent(phase: String, state: RuntimeEventState, msg: Option[String] = None): Unit = {
    runtimeEvents.append(RuntimeEvent(LocalDateTime.now, phase, state, msg))
  }

  /**
   * get latest runtime information for this action as string
   */
  def getRuntimeInfo: Option[RuntimeInfo] = {
    if (runtimeEvents.nonEmpty) {
      val lastEvent = runtimeEvents.last
      val startEvent = runtimeEvents.reverse.find( event => event.state == RuntimeEventState.STARTED && event.phase == lastEvent.phase )
      val duration = startEvent.map( start => Duration.between(start.tstmp, lastEvent.tstmp))
      Some(RuntimeInfo(lastEvent.state, startTstmp = startEvent.map(_.tstmp), duration = duration, msg = lastEvent.msg))
    } else None
  }

  /**
   * This is displayed in ascii graph visualization
   */
  final override def toString: String = {
   nodeId + getRuntimeInfo.map(" "+_).getOrElse("")
  }

  def toStringShort: String = {
    s"$id[${this.getClass.getSimpleName}]"
  }

  def toStringMedium: String = {
    val inputStr = inputs.map( _.toStringShort).mkString(", ")
    val outputStr = outputs.map( _.toStringShort).mkString(", ")
    s"$toStringShort Inputs: $inputStr Outputs: $outputStr"
  }
}

/**
 * Additional metadata for a Action
 * @param name Readable name of the Action
 * @param description Description of the content of the Action
 * @param feed Name of the feed this Action belongs to
 * @param tags Optional custom tags for this object
 */
case class ActionMetadata(
                           name: Option[String] = None,
                           description: Option[String] = None,
                           feed: Option[String] = None,
                           tags: Seq[String] = Seq()
                         )

/**
 * A structure to collect runtime information
 */
private[smartdatalake] case class RuntimeEvent(tstmp: LocalDateTime, phase: String, state: RuntimeEventState, msg: Option[String])
private[smartdatalake] object RuntimeEventState extends Enumeration {
  type RuntimeEventState = Value
  val STARTED, SUCCEEDED, FAILED, SKIPPED, PENDING = Value
}
private[smartdatalake] case class RuntimeInfo(state: RuntimeEventState, startTstmp: Option[LocalDateTime] = None, duration: Option[Duration] = None, msg: Option[String] = None) {
  override def toString: String = {
    duration.map(d => s"$state $d")
      .getOrElse(state.toString)
  }
}

