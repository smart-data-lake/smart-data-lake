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
package io.smartdatalake.meta

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.{Action, ActionSubFeedsImpl, DataFrameActionImpl}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.file.{CanCreateInputStream, CanCreateOutputStream, FileRefDataObject}
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanReceiveParameterNotification, CanWriteDataFrame}
import io.smartdatalake.workflow.{DataFrameSubFeed, FileSubFeed, ParameterSubFeed}
import org.objenesis.ObjenesisStd

import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.{Failure, Success, Try}

/**
 * Determine the SubFeed types supported by Actions and DataObjects for the json schema export.
 *
 * Supported DataFrame SubFeed types are only known by instances, e.g. they depend on the engine implementations on the classpath.
 * As DataObjects and Actions can not be created without valid configuration (e.g. connections are looked up in the constructor),
 * instances are allocated without calling the constructor, and the methods returning the supported types are called on them.
 * This works as long as these methods don't depend on the configuration of the instance.
 */
private[smartdatalake] object SubFeedTypeUtil extends SmartDataLakeLogger {

  private val objenesis = new ObjenesisStd(true)
  private val mirror = scala.reflect.runtime.currentMirror

  /**
   * Get the supported SubFeed types of a concrete Action or DataObject type, None for other types.
   */
  def subFeedTypes(tpe: Type): Option[SubFeedTypesDef] = {
    if (tpe <:< typeOf[DataObject]) Some(dataObjectSubFeedTypes(tpe))
    else if (tpe <:< typeOf[Action]) actionSubFeedTypes(tpe).map(types => SubFeedTypesDef(types, types))
    else None
  }

  /**
   * - Reading a DataFrame is supported according to getSubFeedSupportedTypes, writing according to writeSubFeedSupportedTypes.
   * - FileSubFeed is supported by FileRefDataObjects which can create an input or output stream.
   * - ParameterSubFeed can be read from any DataObject, e.g. as input of a CustomScriptAction,
   *   and written to DataObjects which can receive a parameter notification.
   */
  private def dataObjectSubFeedTypes(tpe: Type): SubFeedTypesDef = {
    lazy val dataObject = allocate(tpe)
    val isFileRef = tpe <:< typeOf[FileRefDataObject]
    val read = Seq(
      if (tpe <:< typeOf[CanCreateDataFrame]) getTypes(tpe, "getSubFeedSupportedTypes", dataObject.asInstanceOf[CanCreateDataFrame].getSubFeedSupportedTypes) else Seq(),
      if (isFileRef && tpe <:< typeOf[CanCreateInputStream]) Seq(typeOf[FileSubFeed]) else Seq(),
      Seq(typeOf[ParameterSubFeed])
    ).flatten
    val write = Seq(
      if (tpe <:< typeOf[CanWriteDataFrame]) getTypes(tpe, "writeSubFeedSupportedTypes", dataObject.asInstanceOf[CanWriteDataFrame].writeSubFeedSupportedTypes) else Seq(),
      if (isFileRef && tpe <:< typeOf[CanCreateOutputStream]) Seq(typeOf[FileSubFeed]) else Seq(),
      if (tpe <:< typeOf[CanReceiveParameterNotification]) Seq(typeOf[ParameterSubFeed]) else Seq()
    ).flatten
    SubFeedTypesDef(normalize(read), normalize(write))
  }

  /**
   * The SubFeed type of an Action is given by the type parameter of ActionSubFeedsImpl.
   * A DataFrameAction supports all DataFrame SubFeed types, unless it restricts the type of its transformers independently of the configuration.
   */
  private def actionSubFeedTypes(tpe: Type): Option[Seq[Type]] = {
    tpe.baseType(typeOf[ActionSubFeedsImpl[_]].typeSymbol).typeArgs.headOption.map {
      case t if t =:= typeOf[DataFrameSubFeed] =>
        // transformerSubFeedType normally depends on the configured transformers, then it fails or returns null on an allocated instance.
        val transformerSubFeedType = Try(Option(allocate(tpe).asInstanceOf[DataFrameActionImpl].transformerSubFeedType).flatten).toOption.flatten
          .filterNot(_ =:= typeOf[DataFrameSubFeed])
        normalize(transformerSubFeedType.map(Seq(_)).getOrElse(Seq(t)))
      case t => Seq(t)
    }
  }

  private def allocate(tpe: Type): AnyRef = objenesis.newInstance(mirror.runtimeClass(tpe.typeSymbol.asClass)).asInstanceOf[AnyRef]

  private def getTypes(tpe: Type, method: String, fn: => Seq[Type]): Seq[Type] = {
    Try(Option(fn).getOrElse(throw new IllegalStateException("result is null"))) match {
      case Success(types) => types
      case Failure(e) =>
        logger.warn(s"Could not get supported SubFeed types of ${tpe.typeSymbol.fullName} by calling $method on an uninitialized instance: ${e.getClass.getSimpleName} ${e.getMessage}")
        Seq()
    }
  }

  /**
   * Replace the generic DataFrameSubFeed by all known DataFrame SubFeed types, remove duplicates and sort by name.
   */
  private def normalize(types: Seq[Type]): Seq[Type] = {
    types.flatMap(t => if (t =:= typeOf[DataFrameSubFeed]) DataFrameSubFeed.getKnownSubFeedTypes else Seq(t))
      .groupBy(_.typeSymbol.fullName).values.map(_.head).toSeq
      .sortBy(_.typeSymbol.name.toString)
  }
}
