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
package io.smartdatalake.workflow.dataobject.generic

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.ReflectionUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.DataObject
import org.reflections.Reflections

import java.lang.reflect.Modifier
import scala.reflect.runtime.universe.{Type, TypeTag}
import scala.util.Try

/**
 * An engine-specific implementation of a DataObject.
 *
 * DataObjects describe data assets independently of the execution engine. If reading/writing a data asset
 * needs engine-specific code (e.g. classic Spark vs. Spark Connect), the DataObject stays in sdl-core and
 * delegates to implementations of this trait, which are provided by the engine modules
 * (e.g. sdl-deltalake for classic Spark, sdl-sparkconnect for Spark Connect).
 *
 * Implementations are discovered on the classpath by reflection (see [[DataObjectEngine.createEngines]])
 * and must provide a public constructor with the concrete DataObject as single parameter.
 */
trait DataObjectEngine {

  /**
   * The DataFrameSubFeed type this engine executes with, e.g. SparkSubFeed or SparkConnectSubFeed.
   */
  def subFeedType: Type
}

object DataObjectEngine {

  /**
   * Discover and instantiate engine implementations of trait E for the given DataObject.
   * Implementations are searched on the classpath in package io.smartdatalake (like [[io.smartdatalake.app.ModulePlugin]])
   * and instantiated through a constructor with the concrete DataObject as single parameter.
   */
  def createEngines[E <: DataObjectEngine : TypeTag, D <: DataObject](dataObject: D, dataObjectClass: Class[D]): Seq[E] = {
    implicit val reflections: Reflections = ReflectionUtil.getReflections("io.smartdatalake")
    ReflectionUtil.getTraitImplClasses[E]
      .filter(c => !c.isInterface && !Modifier.isAbstract(c.getModifiers))
      .map(_.getConstructor(dataObjectClass).newInstance(dataObject).asInstanceOf[E])
  }
}

/**
 * Mixin for DataObjects that route to engine-specific implementations discovered on the classpath.
 *
 * The supported subFeed types are derived from the discovered engine implementations, which plugs into
 * the engine selection of DataFrameActionImpl (intersection of supported types filtered by the actions engine connection).
 */
trait HasEngineImplementation[E <: DataObjectEngine] extends CanCreateDataFrame with CanWriteDataFrame { this: DataObject =>

  /**
   * Instantiate available engine implementations, normally through [[DataObjectEngine.createEngines]].
   */
  protected def createEngines: Seq[E]

  /**
   * Appended to the error message when no engine implementation is found on the classpath.
   */
  protected def engineNotFoundHint: String = ""

  @transient protected lazy val engines: Seq[E] = {
    val discoveredEngines = createEngines
    if (discoveredEngines.isEmpty) throw ConfigurationException(
      s"($id) No engine implementation found on classpath for ${this.getClass.getSimpleName}. $engineNotFoundHint")
    discoveredEngines
  }

  /**
   * Route to the engine implementation for an explicitly requested subFeedType (used by getDataFrame/writeDataFrame).
   */
  protected def engine(subFeedType: Type): E = {
    engines.find(_.subFeedType =:= subFeedType)
      .getOrElse(throw new IllegalStateException(s"($id) No engine implementation for subFeedType ${subFeedType.typeSymbol.name} found on classpath. $engineNotFoundHint"))
  }

  /**
   * Route to the engine implementation by execution context (used by methods without a DataFrame, e.g. prepare or listPartitions).
   * Selection order: engine connection of the current action -> default engine connection -> first engine implementation (with warn log)
   */
  protected def engine(implicit context: ActionPipelineContext): E = {
    // note that context.engineConnection throws if the current actions engine connection is not registered, e.g. in tests
    val connectionSubFeedType = Try(context.engineConnection).toOption.flatten.map(_.subFeedType)
      .orElse(Try(context.instanceRegistry.get[Connection with EngineConnection](ConnectionId(Environment.defaultEngineConnectionId))).toOption.map(_.subFeedType))
    connectionSubFeedType.flatMap(tpe => engines.find(_.subFeedType =:= tpe))
      .getOrElse {
        logger.warn(s"($id) Could not determine engine implementation from context, using ${engines.head.getClass.getSimpleName}")
        engines.head
      }
  }

  override private[smartdatalake] def getSubFeedSupportedTypes: Seq[Type] = engines.map(_.subFeedType)

  override private[smartdatalake] def writeSubFeedSupportedTypes: Seq[Type] = engines.map(_.subFeedType)
}
