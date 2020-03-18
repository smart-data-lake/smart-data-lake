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
package io.smartdatalake.config

import com.typesafe.config.{Config, ConfigValueFactory, ConfigValueType}
import configs.Result
import configs.syntax._
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, ConfigObjectId, ConnectionId, DataObjectId}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.DataObject

import scala.reflect.runtime.universe._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * Entry point for SDL config object parsing.
 */
private[smartdatalake] object ConfigParser extends SmartDataLakeLogger {

  /**
   * Parses the supplied config and returns a populated [[InstanceRegistry]].
   *
   * @param config  the configuration to parse.
   * @return  a new instance registry populated with all [[Action]]s and [[DataObject]]s defined in the configuration.
   */
  def parse(config: Config): InstanceRegistry = {
    implicit val registry: InstanceRegistry = new InstanceRegistry

    //val actions1 = config.get[Map[ConfigObjectId, Config]]("actions")

    val connections: Map[ConnectionId, Connection] = config.get[Map[String, Config]]("connections")
      .valueOrElse(Map.empty)
      .map{ case (id, config) => (ConnectionId(id), parseConfigObject[Connection](id, config))}
    registry.register(connections)

    val dataObjects: Map[DataObjectId, DataObject] = config.get[Map[String, Config]]("dataObjects")
      .valueOrElse(Map.empty)
      .map{ case (id, config) => (DataObjectId(id), parseConfigObject[DataObject](id, config))}
    registry.register(dataObjects)

    val actions: Map[ActionObjectId, Action] = config.get[Map[String, Config]]("actions")
      .valueOrElse(Map.empty)
      .map{ case (id, config) => (ActionObjectId(id), parseConfigObject[Action](id, config))}
    registry.register(actions)

    registry
  }

  /**
   * Parse a [[SdlConfigObject]] from the supplied config.
   *
   * The config is expected to contain only the settings for this instance.
   *
   * @param id        the id to assign to this [[SdlConfigObject]].
   * @param config    the "local" config specifying this [[SdlConfigObject]].
   * @param registry  the [[InstanceRegistry]] to pass to the [[SdlConfigObject]] instance.
   * @tparam A        the abstract type this object, i.e.: [[Action]] or [[DataObject]]
   * @return          a new instance of this [[SdlConfigObject]].
   */
  def parseConfigObject[A <: SdlConfigObject with ParsableFromConfig[A]](id: String, config: Config)
                                                                        (implicit registry: InstanceRegistry): A = {
    val clazz = Class.forName(className(id, config))
    val mirror = runtimeMirror(clazz.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)

    require(classSymbol.companion.isModule,
      s"Can not instantiate ${classOf[DataObject].getSimpleName} of class '${clazz.getTypeName}'. " +
        "It does not have a companion object.")

    val companionObjectSymbol = classSymbol.companion.asModule

    logger.debug(s"Instance requested for '${clazz.getTypeName}'. Extracting factory method from companion object.")
    val factoryMethod: FactoryMethod[A] = new FactoryMethod(companionObjectSymbol, FactoryMethodExtractor.extract(companionObjectSymbol))

    // prepare refined config
    val configWithId = config.withValue("id", ConfigValueFactory.fromAnyRef(id))
    val refinedConfig = Environment.configPathsForLocalSubstitution.foldLeft(configWithId){
      case (config, path) => try {
        localSubstitution(config, path)
      } catch {
        case e: ConfigurationException => throw ConfigurationException(s"Error in local config substitution for object with id='$id' and path='$path': ${e.message}", Some(s"$id.$path"), e)
      }
    }

    logger.debug(s"Invoking extracted method: $factoryMethod.")
    Try(factoryMethod.invoke(refinedConfig, registry, mirror)) match {
      case Success(instance) => instance
      case Failure(e) =>
        logger.debug(s"Failed to invoke '$factoryMethod' with '$config'.", e)
        val cause = Option(e.getCause)
        cause match {
          case Some(c) => throw c
          case None => throw e
        }
    }
  }

  /**
   * Extracts the fully qualified class name from the type parameter in the config.
   *
   * If a "short name" is provided without package specification, it prepends the package name of its abstract type
   * inferred from the short name (xxxAction or yyyDataObject).
   *
   * @param id            the [[ConfigObjectId]] of this class.
   * @param config        the "local" config of this class..
   * @tparam A            the abstract type this object, i.e.: [[Action]] or [[DataObject]]
   * @return              the fully qualified class name of this class.s
   */
  def className[A <: SdlConfigObject](id: String, config: Config): String = config.get[String]("type") match {
    case Result.Success(className) =>
      require(className.nonEmpty, s"Configuration setting 'type' must not be empty for config object with id='$id'.")
      val cleanClassName = className.stripPrefix(".").stripSuffix(".")
      if (cleanClassName.count(_.equals('.')) == 0) {
        val abstractSymbol = cleanClassName.toLowerCase match {
          case cn if cn.contains("connection") => symbolOf[Connection]
          case cn if cn.contains("dataobject") => symbolOf[DataObject]
          case cn if cn.contains("action") => symbolOf[Action]
          case _ => throw new IllegalArgumentException(s"Can not infer fully qualified class name from short name $cleanClassName. " +
            s"Specify the full class name for config object with id='$id'.")
        }
        val abstractOwner = Iterator.iterate(abstractSymbol)(_.owner.asType).takeWhile(!_.isPackage).toSeq.last
        s"${abstractOwner.fullName.split('.').dropRight(1).mkString(".")}.$cleanClassName"
      }
      else {
        cleanClassName
      }
    case Result.Failure(error) =>
      throw new IllegalArgumentException(s"Required configuration setting 'type' is missing for config object with id='$id'.", error.configException)
  }

  /**
   * Substitutes parts inside values by other paths of the configuration
   * Token for substitution is "~{replacementPath}"
   *
   * @param config configuration object for local substitution
   * @param path path to search for local substitution tokens and execute substitution
   * @return config with local substitution executed on path
   */
  def localSubstitution(config: Config, path: String): Config = {

    val localSubstituter = (regMatch: Regex.Match) => {
      val replacementPath = regMatch.group(1)
      if (config.hasPath(replacementPath)) {
        if (config.getValue(replacementPath).valueType() == ConfigValueType.STRING
          ||config.getValue(replacementPath).valueType() == ConfigValueType.NUMBER) config.getString(replacementPath)
        else throw ConfigurationException(s"local substitution path '$replacementPath' in path '$path' is not a string")
      } else throw ConfigurationException(s"local substitution path '$replacementPath' in path '$path' does not exist")
    }

    if (config.hasPath(path) && config.getValue(path).valueType() == ConfigValueType.STRING) {
      val value = config.getString(path)
      val valueSustituted = """~\{(.*?)\}""".r.replaceAllIn(value, localSubstituter)
      config.withValue(path, ConfigValueFactory.fromAnyRef(valueSustituted))
    } else config
  }
}
