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

import com.typesafe.config.{Config, ConfigException, ConfigValueFactory, ConfigValueType}
import configs.syntax._
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{ReflectionUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.{Action, ProxyAction}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.DataObject
import org.reflections.Reflections

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.Try
import scala.util.matching.Regex

/**
 * Entry point for SDL config object parsing.
 */
private[smartdatalake] object ConfigParser extends SmartDataLakeLogger {

  /**
   * Parses the supplied config and returns a populated [[InstanceRegistry]].
   *
   * @param config  the configuration to parse.
   * @param instanceRegistry instance registry to use, default is to create a new instance.
   * @return  instance registry populated with all [[Action]]s and [[DataObject]]s defined in the configuration.
   */
  def parse(config: Config, instanceRegistry: InstanceRegistry = new InstanceRegistry): InstanceRegistry = {
    implicit val registry: InstanceRegistry = instanceRegistry

    val connections: Map[ConnectionId, Connection] = getConnectionConfigMap(config)
      .map{ case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config))}
    registry.register(connections)

    val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(config)
      .map{ case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config))}
    registry.register(dataObjects)

    val actions: Map[ActionId, Action] = getActionConfigMap(config)
      .map { case (id, config) => (ActionId(id), parseActionWithId(id, config)) }
    registry.register(actions)

    registry
  }

  final val CONFIG_SECTION_CONNECTIONS = "connections"
  final val CONFIG_SECTION_DATAOBJECTS = "dataObjects"
  final val CONFIG_SECTION_ACTIONS = "actions"
  def getConnectionEntries(config: Config): Seq[String] = extractConfigKeys(config, CONFIG_SECTION_CONNECTIONS)
  def getDataObjectsEntries(config: Config): Seq[String] = extractConfigKeys(config, CONFIG_SECTION_DATAOBJECTS)
  def getActionsEntries(config: Config): Seq[String] = extractConfigKeys(config, CONFIG_SECTION_ACTIONS)
  def extractConfigKeys(config: Config, entry: String): Seq[String] = {
    if (config.hasPath(entry)) config.getObject(entry).keySet().asScala.toSeq
    else Seq()
  }
  def getConnectionConfigMap(config: Config): Map[String, Config] = extractConfigMap(config, CONFIG_SECTION_CONNECTIONS)
  def getDataObjectConfigMap(config: Config): Map[String, Config] = extractConfigMap(config, CONFIG_SECTION_DATAOBJECTS)
  def getActionConfigMap(config: Config): Map[String, Config] = extractConfigMap(config, CONFIG_SECTION_ACTIONS)
  def extractConfigMap(config: Config, entry: String): Map[String, Config] = {
    if (config.hasPath(entry)) {
      config.get[Map[String, Config]](entry)
        .valueOrThrow(e => new ConfigurationException(s"Error extracting $entry: ${e.messages.mkString(", ")}", Some(s"$entry")))
    } else Map()
  }

  /**
   * Parse a [[SdlConfigObject]] from the supplied config.
   *
   * The config is expected to contain only the settings for this instance.
   *
   * @param config    the "local" config specifying this [[SdlConfigObject]].
   * @param configPath the current path in the configuration. Note that this is only used for error messages.
   * @param additionalConfigValues additional configuration values to add to the config before parsing.
   * @param registry  the [[InstanceRegistry]] to pass to the [[SdlConfigObject]] instance.
   * @tparam A        the abstract type this object, i.e.: [[Action]] or [[DataObject]]
   * @return          a new instance of this [[SdlConfigObject]].
   */
  def parseConfigObject[A <: ParsableFromConfig[A] : TypeTag](config: Config, configPath: Option[String] = None, additionalConfigValues : Map[String,AnyRef] = Map())
                                                             (implicit registry: InstanceRegistry): A = try {
    // get class & module
    val configuredType = config.get[String]("type")
      .mapError(error => throw ConfigurationException(s"Required configuration setting 'type' is missing.", None, error.configException))
      .value
    val clazz = Environment.classLoader.loadClass(className(configuredType))
    val mirror = runtimeMirror(clazz.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    require(classSymbol.companion.isModule, s"Can not instantiate ${classOf[DataObject].getSimpleName} of class '${clazz.getTypeName}'. It does not have a companion object.")
    val companionObjectSymbol = classSymbol.companion.asModule

    // get factory method
    logger.debug(s"Instance requested for '${clazz.getTypeName}'. Extracting factory method from companion object.")
    val factoryMethod: FactoryMethod[A] = new FactoryMethod(companionObjectSymbol, FactoryMethodExtractor.extract(companionObjectSymbol))

    // prepare refined config
    val configExtended = additionalConfigValues.foldLeft(config){
      case (config, (key,value)) => config.withValue(key, ConfigValueFactory.fromAnyRef(value))
    }
    .withoutPath("type")
    val configSubstituted = Environment.configPathsForLocalSubstitution.foldLeft(configExtended) {
      case (config, path) => try {
          localSubstitution(config, path)
        } catch {
          case e: ConfigurationException => throw ConfigurationException(s"Error in local config substitution for path='$path': ${e.message}", Some(s"$configPath.$path"), e)
        }
    }

    // create object
    logger.debug(s"Invoking extracted method: $factoryMethod.")
    Try(factoryMethod.invoke(configSubstituted, registry, mirror)).recoverWith {
      case e =>
        logger.debug(s"Failed to invoke '$factoryMethod' with '$config'.", e)
        throw Option(e.getCause).getOrElse(e)
    }.get

  } catch {
    case e: Exception => throw enrichExceptionMessageConfigPath(enrichExceptionMessageClassName(e), configPath)
  }

  def parseConfigObjectWithId[A <: ParsableFromConfig[A] : TypeTag](id: String, config: Config)(implicit registry: InstanceRegistry): A = {
    parseConfigObject[A](config, Some(getIdWithClassNamePrefixed[A](id)), Map("id" -> id))
  }

  def parseActionWithId(id: String, config: Config)(implicit registry: InstanceRegistry): Action = {
    val parsedAction = parseConfigObjectWithId[Action](id, config)
    if (parsedAction.remoteActionConfig.isDefined) {
      ProxyAction(parsedAction, parsedAction.id)
    } else {
      parsedAction
    }
  }

  /**
   * Add exceptions class name to exception message if not yet included.
   */
  private def enrichExceptionMessageClassName(e: Exception): Exception = {
    // recursively get root cause of exception
    def getRootCause(cause: Throwable): Throwable = {
      Option(cause.getCause).map(getRootCause).getOrElse(cause)
    }

    val rootCause = getRootCause(e)
    if (!rootCause.isInstanceOf[ConfigException]) {
      val rootCauseClassName = rootCause.getClass.getSimpleName
      if (!e.getMessage.contains(rootCauseClassName)) e match {
        case c: ConfigurationException => c.copy(message = s"${rootCauseClassName}: ${c.getMessage}")
        case e => ConfigurationException(s"${rootCauseClassName}: ${e.getMessage}", throwable = e)
      } else e
    } else e
  }

  /**
   * Add optional configuration path to exception message it not yet included.
   */
  private def enrichExceptionMessageConfigPath(e: Exception, configPath: Option[String]): Exception = {
    if (configPath.isDefined && !e.getMessage.contains(configPath.get)) e match {
      case c: ConfigurationException => c.copy(message = s"(${configPath.get}) ${c.getMessage}")
      case e => ConfigurationException(s"(${configPath.get}) ${e.getMessage}", configPath, e)
    } else e
  }

  /**
   * Adds the class name to a given Id as prefix.
   * This is mainly used for error messages.
   */
  def getIdWithClassNamePrefixed[A : TypeTag](id: String): String = {
    val configObjectType = typeOf[A]
    configObjectType.typeSymbol.name.toString + "~" + id
  }

  /**
   * Extracts the fully qualified class name from the type parameter in the config.
   *
   * If a "short name" is provided without package specification, search for an implementation with name configuredType
   * of the abstract type inside package "io.smartdatalake.workflow".
   *
   * @param configuredType type attribute from configuration.
   * @tparam A            the abstract type of this object, i.e.: [[Action]] or [[DataObject]]
   * @return              the fully qualified class name of this class.
   */
  private def className[A <: ParsableFromConfig[_] : TypeTag](configuredType: String): String = {
    // if no package name is given, we search for an implementation with simple class name <configuredType> of the abstract type [A] inside package "io.smartdatalake.workflow"
    if (!configuredType.contains('.')) {
      implicit val reflections: Reflections = ReflectionUtil.getReflections("io.smartdatalake.workflow")
      val implClasses = ReflectionUtil.getTraitImplClasses[A]
        .filter(_.getSimpleName == configuredType)
      val abstractSymbol = symbolOf[A]
      if (implClasses.isEmpty) throw new ClassNotFoundException(s"Implementation $configuredType of interface ${abstractSymbol.name} not found")
      if (implClasses.size>1) throw new IllegalStateException(s"Multiple implementation named $configuredType for interface ${abstractSymbol.name} found: ${implClasses.map(_.getName).mkString(", ")}")
      implClasses.head.getName
    } else configuredType
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
      val valueSubstituted = """~\{(.*?)\}""".r.replaceAllIn(value, localSubstituter)
      config.withValue(path, ConfigValueFactory.fromAnyRef(valueSubstituted))
    } else config
  }
}
