/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.meta.atlas

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import io.smartdatalake.workflow.AtlasExportable
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.{DataObject, HttpTimeoutConfig, WebserviceFileDataObject}
import org.json4s.DefaultFormats
import org.json4s.Extraction.decompose
import org.json4s.jackson.JsonMethods.{pretty, render}

import scala.util.{Failure, Success, Try}

/**
 * Class to generate and push entities to atlas from an sdl instance registry.
 */
case class AtlasEntitiesUtil(atlasConfig: AtlasConfig) extends SmartDataLakeLogger {

  val appName: String = atlasConfig.getAppName
  val typeDefPrefix: String = atlasConfig.getTypeDefPrefix
  val typeUtil = AtlasTypeUtil(atlasConfig)

  /**
   * Generates and pushes entities to atlas.
   */
  def export(implicit instanceRegistry: InstanceRegistry): Unit = {
    val definedEntities = typeUtil.atlasDefinedEntities
      .filter(_.name.startsWith(typeDefPrefix))

    // Get all DataObjects from registry
    val dataObjects = instanceRegistry.getDataObjects.map(_.asInstanceOf[DataObject with Product])
    // Get all Connections from registry
    val connections = instanceRegistry.getConnections.map(_.asInstanceOf[Connection with Product])

    implicit val formats: DefaultFormats.type = DefaultFormats

    val dataObjectEntities = dataObjects.flatMap(dataObject => {
      toAtlasDataObject(dataObject, typeDefPrefix + dataObject.getClass.getSimpleName, definedEntities, dataObject.atlasQualifiedName(appName), dataObject.atlasName) match {
        case (Some(atlasObject), dependentObjects) =>
          (dependentObjects + atlasObject).toSeq
        case (None, _) =>
          Seq()
      }
    })

    val connectionEntities = connections.flatMap(connection => {
      toAtlasDataObject(connection, typeDefPrefix + connection.getClass.getSimpleName, definedEntities, connection.atlasQualifiedName(appName), connection.atlasName) match {
        case (Some(atlasObject), dependentObjects) =>
          (dependentObjects + atlasObject).toSeq
        case (None, _) =>
          Seq()
      }
    })

    // Get all actions from registry
    val actions: Seq[Action with Product] = instanceRegistry.getActions.map(_.asInstanceOf[Action with Product])

    val actionEntities = actions.flatMap(action => {
      toAtlasDataObject(action, typeDefPrefix + action.getClass.getSimpleName, definedEntities, action.atlasQualifiedName(appName), action.atlasName) match {
        case (Some(atlasObject), dependentObjects) =>
          val inputGuids = action.inputs.map(input => Map("guid" -> (dataObjectEntities ++ connectionEntities).find(atlasDataObject => atlasDataObject.obj == input).get.guid))
          val outputGuids = action.outputs.map(output => Map("guid" -> (dataObjectEntities ++ connectionEntities).find(atlasDataObject => atlasDataObject.obj == output).get.guid))
          (dependentObjects + atlasObject.copy(attributes = atlasObject.attributes + ("inputs" -> inputGuids) + ("outputs" -> outputGuids), classifications = Seq(Map("typeName" -> appName)))).toSeq
        case (None, _) =>
          Seq()
      }
    }).toSet

    exportToAtlas(actionEntities ++ dataObjectEntities)
    logger.info(s"Exported ${actionEntities.size} actions and ${dataObjectEntities.size} data objects")
  }

  /**
   * Generates an exportable atlas entity as well as all dependent atlas entities for a given object.
   * Dependent entities inherit the qualifiedName and name from the object they depend on, if they do not implement exportable.
   *
   * @param value           the object to be exported.
   * @param atlasTypeName   the typename of the typedef to be used.
   * @param atlasEntityDefs all entity defs available in atlas.
   * @param qualifiedName   the qualified to be used for export. Has to be unique in objects of the same type.
   * @param name            the name to be used for export.
   * @return a tuple containing the entity corresponding to value (None, if it is not an existing atlas entityDef),
   *         and a set of dependent entities.
   */
  def toAtlasDataObject(value: Any, atlasTypeName: String, atlasEntityDefs: Set[AtlasTypeDef], qualifiedName: String, name: String): (Option[AtlasEntity], Set[AtlasEntity]) = {
    var dependentAtlasObjects = Set[AtlasEntity]()
    atlasEntityDefs.find(_.name.equals(atlasTypeName)) match {

      // Entity is atlas defined sdl_ type
      case Some(entity) =>
        val attributeDefs = attributeDefsRecursive(entity, atlasEntityDefs)
        val attributeDefsWithoutUndefinedOptionals = attributeDefs.filter(attributeDef => {
          val method = Try(value.getClass.getDeclaredMethod(attributeDef.name))
          method.map { method =>
            val attributeValue = method.invoke(value)
            !(attributeDef.isOptional && attributeValue.asInstanceOf[Option[_]].isEmpty)
          }.getOrElse(false)
        })
        val attributes = attributeDefsWithoutUndefinedOptionals.map(attributeDef => {
          val method = value.getClass.getDeclaredMethod(attributeDef.name)
          val attributeValue = if (attributeDef.isOptional) {
            method.invoke(value).asInstanceOf[Option[_]].get
          } else {
            method.invoke(value)
          }

          // Recursively get dependent entities
          toAtlasDataObject(attributeValue, attributeDef.typeName, atlasEntityDefs, qualifiedName, name) match {

            //Attribute is defined sdl_ type
            case (Some(attributeDO), dependentObjects) =>
              dependentAtlasObjects = dependentAtlasObjects ++ dependentObjects + attributeDO
              attributeDef.name -> Map("guid" -> attributeDO.guid)

            //Attribute is non sdl_ type or undefined sdl_ type
            case (None, _) =>
              attributeDef.typeName match {
                case arrayType if arrayType.startsWith("array") =>
                  val pattern = "array<(.*)>".r
                  val pattern(typeArgName) = arrayType
                  val values = attributeValue.asInstanceOf[Seq[_]].map(element => {
                    toAtlasDataObject(element, typeArgName, atlasEntityDefs, qualifiedName, name) match {
                      case (Some(e), dependentObjects) =>
                        dependentAtlasObjects = dependentAtlasObjects ++ dependentObjects + e
                        e
                      case (None, _) =>
                        element.toString
                    }
                  })
                  attributeDef.name -> values

                case mapType if mapType.startsWith("map") =>
                  val pattern = "map<(.*),(.*)>".r
                  val pattern(keyTypeName, valueTypeName) = mapType
                  val values = attributeValue.asInstanceOf[Map[_, _]].map { case (key, value) =>
                    val keyObject = toAtlasDataObject(key, keyTypeName, atlasEntityDefs, qualifiedName, name) match {
                      case (Some(k), keyDependentObjects) =>
                        dependentAtlasObjects = dependentAtlasObjects ++ keyDependentObjects + k
                        k
                      case (None, _) =>
                        key.toString
                    }
                    val valueObject = toAtlasDataObject(value, valueTypeName, atlasEntityDefs, qualifiedName, name) match {
                      case (Some(v), valueDependentObjects) =>
                        dependentAtlasObjects = dependentAtlasObjects ++ valueDependentObjects + v
                        v
                      case (None, _) =>
                        Some(value).getOrElse("").toString
                    }
                    (keyObject, valueObject)
                  }
                  attributeDef.name -> values

                // Attribute is neither map, array or sdl_ type.
                // Default to string.
                case _ =>
                  attributeDef.name -> attributeValue.toString
              }
          }
        })

        // Add attributes qualifiedName and name
        value match {
          case exportable: AtlasExportable =>
            (
              Some(AtlasEntity(atlasTypeName, (attributes + ("qualifiedName" -> exportable.atlasQualifiedName(appName)) + ("name" -> exportable.atlasName)).toMap, value)),
              dependentAtlasObjects
            )
          case _ =>
            (
              Some(AtlasEntity(atlasTypeName, (attributes + ("qualifiedName" -> qualifiedName) + ("name" -> name)).toMap, value)),
              dependentAtlasObjects
            )
        }

      // Entity is non sdl_ type or undefined sdl_ type
      case None =>
        (None, Set())
    }
  }

  /**
   * Gets all attributeDefs defined by the entity and inherited by parents.
   *
   * @param entityDef  the entityDef.
   * @param entityDefs entityDefs to be searched for parents.
   * @return defined and inherited attributeDefs
   */
  def attributeDefsRecursive(entityDef: AtlasTypeDef, entityDefs: Set[AtlasTypeDef]): Set[AtlasAttributeDef] = {
    val parentEntities = entityDefs.filter(e => entityDef.superTypes.contains(e.name))
    entityDef.attributeDefs.union(parentEntities.flatMap(e => attributeDefsRecursive(e, entityDefs)))
  }

  /**
   * Push entities to atlas.
   *
   * @return atlas response.
   */
  def exportToAtlas(entities: Set[AtlasEntity])(implicit instanceRegistry: InstanceRegistry): Array[Byte] = {
    val target = "/api/atlas/v2/entity/bulk?"
    val webserviceDO = WebserviceFileDataObject("atlasWebservice", url = atlasConfig.getAtlasUrl + target, authMode = atlasConfig.getAtlasAuth, timeouts = Some(HttpTimeoutConfig(30000, 60000)))
    val webservice = ScalaJWebserviceClient(webserviceDO)
    implicit val formats: DefaultFormats.type = DefaultFormats
    val body = pretty(render(decompose("entities" -> entities.toSeq)))
    webservice.post(body.getBytes(), "application/json")
      .get // throw exception if any
  }
}
