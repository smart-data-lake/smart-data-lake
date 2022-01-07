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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.meta.{BaseType, GenericTypeUtil}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import io.smartdatalake.workflow.dataobject.{HttpTimeoutConfig, WebserviceFileDataObject}
import org.json4s.Extraction.decompose
import org.json4s.jackson.JsonMethods.{parse, pretty, render}
import org.json4s.{DefaultFormats, Formats, JObject}
import org.reflections.Reflections

import scala.reflect.runtime.universe.typeOf
import scala.util.{Failure, Success}

/**
 * Class to generate and push atlas type definitions from an sdl instance registry.
 */
case class AtlasTypeUtil(atlasConfig: AtlasConfig) extends SmartDataLakeLogger {
  val typeDefPrefix: String = atlasConfig.getTypeDefPrefix

  val sdlBaseTypesWithAtlasSuperTypes: Map[String, String] = atlasConfig.baseClassNames.map(
    baseClassName => baseClassName -> atlasConfig.getAtlasBaseType(baseClassName).get
  ).toMap

  /**
   * Generates and pushes the definition of a classification with the name of the app as defined in the atlas config.
   * Generates and pushes definitions for all classes that extend one of the baseTypes defined in the atlas config.
   *
   * @return atlas response.
   */
  def export(implicit instanceRegistry: InstanceRegistry): Unit = {

    val atlasDefinedTypes = atlasDefinedTypeNames

    val reflections = new Reflections("io.smartdatalake")
    val sdlEntityTypes = GenericTypeUtil.typeDefs(reflections).map(AtlasTypeDef.apply)

    val classificationTypes = Set(Map("name" -> "app"), Map("name" -> atlasConfig.getAppName, "superTypes" -> Seq("app")))

    // update existing types
    val existingEntityTypes = sdlEntityTypes.filter(entity => atlasDefinedTypes("entityDefs").contains(entity.name))
    val existingClassificationTypes = classificationTypes.filter(classification => atlasDefinedTypes("classificationDefs").contains(classification("name").toString))
    exportTypesToAtlas(Map("classificationDefs" -> existingClassificationTypes, "entityDefs" -> existingEntityTypes), update = true)
    logger.info(s"Updated ${existingEntityTypes.size} types")

    // create new types
    val newEntityTypes = sdlEntityTypes.diff(existingEntityTypes)
    val newClassificationTypes = classificationTypes.diff(existingClassificationTypes)
    exportTypesToAtlas(Map("classificationDefs" -> newClassificationTypes, "entityDefs" -> newEntityTypes))
    logger.info(s"Created ${newEntityTypes.size} types")
  }

  /**
   * Converts a canonical scala type name (e.g. io.smartdatalake.workflow.entity.DataObject) to the typeName that
   * is used in atlas (e.g. sdl_DataObject).
   * For types that are not exported the default "string" is returned.
   *
   * @param typeName the scala canonical typeName
   * @return atlas typeName
   */
  def exportTypeName(typeName: String): String = {
    val simpleTypeNames = Set(
      typeOf[String],
      typeOf[Boolean],
      typeOf[Int],
      typeOf[Float],
      typeOf[Double]
    ).map(_.typeSymbol.name.toString)

    if (simpleTypeNames.contains(typeName)) return typeName.toLowerCase()
    if (BaseType.values.map(_.toString).contains(typeName)) return typeDefPrefix + typeName.split("\\.").last
    if (sdlBaseTypesWithAtlasSuperTypes.values.toSet.contains(typeName)) return typeName
    if (typeName.startsWith("Seq[")) {
      val typeArgument = exportTypeName(typeName.drop(4).dropRight(1))
      return "array<%s>".format(typeArgument)
    }
    if (typeName.startsWith("Map[")) {
      val typeArguments = typeName.drop(4).dropRight(1).split(",").map(exportTypeName)
      return "map<%s>".format(typeArguments.mkString(","))
    }
    "string"
  }

  /**
   * Retrieves names of entityDefs and classificationDefs from atlas.
   *
   * @return A Map with the keys "entityDefs" and "classificationDefs" and sets of typedefNames as values.
   */
  def atlasDefinedTypeNames(implicit instanceRegistry: InstanceRegistry): Map[String, Set[String]] = {
    val target = "/api/atlas/v2/types/typedefs?"
    val webserviceDO = WebserviceFileDataObject("atlasWebservice", url = atlasConfig.getAtlasUrl + target, authMode = atlasConfig.getAtlasAuth)
    val webservice = ScalaJWebserviceClient(webserviceDO)
    val result = webservice.get().get
    val response = parse(result.map(_.toChar).mkString("", "", "")).asInstanceOf[JObject]
    Map(
      "entityDefs" -> response.values("entityDefs").asInstanceOf[Seq[Map[String, Any]]].map(v => v("name")).asInstanceOf[Seq[String]].toSet,
      "classificationDefs" -> response.values("classificationDefs").asInstanceOf[Seq[Map[String, Any]]].map(v => v("name")).asInstanceOf[Seq[String]].toSet
    )
  }

  /**
   * Retrieves entityDefs from atlas
   */
  def atlasDefinedEntities(implicit instanceRegistry: InstanceRegistry): Set[AtlasTypeDef] = {
    val target = "/api/atlas/v2/types/typedefs?"
    val webserviceDO = WebserviceFileDataObject("atlasWebservice", url = atlasConfig.getAtlasUrl + target, authMode = atlasConfig.getAtlasAuth)
    val webservice = ScalaJWebserviceClient(webserviceDO)
    webservice.get() match {
      case Success(response) =>
        implicit val formats: Formats = DefaultFormats
        val jsonResponse = parse(new String(response))
        val jsonEntities = jsonResponse \ "entityDefs"
        jsonEntities.extract[Seq[AtlasTypeDef]].toSet
      case Failure(exception) => throw exception
    }
  }

  /**
   * Post or put the typeDefs to atlas.
   *
   * @param typeDefs the map to serialize to json.
   * @param update   flag to be used for types that are already defined in atlas.
   * @return the atlas response.
   */
  def exportTypesToAtlas(typeDefs: Map[String, Set[_]], update: Boolean = false)(implicit instanceRegistry: InstanceRegistry): Array[Byte] = {
    val target = "/api/atlas/v2/types/typedefs?"
    val webserviceDO = WebserviceFileDataObject("atlasWebservice", url = atlasConfig.getAtlasUrl + target, authMode = atlasConfig.getAtlasAuth, timeouts = Some(HttpTimeoutConfig(30000, 60000)))
    val webservice = ScalaJWebserviceClient(webserviceDO)
    implicit val formats: DefaultFormats.type = DefaultFormats
    val body = pretty(render(decompose(typeDefs)))
    val result = if (update) webservice.put(body.getBytes(), "application/json")
    else webservice.post(body.getBytes(), "application/json")
    result.get // throw exception if any
  }
}
