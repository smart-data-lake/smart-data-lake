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

import com.typesafe.config.Config
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode}
import io.smartdatalake.util.secrets.StringOrSecret

//TODO: use kxbmap configs to read Hocon Config into case class, as done in SDL...
case class AtlasConfig(config: Config) {

  import configs.syntax._

  lazy val baseTypesConfigList: List[Config] = config.get[List[Config]]("baseTypes").valueOrElse(List.empty)
  lazy val baseClassNames: Set[String] = baseTypesConfigList.map(baseTypeConfig => baseTypeConfig.get[String]("path").valueOrElse(null)).toSet.filterNot(_ == null)

  def getAtlasBaseType(clazz: Class[_]): Option[String] = {
    getAtlasBaseType(clazz.getCanonicalName)
  }

  def getAtlasBaseType(className: String): Option[String] = {
    baseTypesConfigList.find(obj => obj.get[String]("path").value.equals(className))
      .map(_.get[String]("atlasType").value)
  }

  def getAppName: String = {
    config.get[String]("appName").value
  }

  def getTypeDefPrefix: String = {
    config.get[String]("typeDefPrefix").value
  }

  def getAtlasAuth: Option[AuthMode] = {
    val auth = config.get[Map[String, String]]("atlasServer.auth").value
    //Some(TokenAuthMode(auth("token")))
    Some(BasicAuthMode(Some(StringOrSecret(auth("user"))), Some(StringOrSecret(auth("password")))))
  }

  def getAtlasUrl: String = {
    config.get[String]("atlasServer.url").value
  }

}
