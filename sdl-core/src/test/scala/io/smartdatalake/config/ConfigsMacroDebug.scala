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

import com.typesafe.config.ConfigFactory
import io.smartdatalake.workflow.dataobject.CustomDfDataObject

/**
 * Configs macro expansion can be debugged in Intellij by creating a run configuration as follows:
 * - Main class: scala.tools.nsc.Main
 * - VM options: -Dscala.usejavacp=true
 * - program arguments: -cp io.smartdatalake.config.ConfigsMacroDebug C:\Entwicklung\smart-data-lake\sdl-core\src\test\scala\io\smartdatalake\config\ConfigsMacroDebug.scala
 */
object ConfigsMacroDebug extends App with ConfigImplicits {

  val config = ConfigFactory.parseString(
    """
      | {
      |   id = 123
      |   creator {
      |     class-name = io.smartdatalake.config.TestCustomDfCreator
      |     options = {
      |       test = foo
      |     }
      |   }
      | }
      |""".stripMargin).resolve

  implicit val instanceRegistry = new InstanceRegistry
  import configs.syntax.RichConfig
  config.extract[CustomDfDataObject].value
  //CustomDfDataObject.fromConfig(config)(new InstanceRegistry)

}
