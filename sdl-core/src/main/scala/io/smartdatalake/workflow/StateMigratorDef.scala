/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.json4s.JsonAST.{JInt, JNothing, JObject}
import org.json4s.{JArray, JValue}

/**
 * Definition of how to migrate from one state version to another.
 * Note that this should always migrate an existing version to the next version, without skipping a version.
 * There might be other state migrators applied afterwards to migrate to the current version.
 */
private[smartdatalake] trait StateMigratorDef {
  def versionFrom: Int
  def versionTo: Int
  def migrate(json: JObject): JObject

  /**
   * Updates runStateFormatVersion to the given version number.
   * First state files did not have a version field.
   */
  protected def updateVersion(json: JObject, version: Int): JObject = {
    import org.json4s.JsonDSL._
    val jVersion = JInt(version)
    if ((json \ "runStateFormatVersion").values == None) {
      json ~ ("runStateFormatVersion" -> jVersion)
    } else {
      json.transformField {
        case (name, _) if name == "runStateFormatVersion" =>
          (name, jVersion)
      }.asInstanceOf[JObject]
    }
  }

  override def toString: String = s"$versionFrom -> $versionTo"
}

/**
 * Migrate state from format version 3 to 4:
 * - the structure of `actionState.results` was cleaned up.
 *   instead of having `results.subFeed`, the attributes of `subFeed` have been moved directly into `result`.
 * - Additionally `actionState.inputIds[].id` was cleaned up as `inputIds[]`, and the same applies for `actionState.outputIds`.
 */
class StateMigratorDef3To4 extends StateMigratorDef with SmartDataLakeLogger {
  override val versionFrom = 3
  override val versionTo = 4
  override def migrate(json: JObject): JObject = {
    assert(json \ "runStateFormatVersion" match {
      case JInt(version) => version <= versionFrom
      case JNothing => true // first state files did not have an attribute runStateFormatVersion
    }, s"Version should be equals or less than $versionFrom")

    // migrate json
    val migratedJson = json.transformField {
      case (name, actionsState) if name == "actionsState" =>
        (name, actionsState.transformField {
          case (name, results) if name == "results" =>
            (name, JArray(results.children.map {
              result =>
                if (logger.isDebugEnabled) logger.debug(s"migrating result $result")
                val subFeed = result \ "subFeed" transformField {
                  case (name, partitionValues) if name == "partitionValues" =>
                    (name, JArray(partitionValues.children.map {
                      entry => entry \ "elements"
                    }))
                }
                result.removeField {
                  case (name, _) => name == "subFeed"
                }.merge(subFeed)
            }))
          case (name, inputIds) if name == "inputIds" =>
            if (logger.isDebugEnabled) logger.debug(s"migrating inputIds $inputIds")
            (name, JArray(inputIds.children.map {
              id => id \ "id"
            }))
          case (name, outputIds) if name == "outputIds" =>
            if (logger.isDebugEnabled) logger.debug(s"migrating outputIds $outputIds")
            (name, JArray(outputIds.children.map {
              id => id \ "id"
            }))
        })
    }.asInstanceOf[JObject]

    // update version and return
    updateVersion(migratedJson, versionTo)
  }
}