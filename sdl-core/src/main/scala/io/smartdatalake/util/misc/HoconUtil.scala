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

package io.smartdatalake.util.misc

import com.typesafe.config.{ConfigList, ConfigObject, ConfigValue, ConfigValueFactory, ConfigValueType}

import scala.jdk.CollectionConverters._

object HoconUtil {

  /**
   * Hocon does not support config.withValue(path, newValue) if there are array is the path.
   * This method supports it.
   */
  def updateConfigValue(config: ConfigValue, path: Seq[String], newValue: ConfigValue): ConfigValue = {
    // check if next element is a list
    if (isListIdx(path.head)) {
      assert(config.valueType() == ConfigValueType.LIST)
      val selectedIdx = path.head.stripPrefix("[").stripSuffix("]").toInt
      ConfigValueFactory.fromIterable(
        config.asInstanceOf[ConfigList].asScala.zipWithIndex.map {
          case (element, idx) if idx == selectedIdx =>
            updateConfigValue(element, path.tail, newValue)
          case (element, _) => element
        }.asJava
      )
    } else {
      assert(config.valueType() == ConfigValueType.OBJECT)
      val configObj = config.asInstanceOf[ConfigObject]
      val nextPath = path.takeWhile(!isListIdx(_)).mkString(".")
      val restPath = path.dropWhile(!isListIdx(_))
      if (restPath.isEmpty) {
        configObj.toConfig.withValue(nextPath, newValue).root
      } else {
        val nextElement = configObj.toConfig.getValue(nextPath)
        configObj.toConfig.withValue(nextPath, updateConfigValue(nextElement, restPath, newValue)).root
      }
    }
  }

  /**
   * Hocon does not support config.getValue(path) if there are array is the path.
   * This method supports it.
   */
  def getConfigValue(config: ConfigValue, path: Seq[String]): ConfigValue = {
    // element to update found?
    if (path.isEmpty) return config
    // check if next element is a list
    if (isListIdx(path.head)) {
      assert(config.valueType() == ConfigValueType.LIST)
      val selectedIdx = path.head.stripPrefix("[").stripSuffix("]").toInt
      val element = config.asInstanceOf[ConfigList].get(selectedIdx)
      getConfigValue(element, path.tail)
    } else {
      assert(config.valueType() == ConfigValueType.OBJECT)
      val configObj = config.asInstanceOf[ConfigObject]
      val nextPath = path.takeWhile(!isListIdx(_)).mkString(".")
      val restPath = path.dropWhile(!isListIdx(_))
      val nextElement = configObj.toConfig.getValue(nextPath)
      getConfigValue(nextElement, restPath)
    }
  }

  private def findInConfigEntry(key: String, value: ConfigValue, conditionFn: (String,ConfigValue) => Boolean, path: Seq[String]): Seq[Seq[String]] = {
    value.valueType match {
      // recursion for objects and lists
      case ConfigValueType.OBJECT =>
        findInConfigObject(value.asInstanceOf[ConfigObject], conditionFn, path)
      case ConfigValueType.LIST =>
        val elements = value.asInstanceOf[ConfigList].asScala.zipWithIndex
        elements.flatMap {
          case (value, idx) => findInConfigEntry(idx.toString, value, conditionFn, path :+ s"[$idx]")
        }
      // we are looking for className and type attributes
      case _ =>
        if (conditionFn(key,value)) Seq(path) // found! return configuration path
        else Seq() // default -> nothing found
    }
  }

  /**
   * Finds all paths in a Hocon configuration matching a given condition.
   * @param obj the config object to search
   * @param conditionFn search condition defined on key and value of a config entry.
   * @return list of paths matching condition
   */
  def findInConfigObject(obj: ConfigObject, conditionFn: (String,ConfigValue) => Boolean, path: Seq[String] = Seq()): Seq[Seq[String]] = {
    obj.entrySet.asScala.toSeq.flatMap(e => findInConfigEntry(e.getKey, e.getValue, conditionFn, path :+ e.getKey))
  }

  private def isListIdx(pathElement: String) = pathElement.startsWith("[")
}
