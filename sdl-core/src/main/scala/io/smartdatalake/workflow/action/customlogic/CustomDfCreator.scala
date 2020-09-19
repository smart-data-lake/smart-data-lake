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
package io.smartdatalake.workflow.action.customlogic

import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.CustomCodeUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Interface to define custom logic for DataFrame transformations
 */
trait CustomDfCreator extends Serializable {

  /**
   * This function creates a [[DataFrame]] based on custom code.
   *
   * @param session: the Spark Session
   * @param config Input Config of action
   * @return Map outputID -> dataFrame
   */
  def exec(session: SparkSession, config: Map[String,String]): DataFrame
}

case class CustomDfCreatorConfig(className: Option[String] = None,
                                 scalaFile: Option[String] = None,
                                 scalaCode: Option[String] = None,
                                 options: Option[Map[String,String]] = None
                                ) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined, "Either className, scalaFile or scalaCode must be defined for CustomDfCreator")

  val impl : CustomDfCreator = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomDfCreator](clazz)
  }.orElse{
    scalaFile.map {
      file =>
        val fnTransform = CustomCodeUtil.compileCode[(SparkSession, Map[String, String]) => DataFrame](HdfsUtil.readHadoopFile(file))
        new CustomDfCreatorWrapper(fnTransform)
    }
  }.orElse{
    scalaCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileCode[(SparkSession, Map[String, String]) => DataFrame](code)
        new CustomDfCreatorWrapper(fnTransform)
    }
  }.get


  def exec(implicit session: SparkSession): DataFrame = {
    impl.exec(session, options.getOrElse(Map()))
  }

  override def toString: String = {
    if (className.isDefined)       "className: "+className.get
    else if (scalaFile.isDefined)  "scalaFile: "+scalaFile.get
    else                          "scalaCode: "+scalaCode.get
  }
}

class CustomDfCreatorWrapper(val fnExec: (SparkSession, Map[String,String]) => DataFrame) extends CustomDfCreator {
  override def exec(session: SparkSession, config: Map[String, String]): DataFrame =
    fnExec(session, config)
}
