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
package io.smartdatalake.workflow.action.spark.customlogic

import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfCreatorConfig.{fnExecType, fnSchemaType}
import io.smartdatalake.workflow.dataobject.CustomDfDataObject
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Interface to define custom logic for DataFrame creation
 */
trait CustomDfCreator extends Serializable {

  /**
   * This method creates a [[DataFrame]] based on custom code.
   *
   * @param session the Spark session
   * @param config the input config of the associated action
   * @return the custom DataFrame
   */
  def exec(session: SparkSession, config: Map[String,String]): DataFrame

  /**
   * Adds the possibility to return a custom schema during init
   * If no schema returned, init needs to call exec to get the schema, leading exec to be called twice
   *
   * @param session the Spark Session
   * @param config the input config of the associated action
   * @return the schema of the custom DataFrame
   */
  def schema(session: SparkSession, config: Map[String,String]): Option[StructType] = None
}

/**
 * Configuration of a custom Spark-DataFrame creator as part of [[CustomDfDataObject]]
 * Define a exec function which receives a map of options and returns a DataFrame to be used as input.
 * Optionally define a schema function to return a StructType used as schema in init-phase.
 * See also trait [[CustomDfCreator]].
 *
 * Note that for now implementing CustomDfCreator.schema method is only possible with className configuration attribute.
 *
 * @param className Optional class name implementing trait [[CustomDfCreator]]
 * @param scalaFile Optional file where scala code for creator is loaded from. The scala code in the file needs to be a function of type [[fnExecType]].
 * @param scalaCode Optional scala code for creator. The scala code needs to be a function of type [[fnExecType]].
 * @param options Options to pass to the creator
 */
case class CustomDfCreatorConfig(className: Option[String] = None,
                                 scalaFile: Option[String] = None,
                                 scalaCode: Option[String] = None,
                                 options: Option[Map[String,String]] = None
                                ) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined, "Either className, scalaFile or scalaCode must be defined for CustomDfCreator")

  val fnEmptySchema: CustomDfCreatorConfig.fnSchemaType = (a, b) => None

  val impl : CustomDfCreator = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomDfCreator](clazz)
  }.orElse{
    scalaFile.map {
      file =>
        implicit val defaultHadoopConf: Configuration = new Configuration()
        val fnExec = CustomCodeUtil.compileCode[fnExecType](HdfsUtil.readHadoopFile(file))
        new CustomDfCreatorWrapper(fnExec, fnEmptySchema)
    }
  }.orElse{
    scalaCode.map {
      code =>
        val fnExec = CustomCodeUtil.compileCode[fnExecType](code)
        new CustomDfCreatorWrapper(fnExec, fnEmptySchema)
    }
  }.get

  def exec(implicit context: ActionPipelineContext): DataFrame = {
    impl.exec(context.sparkSession, options.getOrElse(Map()))
  }

  def schema(implicit context: ActionPipelineContext): Option[StructType] = {
    impl.schema(context.sparkSession, options.getOrElse(Map()))
  }

  override def toString: String = {
    if (className.isDefined)       "className: "+className.get
    else if (scalaFile.isDefined)  "scalaFile: "+scalaFile.get
    else                          "scalaCode: "+scalaCode.get
  }
}

object CustomDfCreatorConfig {
  type fnSchemaType = (SparkSession, Map[String, String]) => Option[StructType]
  type fnExecType = (SparkSession, Map[String, String]) => DataFrame
}

class CustomDfCreatorWrapper(val fnExec: fnExecType,
                             val fnSchema: fnSchemaType) extends CustomDfCreator {

  override def exec(session: SparkSession, config: Map[String, String]): DataFrame = fnExec(session, config)

  override def schema(session: SparkSession, config: Map[String, String]): Option[StructType] = fnSchema(session, config)
}
