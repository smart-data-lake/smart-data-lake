package io.smartdatalake.workflow.action.customlogic

import io.smartdatalake.smartdatalake.{SnowparkDataFrame, SnowparkSession, SnowparkStructType}
import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.customlogic.CustomSnowparkDfCreatorConfig.{fnSnowparkExecType, fnSnowparkSchemaType}

trait CustomSnowparkDfCreator extends Serializable {
  def exec(config: Map[String,String]): SnowparkDataFrame
  def schema(config: Map[String,String]): Option[SnowparkStructType] = None
}

case class CustomSnowparkDfCreatorConfig(className: Option[String] = None,
                                 options: Option[Map[String,String]] = None
                                ) {
  require(className.isDefined, "ClassName  must be defined for CustomSnowparkDfCreator")

  val fnEmptySchema: CustomDfCreatorConfig.fnSchemaType = (a, b) => None

  val impl : CustomSnowparkDfCreator = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomSnowparkDfCreator](clazz)
  }.get

  def exec(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    impl.exec(options.getOrElse(Map()))
  }

  def schema(implicit context: ActionPipelineContext): Option[SnowparkStructType] = {
    impl.schema(options.getOrElse(Map()))
  }

  override def toString: String = {
    "className: "+className.get
  }
}

object CustomSnowparkDfCreatorConfig {
  type fnSnowparkSchemaType = (Map[String, String]) => Option[SnowparkStructType]
  type fnSnowparkExecType = (Map[String, String]) => SnowparkDataFrame
}

class CustomDfCreatorWrapper(val fnExec: fnSnowparkExecType,
                             val fnSchema: fnSnowparkSchemaType) extends CustomSnowparkDfCreator {

  override def exec(config: Map[String, String]): SnowparkDataFrame = fnExec(config)

  override def schema(config: Map[String, String]): Option[SnowparkStructType] = fnSchema(config)
}

