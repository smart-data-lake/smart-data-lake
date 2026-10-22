/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action.generic.customlogic

import io.smartdatalake.util.misc.{DynamicTransformContext, DynamicTransformMethodWrapper, TransformParameterMapper, TransformReturnMapper}

import scala.reflect.runtime.universe

/**
 * A trait for all transformers having a custom transform method which allows to extract detailed transformation
 * parameter information.
 */
trait CustomTransformMethodDef {

  /**
   * The custom transform method of this transformer, or None if it implements the standard transform method of its
   * interface only.
   */
  private[smartdatalake] def customTransformMethod: Option[universe.MethodSymbol]
}

/**
 * Implementation of [[CustomTransformMethodDef]] which searches a custom transform method by reflection.
 * The standard transform method defined by the transformer interface is filtered out, so that customTransformMethod
 * is only defined if the transformer implements a transform method with a different signature.
 */
trait CustomTransformMethodDefImpl extends CustomTransformMethodDef {

  /**
   * Type signature of the standard transform method of the transformer interface.
   */
  protected def stdTransformMethodSignature: universe.Type

  /**
   * Message to show if the transformer implements no method with name [[transformMethodName]] at all.
   */
  protected def transformMethodHelpMsg: String

  /**
   * Name of the method implementing the transformation. Default is 'transform'.
   * This can be overridden to call code which was not written as transformer interface implementation, e.g. a
   * function of a Notebook.
   */
  protected def transformMethodName: String = CustomTransformMethodDefImpl.STD_TRANSFORM_METHOD_NAME

  /**
   * Detect the standard transform method of the transformer interface, which is not called dynamically.
   * Note that the standard method must only be filtered out if we are looking for methods with the same name.
   */
  protected def isStdTransformMethod(method: universe.MethodSymbol): Boolean = {
    transformMethodName == CustomTransformMethodDefImpl.STD_TRANSFORM_METHOD_NAME &&
      method.typeSignature =:= stdTransformMethodSignature
  }

  @transient override private[smartdatalake] lazy val customTransformMethod: Option[universe.MethodSymbol] =
    DynamicTransformMethodWrapper.findCustomTransformMethod(getClass, transformMethodName, isStdTransformMethod, transformMethodHelpMsg)
}

/**
 * Implements the dynamic call of a custom transform method for a transformer interface.
 *
 * The transformer interface implements its standard transform method by calling [[callDynamicTransform]], which
 * looks up the custom transform method of the implementing class and fills its parameters from the input
 * DataFrames and options, see [[DynamicTransformMethodWrapper]].
 */
trait DynamicTransform extends CustomTransformMethodDefImpl {

  /**
   * Mappers to fill the parameters of the custom transform method, engine specific.
   */
  protected def transformParameterMappers: Seq[TransformParameterMapper]

  /**
   * Mapper to convert the return value of the custom transform method, engine specific.
   */
  protected def transformReturnMapper: TransformReturnMapper

  @transient private[smartdatalake] lazy val customTransformMethodWrapper: Option[DynamicTransformMethodWrapper] =
    customTransformMethod.map(new DynamicTransformMethodWrapper(_, transformParameterMappers, transformReturnMapper))

  /**
   * Dynamically call the custom transform method and return the transformed DataFrames by name.
   */
  protected def callDynamicTransform(ctx: DynamicTransformContext): Map[String, Any] = {
    require(customTransformMethodWrapper.isDefined,
      s"${this.getClass.getSimpleName} transform method is not overridden and no custom transform method is defined")
    val wrapper = customTransformMethodWrapper.get
    require(wrapper.returnsSingleDataFrame || wrapper.returnsMultipleDataFrames,
      s"The return type of the transform method is ${wrapper.method.returnType}, but should be a DataFrame or a Map of DataFrames")
    wrapper.call(this, ctx)
  }

  /**
   * Dynamically call the custom transform method of a 1:1 transformation, which has to return one DataFrame.
   */
  protected def callDynamicTransformSingleOutput(ctx: DynamicTransformContext): Any = {
    require(customTransformMethodWrapper.isDefined,
      s"${this.getClass.getSimpleName} transform method is not overridden and no custom transform method is defined")
    val wrapper = customTransformMethodWrapper.get
    require(wrapper.returnsSingleDataFrame,
      s"The return type of the transform method is ${wrapper.method.returnType}, but should be a DataFrame as this is a 1:1 transformation")
    wrapper.call(this, ctx).values.head
  }
}

object CustomTransformMethodDefImpl {
  val STD_TRANSFORM_METHOD_NAME = "transform"
}

object DynamicTransform {

  /**
   * Option passed to 1:1 transformations, holding the id of the DataObject of the input SubFeed.
   * This allows a custom transform method to get it as parameter `dataObjectId: String`.
   */
  val OPTION_DATAOBJECT_ID = "dataObjectId"
}

/**
 * A trait to provide detailed information about a transformation
 */
trait TransformInfo {

  /**
   * Get names of input DataObjects. The Names can be DataObjectIds or names of intermediate DataFrames.
   * @return None if input DataObjects are unknown, otherwise a list of input DataObjects in CamelCase notation.
   */
  def getInputDataObjectsNameAndType: Option[Seq[(String, universe.Type)]]

  /**
   * If the transformer has only one output DataObject
   */
  def isSingleOutput: Boolean

  /**
   * If the transformer has only one input DataObject
   */
  def isSingleInput: Boolean
}
