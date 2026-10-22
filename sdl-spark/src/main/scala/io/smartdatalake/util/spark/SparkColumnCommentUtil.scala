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
package io.smartdatalake.util.spark

import io.smartdatalake.util.misc.{ScaladocUtil, SmartDataLakeLogger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{ArrayEncoder, IterableEncoder, OptionEncoder, ProductEncoder}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Expression, GetStructField, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SdlDataFrameMetadata
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata, StructField, StructType}
import scaladoc.Tag

import scala.collection.mutable
import scala.reflect.runtime.universe.Type
import scala.util.{Failure, Success, Try}

/**
 * Enrich column comments of a Spark DataFrame with the ScalaDoc of the case classes the columns originate from.
 *
 * To structure complex transformations, data is often modelled as a Scala case class. Its attributes become
 * (nested) columns of the resulting DataFrame. As the attributes are usually documented in the ScalaDoc of the
 * case class, these descriptions can be reused as column comments, which are then propagated to the target
 * DataObject and shown in the SDLB UI. See issue #765.
 *
 * There are two ways to get to the case class:
 *  - [[enrichColumnCommentsFromUdfs]] recovers it from the analyzed logical plan. A [[ScalaUDF]] carries the
 *    `outputEncoder` it was created with, and its `ClassTag` gives the returned class. This covers UDFs registered
 *    through `SparkClassicConnection.sparkUDFs` as well as UDFs created inside a custom transformer.
 *  - [[enrichColumnCommentsFromCaseClass]] takes the type from the caller, which is used for transformation
 *    methods declaring a typed return value, e.g. `def transform(...): Dataset[MyCaseClass]`.
 *
 * Note that the ScalaDoc is only available at runtime if the case class was compiled with the
 * `com.github.takezoe:runtime-scaladoc-reader` compiler plugin, which adds it as an annotation.
 */
private[smartdatalake] object SparkColumnCommentUtil extends SmartDataLakeLogger {

  /**
   * Identifies a column defined by a user defined function.
   *
   * @param cls       the class returned by the user defined function
   * @param attribute if the column is not the whole return value but one of its attributes, e.g. `myUdf(x).myAttr`,
   *                  the name of that attribute
   */
  private case class UdfResult(cls: Class[_], attribute: Option[String])

  /**
   * Add column comments from the ScalaDoc of case classes returned by user defined functions used in this DataFrame.
   * Existing comments are never overwritten.
   *
   * As column comments are purely descriptive this is best effort - any error is logged and the DataFrame is
   * returned unchanged.
   */
  def enrichColumnCommentsFromUdfs(df: DataFrame): DataFrame = {
    Try(enrichColumnCommentsFromUdfsImpl(df)) match {
      case Success(enrichedDf) => enrichedDf
      case Failure(e) =>
        logger.warn(s"Could not enrich column comments from UDF ScalaDoc: ${e.getClass.getSimpleName} ${e.getMessage}")
        df
    }
  }

  /**
   * Add column comments from the ScalaDoc of a case class the DataFrame was created from, e.g. by a transformer
   * returning a `Dataset[MyCaseClass]`. The top level columns of the DataFrame are matched against the attributes
   * of the case class. Existing comments are never overwritten.
   *
   * Note that a case class can not be recovered from a DataFrame itself: `df.as[MyCaseClass]` only changes the
   * encoder of the Dataset and leaves no trace in its logical plan. The type therefore has to be passed in by the
   * caller, which knows it from the signature of the transformation method.
   *
   * As column comments are purely descriptive this is best effort - any error is logged and the DataFrame is
   * returned unchanged.
   */
  def enrichColumnCommentsFromCaseClass(df: DataFrame, tpe: Type): DataFrame = {
    Try(enrichColumnCommentsFromCaseClassImpl(df, tpe)) match {
      case Success(enrichedDf) => enrichedDf
      case Failure(e) =>
        logger.warn(s"Could not enrich column comments from ScalaDoc of $tpe: ${e.getClass.getSimpleName} ${e.getMessage}")
        df
    }
  }

  private def enrichColumnCommentsFromCaseClassImpl(df: DataFrame, tpe: Type): DataFrame = {
    // columns are matched by name below, which is ambiguous if there are duplicate column names
    if (df.schema.fieldNames.distinct.size != df.schema.fieldNames.length) return df
    val targetSchema = enrichStruct(df.schema, tpe)
    if (targetSchema == df.schema) df
    else {
      logger.debug(s"Enriching column comments from ScalaDoc of $tpe")
      SdlDataFrameMetadata.withColumnMetadata(df, targetSchema)
    }
  }

  private def enrichColumnCommentsFromUdfsImpl(df: DataFrame): DataFrame = {
    val plan = df.queryExecution.analyzed
    // cheap early exit - the vast majority of DataFrames contain no Scala UDF at all
    if (!containsScalaUdf(plan)) return df
    // columns are matched by name below, which is ambiguous if there are duplicate column names
    if (df.schema.fieldNames.distinct.size != df.schema.fieldNames.length) return df

    // map the expression id of every column defined by a UDF to the corresponding UDF result
    val udfResultByExprId = collectUdfResults(plan)
    // resolve the output columns of the DataFrame which stem from a UDF
    val udfResultByColumnName = plan.output.flatMap(a => udfResultByExprId.get(a.exprId).map(a.name -> _)).toMap
    if (udfResultByColumnName.isEmpty) return df
    logger.debug(s"Found UDF result columns ${udfResultByColumnName.mkString(", ")}")

    val targetSchema = StructType(df.schema.fields.map { field =>
      udfResultByColumnName.get(field.name)
        .flatMap(udfResult => Try(getType(udfResult.cls)).toOption.map((udfResult, _)))
        .map { case (udfResult, tpe) => enrichField(field, tpe, udfResult.attribute) }
        .getOrElse(field)
    })
    if (targetSchema == df.schema) df
    else {
      logger.debug(s"Enriching column comments from UDF ScalaDoc for columns ${udfResultByColumnName.keys.mkString(", ")}")
      SdlDataFrameMetadata.withColumnMetadata(df, targetSchema)
    }
  }

  private def containsScalaUdf(plan: LogicalPlan): Boolean =
    plan.exists(_.expressions.exists(_.exists(_.isInstanceOf[ScalaUDF])))

  /**
   * Collect all aliases of the plan which are defined by a UDF and map their expression id to the UDF result.
   *
   * The plan is traversed bottom up, so that an alias referencing a column defined further down in the plan can
   * inherit its UDF result. This makes the mapping robust against renaming and intermediate projections, which
   * would otherwise hide the UDF, e.g. `select(myUdf(x).as("a")).withColumnRenamed("a", "b")`.
   */
  private def collectUdfResults(plan: LogicalPlan): Map[ExprId, UdfResult] = {
    val udfResults = mutable.Map[ExprId, UdfResult]()
    plan.foreachUp { node =>
      node.expressions.foreach(_.foreach {
        case alias: Alias => udfResult(alias.child, udfResults).foreach(udfResults.put(alias.exprId, _))
        case _ => ()
      })
    }
    udfResults.toMap
  }

  /**
   * Get the UDF result defining this expression, if any.
   *
   * Note that only UDFs created with a TypeTag, e.g. `udf((x: String) => MyCaseClass(x))`, have an outputEncoder.
   * UDFs created with an explicit return DataType, e.g. `udf(new UDF1[String, Row] {...}, myDataType)`, carry no
   * type information and are therefore ignored.
   */
  private def udfResult(expression: Expression, known: mutable.Map[ExprId, UdfResult]): Option[UdfResult] = expression match {
    case udf: ScalaUDF => udf.outputEncoder.flatMap(e => caseClassOf(e.encoder)).map(UdfResult(_, None))
    case attribute: AttributeReference => known.get(attribute.exprId)
    // the column is a single attribute of the UDF return value, e.g. myUdf(x).myAttr
    case g: GetStructField =>
      udfResult(g.child, known).filter(_.attribute.isEmpty).map(_.copy(attribute = Some(g.extractFieldName)))
    case other =>
      // The UDF result can be wrapped in additional expressions, e.g. Dataset.to rebuilds a struct column with
      // named_struct. Inherit the UDF result if the expression is derived from exactly one column, as otherwise
      // it combines multiple columns and can not be attributed to a single UDF.
      val references = other.references.toSeq
      if (references.size == 1) known.get(references.head.exprId) else None
  }

  /**
   * Get the case class an encoder is built for, unwrapping collection and option encoders.
   * Returns None if the encoder is not built for a case class, e.g. for a UDF returning a simple type.
   */
  private def caseClassOf(encoder: AgnosticEncoder[_]): Option[Class[_]] = encoder match {
    case e: ProductEncoder[_] => Some(e.clsTag.runtimeClass)
    case e: OptionEncoder[_] => caseClassOf(e.elementEncoder)
    case e: ArrayEncoder[_] => caseClassOf(e.element)
    case e: IterableEncoder[_, _] => caseClassOf(e.element)
    case _ => None
  }

  /**
   * Add the ScalaDoc of the case class as comments to the field and its nested fields.
   * Existing comments are never overwritten.
   *
   * @param tpe       type of the case class returned by the UDF
   * @param attribute if the field is a single attribute of the UDF return value, the name of that attribute
   */
  private def enrichField(field: StructField, tpe: Type, attribute: Option[String]): StructField = attribute match {
    case None =>
      // The field is the whole return value, so its nested fields correspond to the attributes of the case class.
      // Only a struct can carry the attributes - anything else is not documented by the case class ScalaDoc.
      field.dataType match {
        case dt: StructType =>
          val enrichedType = enrichStruct(dt, tpe)
          addComment(field.copy(dataType = enrichedType), classDescription(tpe))
        case dt: ArrayType if dt.elementType.isInstanceOf[StructType] =>
          val enrichedType = enrichStruct(dt.elementType.asInstanceOf[StructType], tpe)
          addComment(field.copy(dataType = dt.copy(elementType = enrichedType)), classDescription(tpe))
        case _ => field
      }
    case Some(attributeName) =>
      // The field is one attribute of the return value. Reuse enrichSchemaCommentsFromCaseClass by looking the
      // attribute up in a schema of its own, so that nested comments are resolved the same way.
      val attributeSchema = StructType(Seq(field.copy(name = attributeName, metadata = Metadata.empty)))
      val enrichedAttribute = SparkSchemaUtil.enrichSchemaCommentsFromCaseClass(attributeSchema, tpe).fields.head
      val mergedType = preferExistingComments(field.dataType, enrichedAttribute.dataType)
      addComment(field.copy(dataType = mergedType), enrichedAttribute.getComment())
  }

  private def enrichStruct(schema: StructType, tpe: Type): StructType =
    preferExistingComments(schema, SparkSchemaUtil.enrichSchemaCommentsFromCaseClass(schema, tpe)).asInstanceOf[StructType]

  /**
   * SparkSchemaUtil.enrichSchemaCommentsFromCaseClass overwrites existing comments. Restore the comments of the
   * original schema, so that comments set explicitly, e.g. through `schemaMin`, always win over the ScalaDoc.
   */
  private def preferExistingComments(original: DataType, enriched: DataType): DataType = (original, enriched) match {
    case (o: StructType, e: StructType) =>
      StructType(e.fields.map { enrichedField =>
        o.fields.find(_.name == enrichedField.name).map { originalField =>
          val dataType = preferExistingComments(originalField.dataType, enrichedField.dataType)
          // keep all metadata of the original field, and only add the comment if it has none
          addComment(enrichedField.copy(dataType = dataType, metadata = originalField.metadata), enrichedField.getComment())
        }.getOrElse(enrichedField)
      })
    case (o: ArrayType, e: ArrayType) => e.copy(elementType = preferExistingComments(o.elementType, e.elementType))
    case _ => enriched
  }

  private def addComment(field: StructField, comment: Option[String]): StructField = {
    if (field.getComment().isDefined) field // never overwrite an existing comment
    else comment.filter(_.nonEmpty).map(field.withComment).getOrElse(field)
  }

  /**
   * Get the description of the ScalaDoc of a class, i.e. the text before the first tag.
   */
  private def classDescription(tpe: Type): Option[String] = {
    ScaladocUtil.extractScalaDoc(tpe.typeSymbol.annotations)
      .flatMap(_.tags.collectFirst { case x: Tag.Description => ScaladocUtil.formatScaladocMarkup(x.makrup) })
  }

  private def getType(cls: Class[_]): Type = mirror.classSymbol(cls).toType

  private lazy val mirror = scala.reflect.runtime.currentMirror
}
