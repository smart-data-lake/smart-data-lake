/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.smartdatalake.util.xml

import java.io.{File, FileInputStream, InputStreamReader, StringReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Path

import scala.collection.JavaConverters._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types._
import org.apache.ws.commons.schema._
import org.apache.ws.commons.schema.constants.Constants

/**
 * Utility to generate a Spark schema from an XSD. Not all XSD schemas are simple tabular schemas,
 * so not all elements or XSDs are supported.
 *
 * Note: this is copied from com.databricks.spark.xml.util and extended with support for
 * - recursive schemas definitions
 */
@Experimental
object XsdSchemaConverter {

  /**
   * Reads a schema from an XSD file.
   * Note that if the schema consists of one complex parent type which you want to use as
   * the row tag schema, then you will need to extract the schema of the single resulting
   * struct in the resulting StructType, and use its StructType as your schema.
   *
   * @param xsdFile XSD file
   * @return Spark-compatible schema
   */
  @Experimental
  def read(xsdFile: File, maxRecursion: Int): StructType = {
    val xmlSchemaCollection = new XmlSchemaCollection()
    xmlSchemaCollection.setBaseUri(xsdFile.getParent)
    val xmlSchema = xmlSchemaCollection.read(
      new InputStreamReader(new FileInputStream(xsdFile), StandardCharsets.UTF_8)
    )
    new XsdSchemaConverter(xmlSchema, maxRecursion).getStructType
  }

  /**
   * Reads a schema from an XSD file.
   * Note that if the schema consists of one complex parent type which you want to use as
   * the row tag schema, then you will need to extract the schema of the single resulting
   * struct in the resulting StructType, and use its StructType as your schema.
   *
   * @param xsdFile XSD file
   * @return Spark-compatible schema
   */
  @Experimental
  def read(xsdFile: Path, maxRecursion: Int): StructType = read(xsdFile.toFile, maxRecursion)

  /**
   * Reads a schema from an XSD as a string.
   * Note that if the schema consists of one complex parent type which you want to use as
   * the row tag schema, then you will need to extract the schema of the single resulting
   * struct in the resulting StructType, and use its StructType as your schema.
   *
   * @param xsdString XSD as a string
   * @return Spark-compatible schema
   */
  @Experimental
  def read(xsdString: String, maxRecursion: Int): StructType = {
    val xmlSchema = new XmlSchemaCollection().read(new StringReader(xsdString))
    new XsdSchemaConverter(xmlSchema, maxRecursion).getStructType
  }
}
class XsdSchemaConverter(xmlSchema: XmlSchema, maxRecursion: Int) {
  private def getStructField(schemaType: XmlSchemaType, path: Seq[String]): Option[StructField] = {
    schemaType match {
      // xs:simpleType
      case simpleType: XmlSchemaSimpleType =>
        val schemaType = simpleType.getContent match {
          case restriction: XmlSchemaSimpleTypeRestriction =>
            simpleType.getQName match {
              case Constants.XSD_BOOLEAN => BooleanType
              case Constants.XSD_DECIMAL =>
                val scale = restriction.getFacets.asScala.collectFirst {
                  case facet: XmlSchemaFractionDigitsFacet => facet
                }
                scale match {
                  case Some(scale) => DecimalType(38, scale.getValue.toString.toInt)
                  case None => DecimalType(38, 18)
                }
              case Constants.XSD_UNSIGNEDLONG => DecimalType(38, 0)
              case Constants.XSD_DOUBLE => DoubleType
              case Constants.XSD_FLOAT => FloatType
              case Constants.XSD_BYTE => ByteType
              case Constants.XSD_SHORT |
                   Constants.XSD_UNSIGNEDBYTE => ShortType
              case Constants.XSD_INTEGER |
                   Constants.XSD_NEGATIVEINTEGER |
                   Constants.XSD_NONNEGATIVEINTEGER |
                   Constants.XSD_NONPOSITIVEINTEGER |
                   Constants.XSD_POSITIVEINTEGER |
                   Constants.XSD_UNSIGNEDSHORT => IntegerType
              case Constants.XSD_LONG |
                   Constants.XSD_UNSIGNEDINT => LongType
              case Constants.XSD_DATE => DateType
              case Constants.XSD_DATETIME => TimestampType
              case _ => StringType
            }
          case _ => StringType
        }
        Some(StructField("baseName", schemaType))

      // xs:complexType
      case complexType: XmlSchemaComplexType =>
        complexType.getContentModel match {
          // check max recursion
          case _ if complexType.getName != null && path.count(_ == complexType.getName) >= maxRecursion =>
            None
          case content: XmlSchemaSimpleContent =>
            // xs:simpleContent
            content.getContent match {
              case extension: XmlSchemaSimpleContentExtension =>
                val baseStructField = getStructField(xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName), path :+ complexType.getName)
                val value = baseStructField.map(f => StructField("_VALUE", f.dataType))
                val attributes = extension.getAttributes.asScala.flatMap {
                  case attribute: XmlSchemaAttribute =>
                    val baseStructField = getStructField(xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName), path :+ attribute.getName)
                    baseStructField.map(f => StructField(s"_${attribute.getName}", f.dataType, attribute.getUse != XmlSchemaUse.REQUIRED))
                }
                val fields = value.toSeq ++ attributes
                if (fields.nonEmpty) Some(StructField(complexType.getName, StructType(fields)))
                else None
              case unsupported =>
                throw new IllegalArgumentException(s"Unsupported content: $unsupported")
            }
          case content: XmlSchemaComplexContent =>
            // xs:complexContent
            content.getContent match {
              case extension: XmlSchemaComplexContentExtension =>
                val baseField = getStructField(xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName), path :+ complexType.getName)
                val baseFields = baseField.map(_.dataType).map {
                  case StructType(fields) => fields.toSeq
                }.getOrElse(Seq())
                val childFields = mapParticle(complexType.getParticle, path :+ complexType.getName)
                val attributes = mapAttributes(complexType.getAttributes.asScala, path)
                val fields = baseFields ++ childFields ++ attributes
                if (fields.nonEmpty) Some(StructField(complexType.getName, StructType(fields)))
                else None
              case unsupported =>
                throw new IllegalArgumentException(s"Unsupported content: $unsupported")
            }
          case null =>
            val childFields = mapParticle(complexType.getParticle, path :+ complexType.getName)
            val attributes = mapAttributes(complexType.getAttributes.asScala, path :+ complexType.getName)
            val fields = childFields ++ attributes
            if (fields.nonEmpty) Some(StructField(complexType.getName, StructType(fields)))
            else None
          case unsupported =>
            throw new IllegalArgumentException(s"Unsupported content model: $unsupported")
        }
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported schema element type: $unsupported")
    }
  }

  private def mapParticle(particle: XmlSchemaParticle, path: Seq[String]): Seq[StructField] = {
    particle match {
      // xs:all
      case all: XmlSchemaAll =>
        all.getItems.asScala.flatMap {
          case element: XmlSchemaElement =>
            val baseType = getStructField(element.getSchemaType, path :+ element.getName).map(_.dataType)
            baseType.map { t =>
              val nullable = element.getMinOccurs == 0
              StructField(element.getName, t, nullable)
            }
        }
      // xs:choice
      case choice: XmlSchemaChoice =>
        choice.getItems.asScala.flatMap {
          case e: XmlSchemaElement =>
            val baseType = getStructField(e.getSchemaType, path :+ e.getName).map(_.dataType)
            baseType.map { t =>
              if (math.max(e.getMaxOccurs, choice.getMaxOccurs) > 1) {
                StructField(e.getName, ArrayType(t), true)
              } else {
                StructField(e.getName, t, true)
              }
            }
          case any: XmlSchemaAny =>
            val dataType = if (math.max(any.getMaxOccurs, choice.getMaxOccurs) > 1) ArrayType(StringType) else StringType
            Some(StructField(XmlOptions.DEFAULT_WILDCARD_COL_NAME, dataType, true))
        }
      // xs:sequence
      case sequence: XmlSchemaSequence =>
        // flatten xs:choice nodes
        sequence.getItems.asScala.flatMap( _ match {
          case choice: XmlSchemaChoice =>
            choice.getItems.asScala.map { e =>
              val xme = e.asInstanceOf[XmlSchemaElement]
              val baseType = getStructField(xme.getSchemaType, path :+ xme.getName).map(_.dataType)
              baseType.map { t =>
                val dataType = if (math.max(xme.getMaxOccurs, sequence.getMaxOccurs) > 1) ArrayType(t) else t
                StructField(xme.getName, dataType, true)
              }
            }
          case e: XmlSchemaElement =>
            val baseType = getStructField(e.getSchemaType, path :+ e.getName).map(_.dataType)
            val structField = baseType.map { t =>
              val dataType = if (math.max(e.getMaxOccurs, sequence.getMaxOccurs) > 1) ArrayType(t) else t
              val nullable = e.getMinOccurs == 0
              StructField(e.getName, dataType, nullable)
            }
            Seq(structField)
          case any: XmlSchemaAny =>
            val dataType = if (math.max(any.getMaxOccurs, sequence.getMaxOccurs) > 1) ArrayType(StringType) else StringType
            val nullable = any.getMinOccurs == 0
            Seq(Some(StructField(XmlOptions.DEFAULT_WILDCARD_COL_NAME, dataType, nullable)))
          case unsupported =>
            throw new IllegalArgumentException(s"Unsupported item: $unsupported")
        }).flatten
      case null =>
        Seq.empty
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported particle: $unsupported")
    }
  }

  private def mapAttributes(attributes: Seq[XmlSchemaAttributeOrGroupRef], path: Seq[String]): Seq[StructField] = {
    attributes.flatMap {
      case attribute: XmlSchemaAttribute => Seq(mapAttribute(attribute, path))
      case attributeGroupRef: XmlSchemaAttributeGroupRef =>
        val attributeGroup = xmlSchema.getAttributeGroupByName(attributeGroupRef.getTargetQName)
        attributeGroup.getAttributes.asScala.map{
          case attribute: XmlSchemaAttribute => mapAttribute(attribute, path)
        }
    }.flatten
  }

  private def mapAttribute(attribute: XmlSchemaAttribute, path: Seq[String]): Option[StructField] = {
    val attributeType = attribute.getSchemaTypeName match {
      case null => Some(StringType)
      case t => getStructField(xmlSchema.getParent.getTypeByQName(t), path :+ attribute.getName).map(_.dataType)
    }
    attributeType.map { t =>
      StructField(s"_${attribute.getName}", t, attribute.getUse != XmlSchemaUse.REQUIRED)
    }
  }

  private def getStructType: StructType = {
    StructType(xmlSchema.getElements.asScala.toSeq.map { case (_, schemaElement) =>
      val schemaType = schemaElement.getSchemaType
      // if (schemaType.isAnonymous) {
      //   schemaType.setName(qName.getLocalPart)
      // }
      val rootType = getStructField(schemaType, Seq(schemaElement.getName)).get
      StructField(schemaElement.getName, rootType.dataType, schemaElement.getMinOccurs == 0)
    })
  }

}

private[xml] object XmlOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "_"
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_DECLARATION = "version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\""
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null
  val DEFAULT_WILDCARD_COL_NAME = "xs_any"
}