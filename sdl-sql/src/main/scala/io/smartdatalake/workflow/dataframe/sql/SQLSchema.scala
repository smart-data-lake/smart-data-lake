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
package io.smartdatalake.workflow.dataframe.sql

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.sqlglot.SqlGlotField
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.json4s.{DefaultFormats, Formats, JNothing, JObject, JString, JValue}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Schema of the SQL engine. Data types are SQLGlot data types.
 * Nullability is not inferred by SQLGlot, fields of inferred schemas are therefore always nullable.
 */
case class SQLSchema(fields: Seq[SQLField]) extends GenericSchema {
  override def subFeedType: Type = typeOf[SQLSubFeed]

  override def diffSchema(schema: GenericSchema): Option[GenericSchema] = schema match {
    case other: SQLSchema =>
      val otherFields = other.toLowerCase.fields.toSet
      val diff = toLowerCase.fields.filterNot(otherFields.contains)
      if (diff.isEmpty) None else Some(SQLSchema(diff))
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
  }

  override def columns: Seq[String] = fields.map(_.name)

  override def sql: String = fields.map(f => s"${SQLDataType.quoteIdentifier(f.name)} ${f.dataType.sql}${if (f.nullable) "" else " NOT NULL"}").mkString(", ")

  override def add(colName: String, dataType: GenericDataType): SQLSchema = add(SQLField(colName, SQLDataType.of(dataType)))

  override def add(field: GenericField): SQLSchema = copy(fields = fields :+ SQLField.of(field))

  override def remove(colName: String): SQLSchema = copy(fields = fields.filterNot(_.name == colName))

  override def filter(func: GenericField => Boolean): SQLSchema = copy(fields = fields.filter(func))

  override def getEmptyDataFrame(dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SQLDataFrame =
    SQLSubFeed.getEmptyDataFrame(this, dataObjectId)

  override def getDataType(colName: String): SQLDataType = fields.find(_.name == colName).map(_.dataType)
    .getOrElse(throw new IllegalArgumentException(s"Column $colName not found in schema $sql"))

  override def makeNullable: SQLSchema = copy(fields = fields.map(_.makeNullable))

  override def toLowerCase: SQLSchema = copy(fields = fields.map(_.toLowerCase))

  override def removeMetadata: SQLSchema = copy(fields = fields.map(_.removeMetadata))

  override def treeString(level: Int): String =
    fields.map(f => s" - ${f.name}: ${f.dataType.sql}${if (!f.nullable) " not null" else ""}").mkString(System.lineSeparator())

  /**
   * Columns and types as expected by the Python bridge
   */
  private[sql] def toBridge: Seq[Seq[String]] = fields.map(f => Seq(f.name, f.dataType.sql))
}

object SQLSchema {
  def of(schema: GenericSchema): SQLSchema = schema match {
    case s: SQLSchema => s
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
  }

  def fromBridge(fields: Seq[SqlGlotField]): SQLSchema =
    SQLSchema(fields.map(f => SQLField(f.name, SQLDataType.fromBridge(f.dataType))))
}

case class SQLField(name: String, dataType: SQLDataType, nullable: Boolean = true, comment: Option[String] = None) extends GenericField {
  override def subFeedType: Type = typeOf[SQLSubFeed]
  override def makeNullable: SQLField = copy(nullable = true, dataType = dataType.makeNullable)
  override def toLowerCase: SQLField = copy(name = name.toLowerCase, dataType = dataType.toLowerCase)
  override def removeMetadata: SQLField = copy(comment = None, dataType = dataType.removeMetadata)
  override def withDataType(dataType: GenericDataType, nullable: Boolean): SQLField = copy(dataType = SQLDataType.of(dataType), nullable = nullable)
}

object SQLField {
  def of(field: GenericField): SQLField = field match {
    case f: SQLField => f
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(field)
  }
}

/**
 * Data type of the SQL engine. `sql` is the type in the default SQLGlot dialect.
 */
sealed trait SQLDataType extends GenericDataType {
  override def subFeedType: Type = typeOf[SQLSubFeed]

  /**
   * The type in Spark SQL, as used in column expressions, see [[SQLColumn]]
   */
  def sparkSql: String
  override def isSameType(other: GenericDataType): Boolean = other match {
    case o: SQLDataType => sql.equalsIgnoreCase(o.sql)
    case _ => false
  }
  override def makeNullable: SQLDataType = this
  override def removeMetadata: SQLDataType = this
  override def toLowerCase: SQLDataType
}

case class SQLSimpleDataType(sql: String) extends SQLDataType with GenericSimpleDataType {
  /**
   * The type name without parameters, e.g. DECIMAL for DECIMAL(10, 2)
   */
  lazy val baseType: String = sql.takeWhile(_ != '(').trim.toUpperCase

  /**
   * The numeric parameters of the type, e.g. Seq(10, 2) for DECIMAL(10, 2), or Seq(20) for VARCHAR(20)
   */
  lazy val parameters: Seq[Int] = sql.dropWhile(_ != '(').drop(1).takeWhile(_ != ')').split(',').map(_.trim)
    .filter(_.nonEmpty).flatMap(_.toIntOption).toSeq
  override def typeName: String = standardizeTypeName(baseType)
  override def isSortable: Boolean = true
  override def isNumeric: Boolean = SQLDataType.numericTypes.contains(baseType)
  override def isImpreciseNumeric: Boolean = SQLDataType.impreciseNumericTypes.contains(baseType)
  override def getDecimalSpec: Option[(Int, Int)] = {
    if (baseType != "DECIMAL") None
    else parameters match {
      case Seq(precision, scale) => Some((precision, scale))
      case Seq(precision) => Some((precision, 0))
      case _ => None
    }
  }
  override def toLowerCase: SQLSimpleDataType = this
  /**
   * The Spark type name, which is the engine-neutral name of simple types in the Json representation of a schema,
   * see GenericSchema.toJson. Types without Spark equivalent are given by their SQL.
   */
  override def toJson: JValue = JString(SQLDataType.sparkTypeName(this).getOrElse(sql))
  // TIMESTAMP of Spark SQL has a time zone, TIMESTAMP of the default SQLGlot dialect has none
  override def sparkSql: String = if (baseType == "TIMESTAMP") "TIMESTAMP_NTZ" + sql.dropWhile(_ != '(') else sql
}

case class SQLStructDataType(fields: Seq[SQLField]) extends SQLDataType with GenericStructDataType {
  override def typeName: String = "struct"
  override def sql: String = s"STRUCT<${fields.map(f => s"${SQLDataType.quoteIdentifier(f.name)} ${f.dataType.sql}").mkString(", ")}>"
  override def sparkSql: String = s"STRUCT<${fields.map(f => s"${SQLColumn.quoteIdentifier(f.name)}: ${f.dataType.sparkSql}").mkString(", ")}>"
  override def isSortable: Boolean = false
  override def fieldIndex(fieldName: String): Int = fields.indexWhere(_.name == fieldName)
  override def withOtherFields[T](other: GenericStructDataType with GenericDataType, func: (Seq[GenericField], Seq[GenericField]) => T): T =
    func(fields, other.fields)
  override def makeNullable: SQLStructDataType = copy(fields = fields.map(_.makeNullable))
  override def toLowerCase: SQLStructDataType = copy(fields = fields.map(_.toLowerCase))
  override def removeMetadata: SQLStructDataType = copy(fields = fields.map(_.removeMetadata))
}

case class SQLArrayDataType(elementDataType: SQLDataType) extends SQLDataType with GenericArrayDataType {
  override def typeName: String = "array"
  override def sql: String = s"ARRAY<${elementDataType.sql}>"
  override def sparkSql: String = s"ARRAY<${elementDataType.sparkSql}>"
  override def isSortable: Boolean = false
  override def containsNull: Boolean = true
  override def withOtherElementType[T](other: GenericArrayDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T =
    func(elementDataType, other.elementDataType)
  override def toLowerCase: SQLArrayDataType = copy(elementDataType = elementDataType.toLowerCase)
}

case class SQLMapDataType(keyDataType: SQLDataType, valueDataType: SQLDataType) extends SQLDataType with GenericMapDataType {
  override def typeName: String = "map"
  override def sql: String = s"MAP<${keyDataType.sql}, ${valueDataType.sql}>"
  override def sparkSql: String = s"MAP<${keyDataType.sparkSql}, ${valueDataType.sparkSql}>"
  override def isSortable: Boolean = false
  override def valueContainsNull: Boolean = true
  override def withOtherKeyType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T =
    func(keyDataType, other.keyDataType)
  override def withOtherValueType[T](other: GenericMapDataType with GenericDataType, func: (GenericDataType, GenericDataType) => T): T =
    func(valueDataType, other.valueDataType)
  override def toLowerCase: SQLMapDataType = copy(keyDataType = keyDataType.toLowerCase, valueDataType = valueDataType.toLowerCase)
}

object SQLDataType {

  /**
   * Quote an identifier for the default SQLGlot dialect, as used in data types
   */
  private[sql] def quoteIdentifier(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  private[sql] val integerTypes = Seq("TINYINT", "SMALLINT", "INT", "BIGINT") // ordered by width
  private val integerDigits = Map("TINYINT" -> 3, "SMALLINT" -> 5, "INT" -> 10, "BIGINT" -> 19)
  val stringTypes: Set[String] = Set("CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "TEXT")
  private val maxDecimalPrecision = 38

  /**
   * The wider type of two simple types, to which both can be cast without loss, or None if there is none.
   * Integer types are widened to the larger integer type, integer and decimal types to a decimal type with enough
   * digits, numeric types with FLOAT or DOUBLE to DOUBLE, string types to the longer or unbounded string type,
   * and DATE with TIMESTAMP to TIMESTAMP.
   */
  def wider(left: SQLSimpleDataType, right: SQLSimpleDataType): Option[SQLSimpleDataType] = {
    def decimal(t: SQLSimpleDataType): Option[(Int, Int)] = t.baseType match {
      case "DECIMAL" => t.getDecimalSpec.orElse(Some((maxDecimalPrecision, 0))) // DECIMAL without precision is not bounded here
      case i if integerDigits.contains(i) => Some((integerDigits(i), 0))
      case _ => None
    }
    (left.baseType, right.baseType) match {
      case _ if left.isSameType(right) => Some(left)
      case (l, r) if integerTypes.contains(l) && integerTypes.contains(r) =>
        Some(if (integerTypes.indexOf(l) >= integerTypes.indexOf(r)) left else right)
      case (l, r) if (l == "DECIMAL" || r == "DECIMAL") && decimal(left).isDefined && decimal(right).isDefined =>
        val ((p1, s1), (p2, s2)) = (decimal(left).get, decimal(right).get)
        val scale = math.max(s1, s2)
        val precision = math.min(maxDecimalPrecision, math.max(p1 - s1, p2 - s2) + scale)
        Some(SQLSimpleDataType(s"DECIMAL($precision, $scale)"))
      case (l, r) if (impreciseNumericTypes.contains(l) || impreciseNumericTypes.contains(r)) && left.isNumeric && right.isNumeric =>
        Some(SQLSimpleDataType("DOUBLE"))
      case (l, r) if stringTypes.contains(l) && stringTypes.contains(r) =>
        (left.parameters.headOption, right.parameters.headOption) match {
          case (Some(n1), Some(n2)) if l == r => Some(if (n1 >= n2) left else right)
          case (None, _) if l != "CHAR" && l != "NCHAR" => Some(left)
          case (_, None) if r != "CHAR" && r != "NCHAR" => Some(right)
          case _ => Some(SQLSimpleDataType("TEXT"))
        }
      case ("DATE", "TIMESTAMP") => Some(right)
      case ("TIMESTAMP", "DATE") => Some(left)
      case _ => None
    }
  }

  private[sql] val numericTypes = Set("TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL", "FLOAT", "DOUBLE", "UTINYINT",
    "USMALLINT", "UINT", "UBIGINT", "INT128", "INT256", "MONEY", "SMALLMONEY")
  private[sql] val impreciseNumericTypes = Set("FLOAT", "DOUBLE")

  // Spark type names, as used in SDLB configurations, and their SQLGlot equivalent
  private val sparkTypeNames = Map(
    "string" -> "TEXT", "integer" -> "INT", "int" -> "INT", "long" -> "BIGINT", "short" -> "SMALLINT", "byte" -> "TINYINT",
    "boolean" -> "BOOLEAN", "double" -> "DOUBLE", "float" -> "FLOAT", "date" -> "DATE", "timestamp" -> "TIMESTAMP",
    "binary" -> "VARBINARY", "decimal" -> "DECIMAL", "timestamp_ntz" -> "TIMESTAMP"
  )

  /**
   * The Spark type name of a simple type, if there is an equivalent, e.g. `integer` for INT or `string` for VARCHAR(20).
   * TIMESTAMP (without time zone) is translated to `timestamp`, as Spark reads it as such from a database with JDBC.
   */
  def sparkTypeName(tpe: SQLSimpleDataType): Option[String] = tpe.baseType match {
    case "TINYINT" => Some("byte")
    case "SMALLINT" => Some("short")
    case "INT" => Some("integer")
    case "BIGINT" => Some("long")
    case "DECIMAL" =>
      val (precision, scale) = tpe.getDecimalSpec.getOrElse((maxDecimalPrecision, 18))
      Some(s"decimal($precision,$scale)")
    case "FLOAT" => Some("float")
    case "DOUBLE" => Some("double")
    case t if stringTypes.contains(t) || t == "UUID" || t == "JSON" => Some("string")
    case "BOOLEAN" => Some("boolean")
    case "DATE" => Some("date")
    case "TIMESTAMP" | "TIMESTAMPTZ" | "TIMESTAMPLTZ" => Some("timestamp")
    case "TIMESTAMPNTZ" => Some("timestamp_ntz")
    case "BINARY" | "VARBINARY" | "BLOB" => Some("binary")
    case _ => None
  }

  def of(dataType: GenericDataType): SQLDataType = dataType match {
    case d: SQLDataType => d
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
  }

  /**
   * Create a simple type from its name. Spark type names like `string` or `long` are translated to their
   * SQLGlot equivalent, other names are taken as SQL type in the default SQLGlot dialect.
   */
  def simple(tpe: String): SQLSimpleDataType = {
    val trimmed = tpe.trim
    val base = trimmed.takeWhile(_ != '(').trim.toLowerCase
    // type parameters are formatted like SQLGlot does, e.g. DECIMAL(10, 2)
    val params = trimmed.dropWhile(_ != '(').replaceAll("\\s*,\\s*", ", ").replaceAll("\\(\\s+", "(").replaceAll("\\s+\\)", ")")
    SQLSimpleDataType(sparkTypeNames.getOrElse(base, base.toUpperCase) + params)
  }

  def fromBridge(json: JValue): SQLDataType = {
    implicit val formats: Formats = DefaultFormats
    json match {
      case j: JObject if (j \ "struct") != JNothing =>
        SQLStructDataType((j \ "struct").children.map(f => SQLField((f \ "name").extract[String], fromBridge(f \ "type"))))
      case j: JObject if (j \ "array") != JNothing => SQLArrayDataType(fromBridge(j \ "array"))
      case j: JObject if (j \ "map") != JNothing => (j \ "map").children match {
        case Seq(key, value) => SQLMapDataType(fromBridge(key), fromBridge(value))
        case x => throw new IllegalArgumentException(s"Unexpected map type from SQLGlot: $x")
      }
      case j => SQLSimpleDataType((j \ "type").extract[String])
    }
  }
}
