package org.apache.spark.sql.avro.confluent

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.avro.confluent.SubjectType.SubjectType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * Wrapper for schema registry client.
 * It supports advanced logic for schema compatibility check and update.
 * It provides Spark SQL functions from/to_confluent_avro for decoding/encoding confluent avro messages.
 */
class ConfluentClient(schemaRegistryUrl: String) extends Logging with Serializable {

  @transient lazy val sr: SchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000)
  @transient private lazy val subjects = mutable.Set(sr.getAllSubjects.asScala.toSeq: _*)
  logInfo(s"Initialize confluent schema registry with url $schemaRegistryUrl")

  val CONFLUENT_MAGIC_BYTE = 0x0 // magic byte needed as first byte of confluent binary kafka message

  /**
   * get the subject name for a topic (confluent standard is postfixing '-key' or '-value')
   */
  def getSubject(topic: String, subjectType: SubjectType): String = subjectType match {
    case SubjectType.key => s"${topic}-key"
    case SubjectType.value => s"${topic}-value"
  }

  /**
   * test connection by calling a simple function
   */
  def test(): Unit = sr.getMode

  /**
   * If schema already exists, update if compatible, otherwise create schema
   * Throws exception if new schema is not compatible.
   * @return confluent schemaId and registered schema
   */
  def setOrUpdateSchema(subject: String, newSchema: Schema): (Int, Schema) = {
    if (!schemaExists(subject)) return registerSchema(subject, newSchema)
    val latestSchema = getLatestSchemaFromConfluent(subject)
    if (!latestSchema._2.equals(newSchema)) {
      if (checkSchemaEvolutionAllowed(latestSchema._2, newSchema)) {
        logInfo(s"New schema for subject $subject is compatible with latest schema: new=$newSchema diffs=${debugSchemaDiff(newSchema, latestSchema._2)}")
        registerSchema(subject, newSchema)
      } else {
        val msg = s"New schema for subject $subject is not compatible with latest schema"
        logError(s"$msg: latest=${latestSchema._2} new=$newSchema diffs=${debugSchemaDiff(newSchema, latestSchema._2)}")
        throw new SchemaIncompatibleException(msg)
      }
    } else {
      logDebug(s"New schema for $subject is equal to latest schema")
      latestSchema
    }
  }

  /**
   * If schema already exists, check if compatible, otherwise create schema
   * Throws exception if new schema is not compatible.
   * @return confluent schemaId and registered schema
   */
  def setOrCheckSchema(subject: String, newSchema: Schema): (Int, Schema) = {
    if (!schemaExists(subject)) return registerSchema(subject, newSchema)
    val latestSchema = getLatestSchemaFromConfluent(subject)
    if (!latestSchema._2.equals(newSchema)) {
      if (checkSchemaMutualReadable(latestSchema._2, newSchema)) {
        logInfo(s"New schema for subject $subject is compatible with latest schema: new=$newSchema diffs=${debugSchemaDiff(newSchema, latestSchema._2)}")
        latestSchema
      } else {
        val msg = s"New schema for subject $subject is not compatible with latest schema"
        logError(s"$msg: latest=${latestSchema._2} new=$newSchema diffs=${debugSchemaDiff(newSchema, latestSchema._2)}")
        throw new SchemaIncompatibleException(msg)
      }
    } else {
      logDebug(s"New schema for subject $subject is equal to latest schema")
      latestSchema
    }
  }

  def getLatestSchemaFromConfluent(subject:String): (Int,Schema) = {
    val m = sr.getLatestSchemaMetadata(subject)
    val schemaId = m.getId
    val schemaString = m.getSchema
    val parser = new Schema.Parser()
    val avroSchema = parser.parse(schemaString)
    // return
    (schemaId,avroSchema)
  }

  def getSchemaFromConfluent(id:Int): (Int,Schema) = (id,sr.getById(id))

  /**
   * Converts a binary column of confluent avro format into its corresponding catalyst value according to the latest
   * schema stored in schema registry.
   * Note: copied from org.apache.spark.sql.avro.*
   *
   * @param data the binary column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   */
  def from_confluent_avro(data: Column, topic: String, subjectType: SubjectType): Column = {
    val subject = getSubject(topic, subjectType)
    new Column(ConfluentAvroDataToCatalyst(data.expr, subject, this))
  }

  /**
   * Converts a column into binary of confluent avro format according to the latest schema stored in schema registry.
   * Note: copied from org.apache.spark.sql.avro.*
   *
   * @param data the data column.
   * @param topic the topic name.
   * @param subjectType the subject type (key or value).
   * @param updateAllowed if subject schema should be updated if compatible
   * @param eagerCheck if true tiggers instantiation of converter object instances
   */
  def to_confluent_avro(data: Column, topic: String, subjectType: SubjectType, updateAllowed: Boolean = false, eagerCheck: Boolean = false): Column = {
    val subject = getSubject(topic, subjectType)
    val converterExpr = CatalystDataToConfluentAvro(data.expr, subject, this, updateAllowed )
    if (eagerCheck) converterExpr.test()
    new Column(converterExpr)
  }

  private def registerSchema(subject:String, schema:Schema): (Int,Schema) = {
    logInfo(s"Register new schema for $subject: schema=$schema")
    val schemaId = sr.register(subject, schema)
    subjects.add(subject)
    // return
    (schemaId,schema)
  }

  private def schemaExists(subject:String): Boolean = subjects.contains(subject)

  // check if the new schema is compatible with the latest schema
  // means old data can be read with the new schema
  // this is needed if we want to update the schema in the schema registry
  private def checkSchemaCompatible(subject:String, newSchema:Schema): Boolean = {
    if (!schemaExists(subject)) throw new SubjectNotExistingException("Subject $subject not found!")
    sr.testCompatibility(subject, newSchema)
  }

  // check if the data produced by the latest schema can be read by the new schema
  // this is needed to do schema evolution, as readers still need to read messages with latest schema.
  private def checkSchemaEvolutionAllowed(latestSchema:Schema, newSchema:Schema): Boolean = {
    val validatorRead = new SchemaValidatorBuilder().canReadStrategy.validateLatest
    Try{validatorRead.validate(newSchema, Seq(latestSchema).asJava)}.isSuccess
  }

  // check if the data produced by the new schema can be read by the latest schema and vice versa
  // this is needed as check if schema should not be updated.
  private def checkSchemaMutualReadable(latestSchema:Schema, newSchema:Schema): Boolean = {
    val validatorRead = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest()
    Try{validatorRead.validate(newSchema, Seq(latestSchema).asJava)}.isSuccess
  }

  private def debugTypeIsCompatible(type1: Schema.Type, type2: Schema.Type) = (type1, type2) match {
    case (Schema.Type.STRING, Schema.Type.ENUM) => true
    case (Schema.Type.ENUM, Schema.Type.STRING) => true
    case _ => type1 == type2
  }

  private def debugSchemaDiff(schema1:Schema, schema2:Schema): Seq[String] = {
    if (schema1.getName!=schema2.getName) Seq(s"schema names don't match (${schema1.getName}, ${schema2.getName})")
    if (!debugTypeIsCompatible(schema1.getType, schema2.getType)) Seq(s"(${schema1.getName}) schema types don't match (${schema1.getType}, ${schema2.getType})")
    else {
      if (schema1.getType==Schema.Type.RECORD) {
        val diffs = mutable.Buffer[String]()
        val fields1 = schema1.getFields.asScala.map( f => (f.name,f.schema)).toMap
        val fields2 = schema2.getFields.asScala.map( f => (f.name,f.schema)).toMap
        val f1m2 = fields1.keys.toSeq.diff(fields2.keys.toSeq)
        if (f1m2.nonEmpty) diffs += s"""fields ${f1m2.mkString(", ")} missing in schema2"""
        val f2m1 = fields2.keys.toSeq.diff(fields1.keys.toSeq)
        if (f2m1.nonEmpty) diffs += s"""fields ${f2m1.mkString(", ")} missing in schema1"""
        diffs ++= fields1.keys.toSeq.intersect(fields2.keys.toSeq).flatMap( f => debugSchemaDiff(debugSchemaReduceUnion(fields1(f)),debugSchemaReduceUnion(fields2(f))))
        diffs
      } else Seq()
    }
  }

  private def debugSchemaReduceUnion(schema:Schema): Schema = {
    if (schema.getType==Schema.Type.UNION) {
      val filteredSubSchemas = schema.getTypes.asScala.filter( _.getType!=Schema.Type.NULL )
      if (filteredSubSchemas.size==1) filteredSubSchemas.head
      else schema
    } else schema
  }

}

object SubjectType extends Enumeration {
  type SubjectType = Value
  val key, value = Value
}

class SubjectNotExistingException(msg:String) extends Exception(msg)

class SchemaIncompatibleException(msg:String) extends Exception(msg)
