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
package io.smartdatalake.testutils

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.testutils.plainScala.ScalaTestUtil.registerDataObject
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.{GenericField, GenericSchema}
import io.smartdatalake.workflow.dataobject.DataObjectMetadata
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion, ExecutionPhase}

/**
 * Parameters for creating the table DataObject under test, see [[CatalogMetadataBehaviour]].
 *
 * The factory of the test suite must name the table `tableName` and pass all attributes on to Table and
 * DataObjectMetadata, see [[CatalogMetadataTestParams.createTable]] and
 * [[CatalogMetadataTestParams.dataObjectMetadata]]. Everything else, e.g. catalog, db, path or connection,
 * is defined by the factory.
 */
case class CatalogMetadataTestParams(
    tableName: String,
    description: Option[String] = None,
    primaryKey: Option[Seq[String]] = None,
    createAndReplacePrimaryKey: Boolean = false,
    foreignKeys: Option[Seq[ForeignKey]] = None,
    createAndReplaceForeignKeys: Boolean = false
) {
  def createTable(catalog: Option[String] = None, db: Option[String] = None): Table = Table(
    catalog = catalog, db = db, name = tableName, primaryKey = primaryKey,
    createAndReplacePrimaryKey = createAndReplacePrimaryKey, foreignKeys = foreignKeys,
    createAndReplaceForeignKeys = createAndReplaceForeignKeys
  )

  def dataObjectMetadata: Option[DataObjectMetadata] = description.map(d => DataObjectMetadata(description = Some(d)))
}

/**
 * Engine-agnostic tests for managing tables in the catalog at deployment time, see issue #1129:
 * creating missing tables, evolving their schema, and creating primary and foreign keys.
 *
 * Note that none of this is applied during a normal SDLB run. It is applied by DataObjectSchemaExporter,
 * which uses [[CatalogMetadataApplier]] with the schemas exported by a dry-run. The behaviours therefore
 * work on the applier and not on Actions, and each of them checks that a second "apply" changes nothing.
 *
 * Instantiated per engine and DataObject implementation, parameterized by DataObject factories.
 */
trait CatalogMetadataBehaviour extends GenericTestTool {
  this: SmartDataLakeLogger =>

  def defaultEngineConnection: Connection with EngineConnection

  /** factory for the table DataObject under test */
  type CatalogMetadataDataObjectFactory = (String, CatalogMetadataTestParams, InstanceRegistry) =>
    TransactionalTableDataObject with CanHandleTableSchema

  /** factory for the table DataObject under test, for behaviours needing primary key constraints */
  type ConstraintsDataObjectFactory = (String, CatalogMetadataTestParams, InstanceRegistry) =>
    TransactionalTableDataObject with CanHandleTableSchema with CanHandleConstraints

  /** factory for the table DataObject under test, for behaviours needing foreign key constraints */
  type ForeignKeysDataObjectFactory = (String, CatalogMetadataTestParams, InstanceRegistry) =>
    TransactionalTableDataObject with CanHandleTableSchema with CanHandleForeignKeys

  private def setupRegistryAndContext(): (InstanceRegistry, ActionPipelineContext) = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(defaultEngineConnection)
    // the catalog is changed in exec phase, like DataObjectSchemaExporter does it
    (instanceRegistry, ScalaTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec))
  }

  /**
   * Create the DataObject and make sure its table doesn't exist, so that the behaviour starts from scratch.
   */
  private def createDataObjectWithoutTable[A <: TransactionalTableDataObject](dataObject: A)
                                                                            (implicit registry: InstanceRegistry, context: ActionPipelineContext): A = {
    val registeredDataObject = registerDataObject(dataObject) // this also drops an existing table
    assert(!registeredDataObject.isTableExisting, s"(${dataObject.id}) table ${dataObject.table.fullName} should not exist at the start of the test")
    registeredDataObject
  }

  private def companionOf(dataObject: TransactionalTableDataObject): DataFrameSubFeedCompanion =
    DataFrameSubFeed.getCompanion(dataObject.writeSubFeedSupportedTypes.head)

  private def field(name: String, dataType: String, nullable: Boolean = true, comment: Option[String] = None)
                   (implicit companion: DataFrameSubFeedCompanion): GenericField =
    companion.createField(name, companion.createSimpleDataType(dataType), nullable, comment)

  private def schemaOf(fields: GenericField*)(implicit companion: DataFrameSubFeedCompanion): GenericSchema =
    companion.createSchema(fields)

  /** column names of the catalog are compared case-insensitively, as every database has its own convention */
  private def normalizedColumns(schema: GenericSchema): Seq[String] = schema.columns.map(_.toLowerCase)

  private def nullableOf(schema: GenericSchema, column: String): Boolean =
    schema.fields.find(_.name.equalsIgnoreCase(column))
      .getOrElse(throw new AssertionError(s"column $column not found in ${schema.columns.mkString(", ")}")).nullable

  private def assertNothingLeftToApply(applier: CatalogMetadataApplier, dataObject: TransactionalTableDataObject)
                                      (implicit context: ActionPipelineContext): Unit = {
    val changes = applier.plan(dataObject)
    assert(changes.exists(_.isEmpty), s"(${dataObject.id}) expected nothing left to apply, got ${changes.map(_.describe)}")
  }

  /**
   * A table which doesn't exist yet is created with the exported schema, including the table and column comments.
   */
  def testCreateMissingTable(createDataObject: CatalogMetadataDataObjectFactory): Unit = {
    val (instanceRegistry, actionPipelineContext) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = actionPipelineContext

    val params = CatalogMetadataTestParams("catalogmeta_create", description = Some("Cities of interest"))
    val dataObject = createDataObjectWithoutTable(createDataObject("createDO", params, instanceRegistry))
    implicit val companion: DataFrameSubFeedCompanion = companionOf(dataObject)
    val exportedSchema = schemaOf(
      field("city", "string", comment = Some("Name of the city")),
      field("nr", "integer")
    )
    val applier = new CatalogMetadataApplier(_ => Some(exportedSchema))

    // plan reports that the table is created
    val changes = applier.plan(dataObject)
    assert(changes.exists(_.createTable.isDefined), s"expected the table to be created, got ${changes.map(_.describe)}")

    // apply creates an empty table with the exported schema
    applier.applyTableChanges(dataObject, changes.get)
    assert(dataObject.isTableExisting)
    assert(dataObject.getCurrentSchema.map(normalizedColumns).contains(Seq("city", "nr")))
    assert(dataObject.getDataFrame().isEmpty)

    // ... and its comments
    dataObject match {
      case metadataDataObject: CanHandleCatalogMetadata =>
        assert(metadataDataObject.getTableComment.contains("Cities of interest"))
        val columnComments = metadataDataObject.getColumnComments.map { case (path, comment) => path.map(_.toLowerCase) -> comment }
        assert(columnComments == Map(Seq("city") -> "Name of the city"))
      case _ => ()
    }

    assertNothingLeftToApply(applier, dataObject)
  }

  /**
   * The schema of an existing table is evolved to the exported schema: new columns are added, data types are
   * changed, and columns which are not written anymore are made nullable instead of being dropped.
   *
   * @param testChangeDataType set to false for engines not supporting to change the data type of a column
   */
  def testEvolveSchema(createDataObject: CatalogMetadataDataObjectFactory, testChangeDataType: Boolean = true): Unit = {
    val (instanceRegistry, actionPipelineContext) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = actionPipelineContext

    val params = CatalogMetadataTestParams("catalogmeta_evolution")
    val dataObject = createDataObjectWithoutTable(createDataObject("evolutionDO", params, instanceRegistry))
    implicit val companion: DataFrameSubFeedCompanion = companionOf(dataObject)

    // create the table with the initial schema
    val initialSchema = schemaOf(
      field("id", "integer"),
      field("rating", "integer"),
      field("obsolete", "string", nullable = false)
    )
    val initialApplier = new CatalogMetadataApplier(_ => Some(initialSchema))
    initialApplier.applyTableChanges(dataObject, initialApplier.plan(dataObject).get)
    assert(dataObject.isTableExisting)
    // not every engine keeps the not null of the created table, e.g. Delta Lake makes all columns of a table
    // created from a DataFrame nullable. There is nothing to change for "obsolete" then.
    val obsoleteIsNotNull = !nullableOf(dataObject.getCurrentSchema.get, "obsolete")

    // the new schema adds a column, changes a data type and doesn't write "obsolete" anymore
    val newRatingType = if (testChangeDataType) "long" else "integer"
    val newSchema = schemaOf(
      field("id", "integer"),
      field("rating", newRatingType),
      field("city", "string", comment = Some("City of the customer"))
    )
    val applier = new CatalogMetadataApplier(_ => Some(newSchema))
    val changes = applier.plan(dataObject).get
    assert(changes.createTable.isEmpty)
    assert(changes.schemaChanges.collect { case c: AddColumn => c.columnName.toLowerCase } == Seq("city"), changes.describe.mkString(", "))
    assert(changes.schemaChanges.collect { case c: ChangeColumnType => c.columnName.toLowerCase } ==
      (if (testChangeDataType) Seq("rating") else Seq()), changes.describe.mkString(", "))
    // columns which are not written anymore are made nullable, they are never dropped
    assert(changes.schemaChanges.collect { case c: ChangeColumnNullable => (c.columnName.toLowerCase, c.nullable) } ==
      (if (obsoleteIsNotNull) Seq(("obsolete", true)) else Seq()), changes.describe.mkString(", "))

    applier.applyTableChanges(dataObject, changes)
    val schemaAfterApply = dataObject.getCurrentSchema.get
    assert(normalizedColumns(schemaAfterApply).sorted == Seq("city", "id", "obsolete", "rating"))
    assert(nullableOf(schemaAfterApply, "obsolete"))
    assert(schemaAfterApply.fields.find(_.name.equalsIgnoreCase("rating")).get.dataType
      .isSameType(companion.createSimpleDataType(newRatingType)))

    assertNothingLeftToApply(applier, dataObject)
  }

  /**
   * The primary key is created if table.createAndReplacePrimaryKey is set, and its columns are made not null.
   */
  def testCreatePrimaryKey(createDataObject: ConstraintsDataObjectFactory): Unit = {
    val (instanceRegistry, actionPipelineContext) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = actionPipelineContext

    val params = CatalogMetadataTestParams("catalogmeta_pk", primaryKey = Some(Seq("id")), createAndReplacePrimaryKey = true)
    val dataObject = createDataObjectWithoutTable(createDataObject("primaryKeyDO", params, instanceRegistry))
    implicit val companion: DataFrameSubFeedCompanion = companionOf(dataObject)
    // note that the exported schema has a nullable primary key column, as SDLB doesn't know about the constraint
    val exportedSchema = schemaOf(field("id", "integer"), field("name", "string"))
    val applier = new CatalogMetadataApplier(_ => Some(exportedSchema))

    val changes = applier.plan(dataObject).get
    assert(changes.primaryKey.contains(Seq("id")))
    assert(changes.schemaChanges.collect { case c: ChangeColumnNullable => (c.columnName.toLowerCase, c.nullable) } == Seq(("id", false)),
      changes.describe.mkString(", "))

    applier.applyTableChanges(dataObject, changes)
    assert(!nullableOf(dataObject.getCurrentSchema.get, "id"))
    val existingPrimaryKey = dataObject.getExistingPKConstraint(dataObject.table.catalog, dataObject.table.db, dataObject.table.name)
    assert(existingPrimaryKey.exists(_.pkColumns.map(_.toLowerCase) == Seq("id")))

    assertNothingLeftToApply(applier, dataObject)
  }

  /**
   * The foreign keys are created in a second phase, after all tables have been created with their primary keys.
   * Note that the referenced table is created *after* the table referencing it, to show that it is the phase
   * and not the order of the DataObjects which makes this work.
   */
  def testCreateForeignKeys(createDataObject: ForeignKeysDataObjectFactory): Unit = {
    val (instanceRegistry, actionPipelineContext) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = actionPipelineContext

    val customerParams = CatalogMetadataTestParams("catalogmeta_fk_customer",
      primaryKey = Some(Seq("id")), createAndReplacePrimaryKey = true)
    val orderParams = CatalogMetadataTestParams("catalogmeta_fk_order",
      primaryKey = Some(Seq("id")), createAndReplacePrimaryKey = true,
      foreignKeys = Some(Seq(ForeignKey(None, customerParams.tableName, Map("customer_id" -> "id"), Some("catalogmeta_fk")))),
      createAndReplaceForeignKeys = true)
    val orderDataObject = createDataObjectWithoutTable(createDataObject("orderDO", orderParams, instanceRegistry))
    val customerDataObject = createDataObjectWithoutTable(createDataObject("customerDO", customerParams, instanceRegistry))
    implicit val companion: DataFrameSubFeedCompanion = companionOf(orderDataObject)

    val exportedSchemas = Map(
      customerDataObject.id -> schemaOf(field("id", "integer"), field("name", "string")),
      orderDataObject.id -> schemaOf(field("id", "integer"), field("customer_id", "integer"))
    )
    val applier = new CatalogMetadataApplier(exportedSchemas.get)

    // the foreign key is planned even though none of the tables exists yet
    val plans = Seq(orderDataObject, customerDataObject).map(dataObject => (dataObject, applier.plan(dataObject).get))
    assert(plans.head._2.foreignKeys.map(_.name) == Seq(Some("catalogmeta_fk")), plans.head._2.describe.mkString(", "))

    // phase 1 creates the tables including their primary keys, phase 2 creates the foreign key
    plans.foreach { case (dataObject, changes) => applier.applyTableChanges(dataObject, changes) }
    plans.foreach { case (dataObject, changes) => applier.applyForeignKeys(dataObject, changes) }

    val existingForeignKeys = orderDataObject.getExistingForeignKeys
    val definedForeignKey = orderDataObject.getDefinedForeignKeys.head
    assert(existingForeignKeys.exists(_.isEqualTo(definedForeignKey)),
      s"expected ${definedForeignKey.describe}, got ${existingForeignKeys.map(_.describe)}")

    assertNothingLeftToApply(applier, orderDataObject)
  }

  /**
   * Foreign keys are metadata for the data catalog only if table.createAndReplaceForeignKeys is not set.
   */
  def testForeignKeysNotCreatedIfNotEnabled(createDataObject: ForeignKeysDataObjectFactory): Unit = {
    val (instanceRegistry, actionPipelineContext) = setupRegistryAndContext()
    implicit val registry: InstanceRegistry = instanceRegistry
    implicit val context: ActionPipelineContext = actionPipelineContext

    val params = CatalogMetadataTestParams("catalogmeta_fk_disabled",
      foreignKeys = Some(Seq(ForeignKey(None, "catalogmeta_fk_customer", Map("customer_id" -> "id"), None))))
    val dataObject = createDataObjectWithoutTable(createDataObject("foreignKeyDisabledDO", params, instanceRegistry))
    implicit val companion: DataFrameSubFeedCompanion = companionOf(dataObject)
    val applier = new CatalogMetadataApplier(_ => Some(schemaOf(field("id", "integer"), field("customer_id", "integer"))))

    val changes = applier.plan(dataObject).get
    assert(changes.foreignKeys.isEmpty, changes.describe.mkString(", "))
  }
}
