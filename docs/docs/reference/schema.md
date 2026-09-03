---
id: schema
title: Schema
---

Smart Data Lake Builder relies on DataFrame schema to verify proper transitions from one DataObject to another. 

Depending on the DataObject the schema is provided directly with the data source. Alternatively, the schema can be specified in the configuration or inferred from the data.
For productive workloads it is not recommended to use schema inference, but the schema of DataObjects that are at the start of a data pipeline should be specified for performance and stability reasons.

Furthermore, depending on the DataObject type, schema evolution is supported, see below.

## Schema inference
For most DataObjects the Schema can be inferred, e.g. by sampling items in an XML stream.
Nevertheless, best practice especially for production cases are fixed schemata.  

## schemaMin
To assert that a defined list of columns is always present in the schema of a specific DataObject, use its `schemaMin` attribute to define a minimal schema. The minimal schema is validated on read and write with Spark.

## Specifying Schema
To specify the schema many DataObjects support the `schema` attribute (e.g. all children of SparkFileDataObject) for reading and writing with Spark.
The `schema` attribute allows to define the schema the DataObject tries to read data with, and can be used to avoid schema inference with Spark DataSources.
On write the DataFrame to be written must match the defined `schema` exactly (nullability and column order are ignored).

The schema can be defined by using one of the schema providers below, default is `ddl`.
The schema provider and its configuration value must be provided in the format `<provider>#<value>`.

Schema Providers available are:

| Provider      | Description                                                 | Example              |
|---------------|-------------------------------------------------------------|----------------------|
| `ddl`         | create the schema from a Spark ddl string                   | `ddl#a string, b array<struct<b1: string, b2: long>>, c struct<c1: string, c2: long>` |
| `ddlFile`     | read a Spark ddl definition from a file and create a schema | `ddlFile#abc/xyz.ddl` |
| `caseClass`   | convert a Scala Case Class to a schema using Spark encoders | `caseClass#com.sample.XyzClass` |
| `javaBean`    | convert a Java Bean to a schema using Spark encoders        | `javaBean#com.sample.XyzClass` |
| `xsdFile`       | read an Xml Schema Definition file and create a schema    | `xsdFile#abc/xyz.xsd` |
| `jsonSchemaFile` | read a Json Schema file and create a schema              | `jsonSchemaFile#abc/xyz.json` | 
| `avroSchemaFile` | read an Avro Schema file and create a schema             | `avroSchemaFile#abc/xyz.avsc` |

Customize `xsdFile` provider behaviour: `xsdFile#<path-to-xsd-file>;<row-tag>;<maxRecursion:Int>;<jsonCompatibility:Boolean>`
- `<row-tag>`: configure the path of the element to extract from the xsd schema. Leave empty to extract the root.
- `<maxRecursion>`: if xsd schema is recursive, this configures the number of levels to create in the schema.
  Default is 10 levels.
- `<jsonCompatibility>`: In XML array elements are modeled with their own tag named with singular name.
  In JSON an array attribute has unnamed array entries, but the array attribute has a plural name.
  If true, the singular name of the array element in the XSD is converted to a plural name by adding an 's'
  in order to read corresponding json files.
  Default is false.

Customize `jsonSchemaFile` provider behaviour: `jsonSchemaFile#<path-to-json-file>;<row-tag>;<strictTyping:Boolean>;<additionalPropertiesDefault:Boolean>`
- `<row-tag>`: configure the path of the element to extract from the json schema. Leave empty to extract the root.
- `<strictTyping>`: if true union types (oneOf) are merged if rational, otherwise they are simply mapped to StringType;
  additional properties are ignored, otherwise the corresponding schema object is mapped to MapType(String,String).
  Default is strictTyping=false.
- `<additionalPropertiesDefault>`: Set to true or false. This is used as default value for 'additionalProperties'-field if it is missing in a schema with type='object'.
  Default value is additionalPropertiesDefault=true, as this conforms with the specification.

Customize `avroSchemaFile` provider behaviour: `avroSchemaFile#<path-to-avsc-file>;<row-tag>`
- `<row-tag>`: configure the path of the element to extract from the avro schema. Leave empty to extract the root.

## Column descriptions from ScalaDoc

If a schema is derived from a Scala case class, the ScalaDoc of that case class is used to document its columns.
The `@param` tags become the comments of the corresponding columns, and are shown in the SDLB UI.
Nested case classes and arrays of case classes are documented as well.

This works in three places:

* The `caseClass` schema provider, e.g. `schema = "caseClass#com.sample.GeoLocation"`.
* The return value of a **user defined function**. If a UDF returns a case class, its attributes become
  (nested) columns of the resulting DataFrame and are documented automatically. This applies to UDFs registered
  in `sparkUDFs` of the Spark connection as well as UDFs created inside a custom transformer.
* A **transformation method declaring a typed Dataset return value**, e.g.
  `def transform(ds: Dataset[GeoLocation]): Dataset[EnrichedLocation]`. `Map[String, Dataset[<CaseClass>]]` is
  supported as well, for transformers with multiple outputs.

For the following case class

```scala
/**
 * A geo location enriched from an address.
 *
 * @param lat  Latitude in decimal degrees, WGS84.
 * @param lon  Longitude in decimal degrees, WGS84.
 */
case class GeoLocation(lat: Double, lon: Double)
```

and a transformer using it

```scala
val geoUdf = udf((address: String) => geocode(address))
df.withColumn("geo", geoUdf($"address"))
```

the columns `geo.lat` and `geo.lon` get the descriptions from the ScalaDoc above.
A comment that is already defined, e.g. through `schemaMin` or a `@column` entry in a markdown description file,
is never overwritten.

Note that the case class has to be visible in the **signature** of the transformation method. A method declared
as returning a `DataFrame` that converts a Dataset internally, e.g. `myDataset.toDF()`, loses the type, and
`df.as[MyCaseClass]` leaves no trace either - it only changes the encoder of the Dataset. Declare the return type
as `Dataset[MyCaseClass]` to get the columns documented.

:::caution Compiler plugin required
The ScalaDoc of a case class is only available at runtime if it is compiled with the
`com.github.takezoe:runtime-scaladoc-reader` compiler plugin, which stores it as an annotation.
Use sdl-parent as maven "parent pom", or add the plugin to your project as follows, otherwise the comments stay empty:

```xml
<plugin>
    <groupId>net.alchim31.maven</groupId>
    <artifactId>scala-maven-plugin</artifactId>
    <configuration>
        <compilerPlugins>
            <compilerPlugin>
                <groupId>com.github.takezoe</groupId>
                <artifactId>runtime-scaladoc-reader_2.13</artifactId>
                <version>1.0.3</version>
            </compilerPlugin>
        </compilerPlugins>
    </configuration>
</plugin>
```
:::

Note that column comments are not written to the catalog during a normal SDLB run.
Table metadata can only change when the configuration or the code changes, so it is applied at
deployment time instead - see [Managing tables in the catalog at deploy time](#managing-tables-in-the-catalog-at-deploy-time).

Limitations:
* Only UDFs created with the typed API, e.g. `udf((x: String) => MyCaseClass(x))`, carry the type information
  needed to find the case class. A UDF declaring its return type explicitly does not.
* Python UDFs are not supported.
* The Spark Connect engine is not supported.

## Managing tables in the catalog at deploy time

The tables of a data pipeline - their schema, the table comment from `metadata.description`, the column
comments and the primary and foreign keys - are *not* created or updated in the catalog during a normal SDLB
run. They can only change when the configuration or the code changes, so writing them on every run causes
unnecessary load on the catalog and races with concurrent write operations. They are managed at deployment
time in two steps instead.

**1. Export the schemas on the development environment.** A dry-run with schema export writes the schema of
every output DataObject, including the column comments SDLB assembled from `schemaMin`, from the Markdown
description files and from the ScalaDoc of case classes, to `global.dataObjectsSchemaSource`:

```bash
sdlb --config config/ --feed-sel '.*' --test dry-run-with-schema-export
```

This is a dry-run: it executes the prepare- and init-phase only and does not write any data. Because the
schemas come from the init-phase and not from the catalog, this works even if the tables do not exist yet.
Commit the resulting schema files together with the configuration.

```hocon
global {
  dataObjectsSchemaSource = "file:./schema"
}
```

**2. Apply the changes on the target environment.** `DataObjectSchemaExporter` reads the desired state from
the configuration and from the exported schema files, compares it with the catalog and writes only what
differs:

```bash
# report what would change, without changing anything
java -cp sdlb.jar io.smartdatalake.meta.configexporter.DataObjectSchemaExporter \
  --config config/ --mode plan --descriptionPath ./description

# apply the changes
java -cp sdlb.jar io.smartdatalake.meta.configexporter.DataObjectSchemaExporter \
  --config config/ --mode apply --descriptionPath ./description
```

Applying is idempotent: running it twice makes no second change. Column descriptions defined with `@column`
in the Markdown description files override the comments from the exported schema.

The following changes are applied:

| Change | Applied when |
| --- | --- |
| create a missing table | the DataObject implements `CanHandleTableSchema` and a schema was exported for it |
| add a new column, change a data type | the DataObject implements `CanHandleTableSchema` |
| make a column nullable which is not written anymore | the DataObject implements `CanHandleTableSchema` |
| table comment, column comments | the DataObject implements `CanHandleCatalogMetadata` |
| primary key | `table.createAndReplacePrimaryKey = true` |
| foreign keys | `table.createAndReplaceForeignKeys = true` |

Columns are never dropped - a column which is not written anymore is made nullable instead, so that existing
data is kept and new records can be written without it. This is the same behaviour as the schema evolution
of an SDLB run, see [Schema Evolution](#schema-evolution). Note that changing the data type of a column needs
the table property `delta.enableTypeWidening` on Delta Lake tables, and that a database can refuse a data type
change which would lose data.

A missing table is created by writing an empty DataFrame with the exported schema, so it gets the same
location, partitioning and options as it would get from the first run of the data pipeline. Primary key
columns are made not null if `table.createAndReplacePrimaryKey` is set.

Foreign keys are applied in a second phase, after all tables of the configuration have been created with
their primary keys, as a foreign key can only reference an existing primary key. The referenced table is
`foreignKeys.table` in the database `foreignKeys.db`, which defaults to the database of this table.

Support by DataObject:

| DataObject | Table & schema | Comments | Primary key | Foreign keys |
| --- | --- | --- | --- | --- |
| `DeltaLakeTableDataObject` | yes | yes | yes (Databricks) | yes (Databricks) |
| `IcebergTableDataObject` | yes | yes | - | - |
| `JdbcTableDataObject` | yes | yes, if the JDBC driver supports `COMMENT ON` | yes | yes |
| `SnowflakeTableDataObject` | - | yes | yes | - |

Other DataObjects are skipped. Note that primary and foreign keys are informational constraints on Databricks
Unity Catalog: they are not enforced, and they are not available on open source Delta Lake.


<!-- TODO Review all below -->

## Schema Evolution
SmartDataLakeBuilder is built to support schema evolution where possible. This means that data pipelines adapt themselves automatically to additional or removed columns and changes of data types if possible.
The following cases can be distinguished:

* Overwrite all (CopyAction): if all data of a DataObject is overwritten, the schema can be replaced: additional columns are added, removed columns are removed and data types are changed. Requirements:
    * Output DataObject needs to be able to replace schema.
* Overwrite all keeping existing data (Historize- & DeduplicateAction): Action consolidates new data with existing data. The schema needs to be evolved: additional columns are added with null value for existing records, removed columns are kept with null values for new records and data types are changed to new data type if supported. Requirements:
    * Output DataObject needs to be able to replace schema.
    * Output DataObject must be a TransactionalSparkTableDataObject (read existing data and overwrite new data in the same SparkJob, preventing data loss in case of errors).
* Overwrite incremental using merge (CopyAction, DeduplicateAction): Action incrementally merges new data into existing data. The schema needs to be evolved: additional columns are added with null value for existing records, removed columns are kept with null values for new records and data types are changed to new data type if supported. Requirements:
    * Output DataObject needs to support CanEvolveSchema (alter schema automatically when writing to this DataObject with different schema)
    * Output DataObject needs to support CanMergeDataFrame (use SQL merge statement to update and insert records transactionally)


## Specific behaviour of DataObjects:

* SparkFileDataObject: see detailed description in [Spark Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html).
    * Many Data Sources support schema inference (e.g. Json, Csv), but we would not recommend this for production data pipelines as the result might not be stable when new data arrives.
    * For Data Formats with included schema (e.g. Avro, Parquet), schema is read from a random data file. If data files have different schemas, Parquet Data Source supports to consolidate schemas by setting option `mergeSchema=true`. Avro Data Source does not support this.
    * If you define the `schema` attribute of the DataObject, SDL tries to read the data files with the defined schema. This is e.g. supported by the Json Data Source, but not the CSV Data Source.
* JdbcTableDataObject: The database table can be created automatically on first write or by providing a create table statement in `createSql` attribute. Also existing table is automatically adapted (add & change column) when option `allowSchemaEvolution=true`.
* DeltaLakeTableDataObject: Existing schema is automatically adapted (add & change column) when option `allowSchemaEvolution=true`.
* IcebergTableDataObject: Existing schema is automatically adapted (add & change column) when option `allowSchemaEvolution=true`.

## Recipes for data pipelines with schema evolution

* "Overwrite all" with CopyAction: overwriting the whole output DataObject including its schema. It needs an output DataObject which doesn't have a fixed schema, e.g. HiveTableDataObject.
* "Overwrite all keeping existing data" with HistorizeAction & DeduplicateAction: consolidate the existing data & schema of the output DataObject with a potentially new schema of the input DataObject. Then it overwrites the whole output DataObject. It needs a TransactionalSparkTableDataObject as output, e.g. TickTockHiveTableDataObject.
* "Overwrite incremental using merge" with CopyAction & DeduplicateAction: evolve the existing schema of the output DataObject and insert and update new data using merge. It needs an output DataObject supporting CanMergeDataFrame and CanEvolveSchema, e.g. JdbcTableDataObject, DeltaLakeTableObject
