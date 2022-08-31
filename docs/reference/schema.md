---
id: schema
title: dataObject Schema
---

Smart Data Lake Builder relies on DataFrame schema to verify proper transitions from one dataObject to another. 

Depending on the dataObject the schema is provided directly with the data source. Alternative, the schema can be specified in the configuration or inferred from the data. 

Furthermore, depending on the dataObject type, schema evolution is supported, see below.

## Schema inference
For most dataObjects the Schema can be inferred, e.g. by sampling items in an XML stream.
Nevertheless, best practice especially for production cases are fixed schemata.  

## schemaMin
To assert that a defined list of columns is always present in the schema of a specific DataObject, use its `schemaMin` attribute to define a minimal schema. The minimal schema is validated on read and write with Spark.

## Specifying Schema
To fix the schema for a specific DataObject many DataObjects support the `schema` attribute (e.g. all children of SparkFileDataObject) for reading and writing with Spark.
The `schema` attribute allows to define the schema the DataObject tries to read data with, and can be used to avoid schema inference with Spark DataSources.
On write the DataFrame to be written must match the defined `schema` exactly (nullability and column order are ignored).


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

* HiveTableDataObject & TickTockHiveTableDataObject: Table schema is managed by Hive and automatically created on first write and updated on subsequent overwrites of the whole table. Changing schema for partitioned tables is not supported.
  By manipulating the table definition with DDL statements (e.g. alter table add columns) its possible to read data files with a different schema.
* SparkFileDataObject: see detailed description in [Spark Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html).
    * Many Data Sources support schema inference (e.g. Json, Csv), but we would not recommend this for production data pipelines as the result might not be stable when new data arrives.
    * For Data Formats with included schema (e.g. Avro, Parquet), schema is read from a random data file. If data files have different schemas, Parquet Data Source supports to consolidate schemas by setting option `mergeSchema=true`. Avro Data Source does not support this.
    * If you define the `schema` attribute of the DataObject, SDL tries to read the data files with the defined schema. This is e.g. supported by the Json Data Source, but not the CSV Data Source.
* JdbcTableDataObject: The database table can be created automatically on first write or by providing a create table statement in `createSql` attribute. Also existing table is automatically adapted (add & change column) when option `allowSchemaEvolution=true`.
* DeltaLakeTableDataObject: Existing schema is automatically adapted (add & change column) when option `allowSchemaEvolution=true`.

## Recipes for data pipelines with schema evolution

* "Overwrite all" with CopyAction: overwriting the whole output DataObject including its schema. It needs an output DataObject which doesn't have a fixed schema, e.g. HiveTableDataObject.
* "Overwrite all keeping existing data" with HistorizeAction & DeduplicateAction: consolidate the existing data & schema of the output DataObject with a potentially new schema of the input DataObject. Then it overwrites the whole output DataObject. It needs a TransactionalSparkTableDataObject as output, e.g. TickTockHiveTableDataObject.
* "Overwrite incremental using merge" with CopyAction & DeduplicateAction: evolve the existing schema of the output DataObject and insert and update new data using merge. It needs an output DataObject supporting CanMergeDataFrame and CanEvolveSchema, e.g. JdbcTableDataObject, DeltaLakeTableObject

