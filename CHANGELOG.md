# Changelog

## 2.9.0

## New Features
- **BigQueryTableDataObject**: new `sdl-gcp` module with `BigQueryTableDataObject` and `BigQueryTableConnection` to read/write Google BigQuery tables, supporting `GCPCredentialsKeyAuth` (#962).
- **ScalaDataFrame and ScalaSubFeed** (#1045): new plain-Scala DataFrame implementation (`plainScala` package) that allows running transformations without a Spark dependency.
- **Expectations and constraints for FileDataObjects** (#1051): extend expectation and constraint validation to all file-based DataObjects (CSV, Parquet, Avro, JSON, XML, …).
- **DsComment.transformCommentCols** (#1050): new dataset utility to add and propagate column comments through transformations.
- **ScalaClassSparkDfsTransformer: configurable input/output DataFrame mapping** (#1047): allows explicitly configuring which DataFrames are passed as inputs and captured as outputs when using a Scala class as a multi-DataFrame transformer.

## Improvements
- Extended dataset utilities and TestTool (#1038):
  - Reorganized `DataFrameUtil` into focused utility classes: `dataset.Quality` (split into `DsQuality`, `DsComment`, `DsPk`), `dataset.Equality`, `dataset.Transform`, `dataset.Types`, and a new `util.Compare` utility.
  - String manipulators moved from `DataFrameUtil` to `util.misc.StringUtil`.
  - `getEmptyDataFrame` and `persist` helpers moved to the `dataset` package.
  - `Collection` and `TestTool` moved to `testutils`.
- DataObjectSchemaExporter: new command-line options `withStats`, `preferredSubFeedType`, and `stopOnError`.
- Implement reading exported schemas for dry-runs (#1017).
- Reduce JSON schema size and duplication by registering `Expectation` and `HttpAuthMode` as base types and including subtype refs (#1030).
- Fix missing or incorrect descriptions in JSON schema export (#1011, #1028).
- SmartDataLakeBuilderLab: sort console output of DataFrames for more readable interactive sessions.
- HousekeepingMode: make abstract methods public to simplify custom implementations.
- Enforce valid `metadata.description` on DataObjects when the attribute is present (#1027).
- SnowparkSubFeed: add support for the Snowflake `VARIANT` datatype.
- Add `sdl-debezium` to `sdl-lang` so the schema-exporter can include Debezium DataObjects.
- Schema export: fix including remote agents (#1031).
- FileExportWriter: use `localfile` as the URI prefix for local file exports.

## Bugfixes
- Fix applying `ExecutionModeResult.outputPartitionValues` (#1036).
- Fix typo: schema diff is now shown only when explicitly requested (#1053).
- Fix unchecked access to optional attribute `bufferSetup` in `ODataDataObject` that caused a runtime exception (#1020).
- Fix parsing `DECIMAL` types from Snowflake schema exports.
- ODataDataObject: rename attribute `authorization` to `authMode` for consistency with other DataObjects (#1022).

## Version Updates and Dependencies
- Update spark-extensions to 3.5.5.

## Upgrade Notes
- **ODataDataObject**: the `authorization` attribute has been renamed to `authMode`. Update your configuration files accordingly before upgrading.
- **DataFrameUtil refactoring (#1038)**: several methods previously available on `DataFrameUtil` / `DataFrameUtil.DfSDL` have been moved to the new `dataset.*` and `util.misc.StringUtil` packages. If you reference these utilities in custom Scala transformers, update your imports.
- **ScalaClassSparkDfsTransformer**: the new `inputMapping`/`outputMapping` options are optional and fully backward-compatible; no migration is required unless you want to take advantage of the new flexible mapping.
