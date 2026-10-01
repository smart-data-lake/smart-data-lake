# sdl-sql

SDLB engine module implementing a `SQLSubFeed`, which does not process data itself but creates SQL statements to be
executed by a database (ELT, see issue [#866](https://github.com/smart-data-lake/smart-data-lake/issues/866)).
Transformations are built as an [SQLGlot](https://github.com/tobymao/sqlglot) query, SQLGlot optimizes it and renders
it in the SQL dialect of the target database. SQLGlot supports many dialects and translates between them, so SQL
transformers can be written in Spark SQL and executed on e.g. Postgres, SQL Server or Snowflake.

## Usage

An Action uses the SQL engine if its `engineConnectionId` references a `JdbcConnection`. All its inputs and
outputs must then be `JdbcTableDataObject`s of this connection. The data is never transferred out of the database:
the transformations are executed with `INSERT INTO ... SELECT` statements, or a merge statement.

```hocon
connections {
  dwh {
    type = JdbcConnection
    url = "jdbc:postgresql://localhost:5432/dwh"
    driver = org.postgresql.Driver
    # dialect = postgres  # SQLGlot dialect of the database, derived from the url by default
  }
}
actions {
  load-customers {
    type = CopyAction
    inputId = stg-customers
    outputId = int-customers
    engineConnectionId = dwh
    transformers = [{
      type = SQLDfTransformer
      code = "select id, upper(name) as name, nvl(city, 'unknown') as city from %{inputViewName}"
      # sqlDialect = spark  # SQLGlot dialect of the SQL code, default is spark
    }]
  }
}
```

Supported are the save modes Overwrite (delete and insert in one transaction, also for virtual partitions), Append
and Merge (with a temporary table created by `CREATE TABLE ... AS SELECT`), and incremental output with
`DataObjectStateIncrementalMode`.

Schema evolution (`allowSchemaEvolution = true`) adds new columns, widens data types, and makes columns missing in
the DataFrame nullable, with `ALTER TABLE` statements rendered by SQLGlot for the dialect of the database.
As the types inferred by SQLGlot are not exact, a type is only changed if the new type is wider (e.g. `INT` to
`BIGINT`, or `DECIMAL(5, 1)` to `DECIMAL(10, 2)`), and string types only if both have a length. Other changes,
e.g. `INT` to `VARCHAR`, fail with a `SchemaEvolutionException`. Schema changes of `CatalogSchemaUpdater` are
applied the same way.

### Views

A `JdbcViewDataObject` (sdl-core) is a view in the database. Writing a DataFrame to it creates or replaces the view
with the query of the DataFrame, e.g. `CREATE OR REPLACE VIEW` (`CREATE OR ALTER VIEW` for SQL Server), so the
Action writing it must use the SQL engine. It is read like a table by all engines of `JdbcTableDataObject`, e.g. by a
Spark Action in the same feed.

```hocon
dataObjects {
  btl-customers-bern {
    type = JdbcViewDataObject
    connectionId = dwh
    table = { db = public, name = customers_bern }
  }
}
actions {
  create-customers-bern {
    type = CopyAction
    inputId = int-customers
    outputId = btl-customers-bern
    engineConnectionId = dwh
    transformers = [{
      type = SQLDfTransformer
      code = "select id, name from %{inputViewName} where city = 'Bern'"
    }]
  }
}
```

The view is replaced in exec phase on every run, and a missing view is created in init phase, like a missing table.
In init phase the query is also validated by executing it without fetching rows. As the query of a view is stored in the database, it must not depend on the current run: an Action
writing a view (see marker trait `ViewDataObject`) ignores the partition values and filters of its inputs, and must
not have an execution mode. Only save mode Overwrite is supported.

With `allowSchemaEvolution = false`, an existing view is not replaced by a run, and a run fails if the columns of the
view changed. Changed views are then deployed with `CatalogSchemaUpdater`, from the query exported by a dry-run with
schema export (`--test dry-run-with-schema-export`), see `docs/docs/reference/schema.md`. A missing view is still
created in init phase, like a missing table. In init phase the SQL engine references its inputs as tables with the
schema passed on by the previous Action (`SQLSubFeed.getInitDataFrame`), so that the exported query and a view
created in init phase read the real tables and views, not empty DataFrames.

Replacing a view keeps the grants on it, by an SDLB run as well as by `CatalogSchemaUpdater`: `CREATE OR REPLACE VIEW`
keeps them for most databases, e.g. Postgres, Oracle and MySQL, and SQL Server uses `CREATE OR ALTER VIEW`. Snowflake
drops them on replace, so `COPY GRANTS` is added, and Databricks as well, so an existing view is changed with
`ALTER VIEW ... AS`. A view is never dropped and created again. For other databases check whether
`CREATE OR REPLACE VIEW` keeps the grants.

Note that Postgres can not replace a view if existing columns are renamed, removed or change their type; the view
must then be dropped first, which also drops its grants.

To compare the query of an existing view with a new one, both are normalized with SQLGlot. For Postgres, the new
query is first rewritten by the database like the definition of a view, e.g. with casts added and aliases removed, by
creating a temporary view in a transaction which is rolled back (`JdbcCatalog.getViewDefinitionOfQuery`).

### Materialized views

With `materialized = true`, a `JdbcViewDataObject` is a materialized view, supported for the SQLGlot dialects
postgres, redshift, oracle, snowflake and databricks. Other dialects fail in prepare phase. A missing materialized
view is created in init phase with data, as Postgres can not read an unpopulated one. On every run the Action writing it
refreshes it (`REFRESH MATERIALIZED VIEW`, `DBMS_MVIEW.REFRESH` on Oracle), except if it was created by the init
phase of the same run. Snowflake refreshes materialized views automatically. If the query changed and
`allowSchemaEvolution = true`, it is replaced instead of refreshed. Its existing query is read from `pg_matviews` (Postgres) and
`ALL_MVIEWS` (Oracle). For other databases it can not be compared, and it is replaced on every run.

Snowflake keeps the grants with `CREATE OR REPLACE MATERIALIZED VIEW ... COPY GRANTS`, and Databricks uses
`CREATE OR REPLACE MATERIALIZED VIEW`. Postgres, Redshift and Oracle can not replace a materialized view, so it is
dropped and created again. The privileges granted on it are read before (`JdbcCatalog.getGrants`, from `pg_class.relacl`
for Postgres and `ALL_TAB_PRIVS` for Oracle), and are granted again after it is created. Drop, create and grants run in
one transaction, so that nothing changes if one of them fails, at least on Postgres, where DDL is transactional. For
Redshift the grants can not be read yet, so a warning says that they are lost. Postgres can not drop a materialized
view while other views depend on it.

The materialized view tests run on an embedded Postgres (zonky embedded-postgres, `SQLTestUtil.createPostgresConnection`),
as DuckDB has no materialized views.

### Mixed feeds

Actions of a feed can use different engines, e.g. Spark loads a table which the SQL engine transforms, and Spark
exports the result. The engine is selected per Action, and the DataFrame is read again from the DataObject by the next
Action. In init phase the schema is passed on to the next Action instead, so that it also works if the DataObject does
not exist yet, e.g. a view created in exec phase. Schemas are converted between engines through their engine-neutral
Json representation, see `SchemaConverter` (sdl-core), which uses the Spark type names for simple types
(`SQLDataType.sparkTypeName`). E.g. `INT` becomes `integer`, string types become `string`, and `TIMESTAMP` becomes
`timestamp`, as Spark reads it from a database with JDBC. Types without Spark equivalent, e.g. `UNKNOWN` for types
SQLGlot can not infer, can not be converted: the next Action then gets the schema from the DataObject.
The same conversion is used to validate a DataFrame of the SQL engine against a `schemaMin`, which is parsed with
Spark if sdl-spark is on the classpath.

## Architecture

SQLGlot is a Python library. It runs in a Python interpreter embedded into the JVM with
[jep](https://github.com/ninia/jep) (Java Embedded Python):

| Component | File | Purpose |
|-----------|------|---------|
| `JepInterpreter` | `util/python/JepInterpreter.scala` | Embeds the Python interpreter. Finds the Python environment by running its executable, loads libpython and jep's native library, and executes all calls on one dedicated thread, as a jep interpreter is bound to the thread which created it. There is at most one per JVM. |
| `bridge.py` | `src/main/python/sdlb_sql/bridge.py` | The Python side. Keeps a registry of DataFrames (SQLGlot queries) under numeric ids, and implements the DataFrame operations on them. Single entry point `call(op, args_json)` with JSON in and out. Packaged into the jar and loaded from the classpath, so only sqlglot and jep need to be installed. |
| `SqlGlotBridge` | `util/sqlglot/SqlGlotBridge.scala` | The Scala side of the bridge: loads `bridge.py`, calls its operations, throws `SqlGlotException` with the Python traceback on errors, and releases Python DataFrames when their `SQLDataFrame` is garbage collected. |
| `SQLSubFeed` | `workflow/dataframe/sql/SQLSubFeed.scala` | The SubFeed, plus its companion implementing `DataFrameSubFeedCompanion`/`DataFrameFunctions`. |
| `SQLDataFrame` | `workflow/dataframe/sql/SQLDataFrame.scala` | Remote-controls a DataFrame of the bridge by its id. `toSql(dialect)` renders the SQL statement. |
| `SQLColumn` | `workflow/dataframe/sql/SQLColumn.scala` | A column expression as Spark SQL text (SQLGlot dialect `databricks`, i.e. Spark SQL with ANSI casts). Operators and functions compose the SQL text in Scala, no call to Python is needed. |
| `SQLSchema` | `workflow/dataframe/sql/SQLSchema.scala` | Schema, fields and data types. Types of results are inferred by SQLGlot. |
| `JdbcTableSqlEngine` | `workflow/dataobject/JdbcTableSqlEngine.scala` | SQL engine implementation of the `JdbcTableEngine` SPI of `JdbcTableDataObject` (sdl-core), discovered on the classpath like the Spark implementation in sdl-spark. Reads the table schema from the JDBC metadata, validates that the DataObject uses the engine connection, and executes the writes. |
| `JdbcViewSqlEngine` | `workflow/dataobject/JdbcViewSqlEngine.scala` | SQL engine implementation of the `JdbcViewEngine` SPI of `JdbcViewDataObject` (sdl-core). Creates the view with a `CREATE OR REPLACE VIEW` statement rendered by SQLGlot, and creates, replaces and refreshes materialized views. Reading the view is done by `JdbcTableSqlEngine`. |

`JdbcConnection` (sdl-core) is the engine connection of the SQL engine: its SubFeed type is `SQLSubFeed`.

Every DataFrame operation wraps its input as subquery. The SQLGlot optimizer merges these subqueries again when
rendering the statement, e.g.

```scala
val df = SQLSubFeed.table("db.test_table", schema) // in the context of an Action using the SQL engine.withColumn("d", col("a") * lit(2))
df.createOrReplaceTempView("test_table_int")
SQLSubFeed.sql("select *, d * 2 as e from test_table_int", DataObjectId("do1"))
  .toSql(Some("postgres"))
// SELECT test_table.a AS a, ..., test_table.a * 2 AS d, test_table.a * 4 AS e FROM db.test_table AS test_table
```

Database tables are registered in SQLGlot under a placeholder name with their schema, and replaced by their real
name when rendering. Temporary views are replaced by their query when parsing the SQL of a transformer.

### Identifiers and case

Identifier names are passed on as they are written, and are resolved case-insensitively, like in Spark:

| Where | Resolution | Rendering in the dialect of the database |
|-------|------------|------------------------------------------|
| Names of the Scala API (`col("Name")`, `as("Name")`, `expr(...)`, filters of execution modes) | case-insensitive, backticks only escape special characters | new names unquoted if they are simple identifiers |
| SQL of transformers in their `sqlDialect` | unquoted: case-insensitive. Quoted: as defined by the dialect, i.e. case-insensitive for spark, tsql and duckdb, case-sensitive for postgres, snowflake and oracle | as above |
| Columns of database tables | against the schema from the JDBC metadata | spelling of the database, quoted only if the database would not resolve it unquoted, e.g. `"Name"` in postgres, but `NAME` in snowflake |

New names, e.g. aliases or the columns of a table created by `CREATE TABLE ... AS SELECT`, are rendered unquoted if
possible, so that the database normalizes their case as usual, like the Spark JDBC engine does. DataFrames report
their columns with the spelling as written, e.g. `df.columns == Seq("TownName")`, or the spelling of the database
for columns of a table.

Internally, the bridge normalizes all identifiers to lower case for resolution by SQLGlot, and keeps their spelling
in the meta data of the identifiers. When rendering a statement, every column reference gets the spelling of the
column it references, see the module documentation of `bridge.py`.

With `Environment.caseSensitive = true`, identifiers are resolved exactly as written and always quoted.

Column lineage (see `docs/docs/reference/columnLineage.md`) is extracted with the lineage module of SQLGlot. The
query of every input DataFrame is replaced by a placeholder table before following the lineage of the output
columns, so that it works for tables as well as for DataFrames created from values or other transformations.

Known limitations:
- Expressions given as string (`expr(...)`, filters of execution modes) are Spark SQL, only SQL of transformers is
  parsed in their `sqlDialect`.
- Column names differing only in case are ambiguous, unless `Environment.caseSensitive` is set.
- Data types of written DataFrames are not validated against the table, only column names, as the types inferred
  by SQLGlot are not exact.
- Schema evolution of nested columns is not supported, jdbc tables have no nested columns.
- Not implemented: `hash`, `from_json`, `raise_error`, `array_construct_compact` and UDFs.

## Python environment

The Python environment needs sqlglot and jep, and is managed with [uv](https://docs.astral.sh/uv), see
`pyproject.toml`. jep is compiled against the local JDK on installation, so `JAVA_HOME` must be set:

```bash
cd sdl-sql
uv sync
export SDL_PYTHON_PATH=$PWD/.venv/bin/python   # .venv\Scripts\python.exe on Windows
```

SDLB finds the environment through `Environment.pythonPath`, which is set by the environment variable
`SDL_PYTHON_PATH` (or the java property `sdl.pythonPath`), and otherwise uses `python3` on the PATH. The version of the jep Maven dependency in `pom.xml` must match the jep Python
package.

## Tests

```bash
cd sdl-sql && uv run pytest                       # Python side
mvn -B test -pl sdl-sql -Dlicense.skip=true       # Scala side, needs SDL_PYTHON_PATH
```

The Scala tests execute SQL on a [DuckDB](https://duckdb.org) database file in `target/duckdb`, see `SQLTestUtil`.
The tests needing Python cancel themselves if no environment with jep is found, so the normal build needs no
Python. They run in the GitHub workflow `sql_engine_tests.yml`.
