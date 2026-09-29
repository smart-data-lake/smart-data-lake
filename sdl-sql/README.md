# sdl-sql

SDLB engine module implementing a `SQLSubFeed`, which does not process data itself but creates SQL statements to be
executed by a database (ELT, see issue [#866](https://github.com/smart-data-lake/smart-data-lake/issues/866)).
Transformations are built as an [SQLGlot](https://github.com/tobymao/sqlglot) query, SQLGlot optimizes it and renders
it in the SQL dialect of the target database. SQLGlot supports many dialects and translates between them, so SQL
transformers can be written in Spark SQL and executed on e.g. Postgres, SQL Server or Snowflake.

## Usage

An Action uses the SQL engine if its `engineConnectionId` references a `JdbcTableConnection`. All its inputs and
outputs must then be `JdbcTableDataObject`s of this connection. The data is never transferred out of the database:
the transformations are executed with `INSERT INTO ... SELECT` statements, or a merge statement.

```hocon
connections {
  dwh {
    type = JdbcTableConnection
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
| `SQLColumn` | `workflow/dataframe/sql/SQLColumn.scala` | A column expression as SQL text in the default SQLGlot dialect. Operators and functions compose the SQL text in Scala, no call to Python is needed. |
| `SQLSchema` | `workflow/dataframe/sql/SQLSchema.scala` | Schema, fields and data types. Types of results are inferred by SQLGlot. |
| `JdbcTableSqlEngine` | `workflow/dataobject/JdbcTableSqlEngine.scala` | SQL engine implementation of the `JdbcTableEngine` SPI of `JdbcTableDataObject` (sdl-core), discovered on the classpath like the Spark implementation in sdl-spark. Reads the table schema from the JDBC metadata, validates that the DataObject uses the engine connection, and executes the writes. |

`JdbcTableConnection` (sdl-core) is the engine connection of the SQL engine: its SubFeed type is `SQLSubFeed`.

Every DataFrame operation wraps its input as subquery. The SQLGlot optimizer merges these subqueries again when
rendering the statement, e.g.

```scala
val df = SQLSubFeed.table("db.test_table", schema) // in the context of an Action using the SQL engine.withColumn("d", col("a") * lit(2))
df.createOrReplaceTempView("test_table_int")
SQLSubFeed.sql("select *, d * 2 as e from test_table_int", DataObjectId("do1"))
  .toSql(Some("postgres"))
// SELECT "test_table"."a" AS "a", ..., "test_table"."a" * 2 AS "d", "test_table"."a" * 4 AS "e" FROM db.test_table AS "test_table"
```

Database tables are registered in SQLGlot under a placeholder name with their schema, and replaced by their real
name when rendering. Temporary views are replaced by their query when parsing the SQL of a transformer.

Known limitations:
- Expressions given as string (`expr(...)`, filters of execution modes) are parsed in the default SQLGlot dialect,
  only SQL of transformers is parsed in their `sqlDialect`.
- Column names are case-sensitive. Unquoted identifiers in SQL of transformers are normalized to lower case.
- Data types of written DataFrames are not validated against the table, only column names, as the types inferred
  by SQLGlot are not exact.
- Not implemented: schema evolution (`allowSchemaEvolution`, applying schema changes), `hash`, `from_json`,
  `raise_error`, `array_construct_compact` and UDFs.

## Python environment

The Python environment needs sqlglot and jep, and is managed with [uv](https://docs.astral.sh/uv), see
`pyproject.toml`. jep is compiled against the local JDK on installation, so `JAVA_HOME` must be set:

```bash
cd sdl-sql
uv sync
export SDLB_PYTHON=$PWD/.venv/bin/python   # .venv\Scripts\python.exe on Windows
```

SDLB finds the environment through the environment variable `SDLB_PYTHON`, and otherwise uses `python3` on the PATH. The version of the jep Maven dependency in `pom.xml` must match the jep Python
package.

## Tests

```bash
cd sdl-sql && uv run pytest                       # Python side
mvn -B test -pl sdl-sql -Dlicense.skip=true       # Scala side, needs SDLB_PYTHON
```

The Scala tests execute SQL on a [DuckDB](https://duckdb.org) database file in `target/duckdb`, see `SQLTestUtil`.
The tests needing Python cancel themselves if no environment with jep is found, so the normal build needs no
Python. They run in the GitHub workflow `sql_engine_tests.yml`.
