# sdl-sql

SDLB engine module implementing a `SQLSubFeed`, which does not process data itself but creates SQL statements to be
executed by a database (ELT, see issue [#866](https://github.com/smart-data-lake/smart-data-lake/issues/866)).
Transformations are built as an [SQLGlot](https://github.com/tobymao/sqlglot) query, SQLGlot optimizes it and renders
it in the SQL dialect of the target database. SQLGlot supports many dialects and translates between them, so SQL
transformers can be written in Spark SQL and executed on e.g. Postgres, SQL Server or Snowflake.

**Status**: DataFrame operations and SQL transformers are translated into SQL statements, and operations reading
data (`collect`, `count`, `isEmpty`, `show`, observations) execute them on the database of the `SQLEngineConnection`.
There are no DataObjects reading or writing `SQLSubFeed`s yet (next step of #866).

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
| `SQLEngineConnection` | `workflow/connection/SQLEngineConnection.scala` | `EngineConnection` selecting the SQL engine, and JDBC connection (pool) to the database executing its statements, see `GenericJdbcExecution` in sdl-core. The SQLGlot `dialect` of the database is derived from the JDBC url if not configured, `sqlDialect` is the dialect of SQL transformers. |

Every DataFrame operation wraps its input as subquery. The SQLGlot optimizer merges these subqueries again when
rendering the statement, e.g.

```scala
val df = SQLSubFeed.table("db.test_table", schema).withColumn("d", col("a") * lit(2))
df.createOrReplaceTempView("test_table_int")
SQLSubFeed.sql("select *, d * 2 as e from test_table_int", DataObjectId("do1"))
  .toSql(Some("postgres"))
// SELECT "test_table"."a" AS "a", ..., "test_table"."a" * 2 AS "d", "test_table"."a" * 4 AS "e" FROM db.test_table AS "test_table"
```

Database tables are registered in SQLGlot under a placeholder name with their schema, and replaced by their real
name when rendering. Temporary views are replaced by their query when parsing the SQL of a transformer.

Known limitations:
- Expressions given as string (`expr(...)`, filters of execution modes) are parsed in the default SQLGlot dialect,
  only SQL of transformers is parsed in `sqlDialect`.
- Column names are case-sensitive. Unquoted identifiers in SQL of transformers are normalized to lower case.
- Not implemented: `hash`, `from_json`, `raise_error`, `array_construct_compact`, UDFs and schema evolution.

## Python environment

The Python environment needs sqlglot and jep, and is managed with [uv](https://docs.astral.sh/uv), see
`pyproject.toml`. jep is compiled against the local JDK on installation, so `JAVA_HOME` must be set:

```bash
cd sdl-sql
uv sync
export SDLB_PYTHON=$PWD/.venv/bin/python   # .venv\Scripts\python.exe on Windows
```

SDLB finds the environment through `SDLB_PYTHON` or the `pythonExecutable` attribute of `SQLEngineConnection`, and
otherwise uses `python3` on the PATH. The version of the jep Maven dependency in `pom.xml` must match the jep Python
package.

## Tests

```bash
cd sdl-sql && uv run pytest                       # Python side
mvn -B test -pl sdl-sql -Dlicense.skip=true       # Scala side, needs SDLB_PYTHON
```

The Scala tests execute SQL on a [DuckDB](https://duckdb.org) database file in `target/duckdb`, see `SQLTestUtil`.
The tests needing Python cancel themselves if no environment with jep is found, so the normal build needs no
Python. They run in the GitHub workflow `sql_engine_tests.yml`.
