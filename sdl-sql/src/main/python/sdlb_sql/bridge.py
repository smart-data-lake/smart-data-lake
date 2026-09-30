#
# Smart Data Lake Builder - Build your data lake the smart way.
#
# Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

"""SQLGlot DataFrame bridge of the SDLB SQL engine.

The Scala class io.smartdatalake.workflow.dataframe.sql.SQLDataFrame remote-controls the DataFrames of this module
through jep. A DataFrame is an SQLGlot AST (a query) stored in a registry under a numeric id. Every DataFrame
operation creates a new DataFrame, wrapping its input as subquery. The SQLGlot optimizer merges these subqueries again
when the SQL statement for the target database is rendered.

Column expressions are passed as SQL text in the default SQLGlot dialect, they are created by the Scala class
SQLColumn.

Tables of the database are registered with their schema under a placeholder name `__sdlb_t<n>`, so that the SQLGlot
schema needs no nesting of catalogs and databases. The placeholders are replaced by the real table names when
rendering the SQL statement.

There is a single entry point for the JVM: `call(op, args_json)`, taking the name of an operation and its arguments as
JSON object, and returning a JSON object with either `result` or `error` and `traceback`.
"""

import json
import re
import traceback
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import OptimizeError
from sqlglot.optimizer import optimize
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.optimizer import RULES
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema

# canonicalize is left out: it rewrites expressions based on the types inferred by SQLGlot, e.g. it removes casts it
# considers redundant, but the database might infer different types (e.g. DECIMAL instead of DOUBLE for 1.5).
_OPTIMIZER_RULES = tuple(rule for rule in RULES if rule is not canonicalize)

_PLACEHOLDER_PREFIX = "__sdlb_t"
_ROW_NUMBER_COLUMN = "__sdlb_rn"

_JOIN_TYPES = {
    "inner": "inner",
    "cross": "cross",
    "left": "left",
    "leftouter": "left",
    "left_outer": "left",
    "right": "right",
    "rightouter": "right",
    "right_outer": "right",
    "full": "full",
    "fullouter": "full",
    "full_outer": "full",
    "outer": "full",
    "semi": "semi",
    "leftsemi": "semi",
    "left_semi": "semi",
    "anti": "anti",
    "leftanti": "anti",
    "left_anti": "anti",
}


@dataclass
class _DataFrame:
    expr: exp.Query
    columns: list
    alias: str
    # True if expr is a join whose inputs can still be referenced by their alias, e.g. `"_t1"."id"`.
    # Projections and filters are then applied to expr directly instead of wrapping it into a subquery, see
    # Session._in_join_scope.
    join_scope: bool = False


def _col(name, table=None):
    return exp.column(name, table=table, quoted=True)


def _parse(sql):
    return sqlglot.parse_one(sql)


def _is_simple_identifier(name):
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is not None


def _parse_ordered(sql):
    return exp.maybe_parse(sql, into=exp.Ordered)


def _type_json(data_type):
    if data_type is None:
        return {"type": "UNKNOWN"}
    if data_type.this == exp.DataType.Type.STRUCT:
        return {"struct": [{"name": f.name, "type": _type_json(f.args.get("kind"))} for f in data_type.expressions]}
    if data_type.this == exp.DataType.Type.ARRAY:
        return {"array": _type_json(data_type.expressions[0] if data_type.expressions else None)}
    if data_type.this == exp.DataType.Type.MAP:
        key, value = (data_type.expressions + [None, None])[:2]
        return {"map": [_type_json(key), _type_json(value)]}
    return {"type": data_type.sql()}


class Session:
    def __init__(self):
        self.reset()

    def reset(self):
        self._counter = 0
        self._dfs = {}
        self._tables = {}
        self._views = {}
        self._schema = MappingSchema(normalize=False)
        return None

    # registry

    def _next_id(self):
        self._counter += 1
        return self._counter

    def _df(self, df_id):
        df = self._dfs.get(df_id)
        if df is None:
            raise ValueError(f"DataFrame {df_id} not found, it might have been released already")
        return df

    def _register(self, expr, alias=None, join_scope=False, columns=None):
        if columns is None:
            columns = qualify(expr.copy(), schema=self._schema, validate_qualify_columns=True).named_selects
        df_id = self._next_id()
        df = _DataFrame(expr, list(columns), alias or f"_t{df_id}", join_scope)
        self._dfs[df_id] = df
        return {"id": df_id, "columns": df.columns, "alias": df.alias}

    def _info(self, df_id):
        df = self._df(df_id)
        return {"id": df_id, "columns": df.columns, "alias": df.alias}

    def _sub(self, df):
        return df.expr.subquery(exp.to_identifier(df.alias, quoted=True))

    def _in_join_scope(self, df, direct, wrapped):
        """Apply an operation directly to a join if its columns can be resolved there, so that they can reference the
        inputs of the join by their alias. Otherwise, e.g. for the join column of a join on columns which exists on
        both sides, apply it to the join wrapped as subquery, where the output columns of the join are referenced."""
        if df.join_scope:
            try:
                return direct()
            except OptimizeError:
                pass
        return wrapped()

    def release(self, ids):
        for df_id in ids:
            self._dfs.pop(df_id, None)
        return None

    # creating DataFrames

    def table(self, name, columns, dialect=None):
        """Create a DataFrame reading the database table `name` with `columns` as list of [name, type]"""
        placeholder = f"{_PLACEHOLDER_PREFIX}{self._next_id()}"
        real = exp.to_table(name, dialect=dialect)
        self._tables[placeholder] = real
        self._schema.add_table(placeholder, {c: exp.DataType.build(t, udt=True) for c, t in columns})
        source = exp.Table(this=exp.to_identifier(placeholder),
                           alias=exp.TableAlias(this=exp.to_identifier(real.name, quoted=True)))
        expr = exp.select(*[_col(c, real.name) for c, _ in columns]).from_(source)
        return self._register(expr, columns=[c for c, _ in columns])

    def query(self, query, columns, dialect=None, alias="q"):
        """Create a DataFrame reading the result of the SQL query `query` in the dialect of the database, with `columns`
        as list of [name, type]. It is used for DataObjects defined by a query instead of a table."""
        placeholder = f"{_PLACEHOLDER_PREFIX}{self._next_id()}"
        parsed = sqlglot.parse_one(query, read=dialect)
        if not isinstance(parsed, exp.Query):
            raise ValueError(f"SQL statement must be a query, but is {type(parsed).__name__}: {query}")
        self._tables[placeholder] = parsed
        self._schema.add_table(placeholder, {c: exp.DataType.build(t, udt=True) for c, t in columns})
        source = exp.Table(this=exp.to_identifier(placeholder), alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
        expr = exp.select(*[_col(c, alias) for c, _ in columns]).from_(source)
        return self._register(expr, columns=[c for c, _ in columns])

    def empty(self, columns):
        """Create an empty DataFrame with `columns` as list of [name, type]"""
        projections = [exp.cast(exp.null(), exp.DataType.build(t, udt=True)).as_(c, quoted=True) for c, t in columns]
        expr = exp.select(*projections).where(exp.false())
        return self._register(expr, columns=[c for c, _ in columns])

    def values(self, rows, columns):
        """Create a DataFrame from `rows` given as list of lists of SQL literals, with `columns` as list of [name, type]"""
        if not rows:
            return self.empty(columns)
        names = [c for c, _ in columns]
        alias = exp.TableAlias(this=exp.to_identifier("_v", quoted=True), columns=[exp.to_identifier(n, quoted=True) for n in names])
        source = exp.Values(expressions=[exp.Tuple(expressions=[_parse(v) for v in row]) for row in rows], alias=alias)
        projections = [exp.cast(_col(n, "_v"), exp.DataType.build(t, udt=True)).as_(n, quoted=True) for n, t in columns]
        return self._register(exp.select(*projections).from_(source), columns=names)

    def register_view(self, name, df):
        self._views[name.lower()] = self._df(df)
        return None

    def sql(self, query, dialect=None):
        """Parse the SQL query `query` in `dialect`, and replace the registered temporary views by their DataFrame"""
        parsed = sqlglot.parse_one(query, read=dialect)
        if not isinstance(parsed, exp.Query):
            raise ValueError(f"SQL statement must be a query, but is {type(parsed).__name__}: {query}")
        cte_names = {cte.alias_or_name.lower() for cte in parsed.find_all(exp.CTE)}
        for table in list(parsed.find_all(exp.Table)):
            if not isinstance(table.this, exp.Identifier):
                continue  # e.g. a table function
            view = self._views.get(table.name.lower()) if not table.db and not table.catalog else None
            if view is not None:
                alias = table.args.get("alias") or exp.TableAlias(this=table.this.copy())
                table.replace(exp.Subquery(this=view.expr.copy(), alias=alias.copy()))
            elif table.db or table.name.lower() not in cte_names:
                raise ValueError(f"Table or view not found: {table.sql()}")
        return self._register(parsed)

    # transforming DataFrames

    def alias(self, df, alias):
        d = self._df(df)
        return self._register(d.expr, alias=alias, join_scope=d.join_scope, columns=d.columns)

    def select(self, df, columns):
        d = self._df(df)
        return self._in_join_scope(
            d,
            lambda: self._register(d.expr.copy().select(*[_parse(c) for c in columns], append=False)),
            lambda: self._register(exp.select(*[_parse(c) for c in columns]).from_(self._sub(d))))

    def filter(self, df, condition):
        d = self._df(df)
        return self._in_join_scope(
            d,
            lambda: self._register(d.expr.copy().where(_parse(condition)), join_scope=True),
            lambda: self._register(exp.select("*").from_(self._sub(d)).where(_parse(condition)), columns=d.columns))

    def with_column(self, df, name, column):
        d = self._df(df)

        def direct():
            projection = _parse(column).as_(name, quoted=True)
            existing = d.expr.expressions
            if name in d.columns:
                projections = [projection if p.alias_or_name == name else p.copy() for p in existing]
            else:
                projections = [p.copy() for p in existing] + [projection]
            return self._register(d.expr.copy().select(*projections, append=False), join_scope=True)

        def wrapped():
            projection = _parse(column).as_(name, quoted=True)
            projections = [projection if c == name else _col(c) for c in d.columns]
            if name not in d.columns:
                projections.append(projection)
            return self._register(exp.select(*projections).from_(self._sub(d)))

        return self._in_join_scope(d, direct, wrapped)

    def with_column_renamed(self, df, name, new_name):
        d = self._df(df)
        if name not in d.columns:
            return self._info(df)
        projections = [_col(c).as_(new_name, quoted=True) if c == name else _col(c) for c in d.columns]
        return self._register(exp.select(*projections).from_(self._sub(d)))

    def drop(self, df, names):
        d = self._df(df)
        remaining = [c for c in d.columns if c not in names]
        if len(remaining) == len(d.columns):
            return self._info(df)
        return self._register(exp.select(*[_col(c) for c in remaining]).from_(self._sub(d)), columns=remaining)

    def join(self, df, other, how="inner", on=None, condition=None):
        left, right = self._df(df), self._df(other)
        if left.alias == right.alias:
            raise ValueError(f"Both sides of the join have the same alias '{left.alias}', use as(...) to give them distinct aliases")
        join_type = _JOIN_TYPES.get(how.lower())
        if join_type is None:
            raise ValueError(f"Unsupported join type {how}, supported are {', '.join(_JOIN_TYPES)}")
        if on is not None:
            for c in on:
                if c not in left.columns or c not in right.columns:
                    raise ValueError(f"Join column {c} does not exist on both sides of the join")
            cond = exp.and_(*[exp.EQ(this=_col(c, left.alias), expression=_col(c, right.alias)) for c in on]) if on else None
        else:
            cond = _parse(condition) if condition else None
        if cond is None and join_type not in ("cross", "inner"):
            raise ValueError(f"Join type {how} needs join columns or a condition")
        if join_type in ("semi", "anti"):
            exists = exp.Exists(this=exp.select("1").from_(self._sub(right)).where(cond))
            expr = exp.select(*[_col(c, left.alias) for c in left.columns]).from_(self._sub(left)) \
                .where(exists if join_type == "semi" else exp.not_(exists))
            return self._register(expr, columns=left.columns)
        if on is not None:
            key_side = {"right": right.alias}.get(join_type, left.alias)
            keys = [exp.Coalesce(this=_col(c, left.alias), expressions=[_col(c, right.alias)]).as_(c, quoted=True)
                    if join_type == "full" else _col(c, key_side) for c in on]
            projections = keys + [_col(c, left.alias) for c in left.columns if c not in on] \
                + [_col(c, right.alias) for c in right.columns if c not in on]
        else:
            projections = [_col(c, left.alias) for c in left.columns] + [_col(c, right.alias) for c in right.columns]
        join_type = "cross" if cond is None else join_type
        expr = exp.select(*projections).from_(self._sub(left)).join(self._sub(right), on=cond, join_type=join_type)
        columns = [p.alias_or_name for p in projections]
        return self._register(expr, join_scope=True, columns=columns)

    def group_by_agg(self, df, group_columns, aggregate_columns):
        d = self._df(df)
        groups = [_parse(c) for c in group_columns]
        expr = exp.select(*groups, *[_parse(c) for c in aggregate_columns]).from_(self._sub(d))
        if groups:
            expr = expr.group_by(*[g.unalias() for g in groups])
        return self._register(expr)

    def _select_columns(self, d, columns):
        return exp.select(*[_col(c) if c in d.columns else exp.null().as_(c, quoted=True) for c in columns]) \
            .from_(self._sub(d))

    def union_by_name(self, df, other, allow_missing_columns=False):
        left, right = self._df(df), self._df(other)
        if not allow_missing_columns and set(left.columns) != set(right.columns):
            raise ValueError(f"unionByName needs the same columns on both sides, but got {left.columns} and {right.columns}")
        columns = left.columns + [c for c in right.columns if c not in left.columns]
        expr = exp.union(self._select_columns(left, columns), self._select_columns(right, columns), distinct=False)
        return self._register(expr, columns=columns)

    def except_(self, df, other):
        left, right = self._df(df), self._df(other)
        expr = exp.except_(self._select_columns(left, left.columns), self._select_columns(right, left.columns), distinct=True)
        return self._register(expr, columns=left.columns)

    def distinct(self, df):
        d = self._df(df)
        return self._register(exp.select("*").from_(self._sub(d)).distinct(), columns=d.columns)

    def drop_duplicates(self, df, columns):
        d = self._df(df)
        if not columns:
            return self.distinct(df)
        keys = [_col(c) for c in columns]
        row_number = exp.Window(this=exp.RowNumber(), partition_by=keys,
                                order=exp.Order(expressions=[exp.Ordered(this=k.copy()) for k in keys]))
        inner = exp.select("*", row_number.as_(_ROW_NUMBER_COLUMN, quoted=True)).from_(self._sub(d)) \
            .subquery(exp.to_identifier("_dedup", quoted=True))
        expr = exp.select(*[_col(c) for c in d.columns]).from_(inner).where(exp.EQ(this=_col(_ROW_NUMBER_COLUMN), expression=exp.Literal.number(1)))
        return self._register(expr, columns=d.columns)

    def order_by(self, df, columns):
        d = self._df(df)
        return self._register(exp.select("*").from_(self._sub(d)).order_by(*[_parse_ordered(c) for c in columns]), columns=d.columns)

    def limit(self, df, n):
        d = self._df(df)
        return self._register(exp.select("*").from_(self._sub(d)).limit(n), columns=d.columns)

    # rendering

    def _replace_placeholders(self, expr):
        for table in list(expr.find_all(exp.Table)):
            real = self._tables.get(table.name)
            if isinstance(real, exp.Query):
                alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier("q"))
                table.replace(exp.Subquery(this=real.copy(), alias=alias.copy()))
            elif real is not None:
                for part in ("this", "db", "catalog"):
                    value = real.args.get(part)
                    table.set(part, value.copy() if value is not None else None)
        return expr

    def _render(self, expr, optimized=True):
        if optimized:
            expr = optimize(expr, schema=self._schema, rules=_OPTIMIZER_RULES)
        return self._replace_placeholders(expr)

    def schema(self, df):
        """Return the fields of the DataFrame as list of {name, type} with type inferred by SQLGlot"""
        expr = optimize(self._df(df).expr.copy(), schema=self._schema, rules=_OPTIMIZER_RULES)
        return [{"name": s.alias_or_name, "type": _type_json(s.type)} for s in expr.selects]

    def to_sql(self, df, dialect=None, optimized=True, pretty=False):
        return self._render(self._df(df).expr.copy(), optimized).sql(dialect=dialect, pretty=pretty)

    def create_table_as(self, df, table, dialect=None, with_data=True, quote_names=False):
        """Create a `CREATE TABLE <table> AS <query>` statement for the DataFrame. `table` is given in the dialect of the
        database. If `with_data` is false, the table is created empty. If `quote_names` is false, column names which
        are valid identifiers are not quoted, so that the database normalizes their case as for unquoted identifiers."""
        d = self._df(df)
        expr = d.expr.copy()
        if not with_data:
            expr = exp.select("*").from_(expr.subquery(exp.to_identifier("_ctas", quoted=True))).where(exp.false())
        expr = self._render(expr)
        if not quote_names:
            for projection in expr.selects:
                alias = projection.args.get("alias")
                if isinstance(projection, exp.Alias) and alias is not None and _is_simple_identifier(alias.name):
                    alias.set("quoted", False)
        create = exp.Create(this=exp.to_table(table, dialect=dialect), kind="TABLE", expression=expr)
        return create.sql(dialect=dialect)

    def create_table(self, table, columns, dialect=None, quote_names=False):
        """Create a `CREATE TABLE` statement with `columns` as list of [name, type, nullable]"""
        column_defs = [
            exp.ColumnDef(this=exp.to_identifier(name, quoted=quote_names or not _is_simple_identifier(name)),
                          kind=exp.DataType.build(tpe, udt=True),
                          constraints=[] if nullable else [exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())])
            for name, tpe, nullable in columns
        ]
        create = exp.Create(this=exp.Schema(this=exp.to_table(table, dialect=dialect), expressions=column_defs), kind="TABLE")
        return create.sql(dialect=dialect)

    def alter_table(self, table, changes, dialect=None, quote_names=False):
        """Create `ALTER TABLE` statements for schema changes, given as list of objects with keys `change` (add, type
        or nullable), `column`, `type` (new type for add and type, current type for nullable) and `nullable`.
        SQLGlot renders them for most dialects. Changes of nullability are not supported by SQLGlot for some dialects,
        and are created here, as well as type changes for Oracle."""
        dialect_name = (dialect or "").lower()
        target = exp.to_table(table, dialect=dialect).sql(dialect=dialect)
        statements = []
        for change in changes:
            name = change["column"]
            identifier = exp.to_identifier(name, quoted=quote_names or not _is_simple_identifier(name))
            column = identifier.sql(dialect=dialect)
            data_type = exp.DataType.build(change["type"], udt=True) if change.get("type") else None
            kind = change["change"]
            if kind == "nullable" and dialect_name in ("tsql", "mysql", "oracle"):
                null_sql = "NULL" if change["nullable"] else "NOT NULL"
                if dialect_name == "oracle":
                    statements.append(f"ALTER TABLE {target} MODIFY ({column} {null_sql})")
                    continue
                if data_type is None:
                    raise ValueError(f"The current data type of column {name} is needed to change its nullability for {dialect}")
                keyword = "ALTER COLUMN" if dialect_name == "tsql" else "MODIFY COLUMN"
                statements.append(f"ALTER TABLE {target} {keyword} {column} {data_type.sql(dialect=dialect)} {null_sql}")
                continue
            if kind == "type" and dialect_name == "oracle":
                statements.append(f"ALTER TABLE {target} MODIFY ({column} {data_type.sql(dialect=dialect)})")
                continue
            canonical_column = identifier.sql()
            if kind == "add":
                sql = f"ALTER TABLE t ADD COLUMN {canonical_column} {data_type.sql()}"
            elif kind == "type":
                sql = f"ALTER TABLE t ALTER COLUMN {canonical_column} SET DATA TYPE {data_type.sql()}"
            elif kind == "nullable":
                sql = f"ALTER TABLE t ALTER COLUMN {canonical_column} {'DROP' if change['nullable'] else 'SET'} NOT NULL"
            else:
                raise ValueError(f"Unknown schema change {kind}")
            statement = sqlglot.parse_one(sql)
            statement.set("this", exp.to_table(table, dialect=dialect))
            statements.append(statement.sql(dialect=dialect))
        return statements

    def parse_types(self, types, dialect=None):
        """Convert data types of the database, given as list of [type name, precision, scale], e.g. from JDBC metadata,
        into SQLGlot types. The result has the same format as the types of `schema`."""
        result = []
        for name, precision, scale in types:
            try:
                data_type = exp.DataType.build(name, dialect=dialect, udt=True)
            except Exception:
                data_type = exp.DataType.build("UNKNOWN")
            if data_type.this == exp.DataType.Type.DECIMAL and not data_type.expressions and precision:
                data_type = exp.DataType.build(f"DECIMAL({precision}, {scale or 0})")
            result.append(_type_json(data_type))
        return result

    def transpile(self, sql, read=None, write=None):
        """Translate an SQL statement or expression from dialect `read` to dialect `write`"""
        return sqlglot.transpile(sql, read=read, write=write)[0]


_session = Session()

_OPS = {
    "reset": _session.reset,
    "release": _session.release,
    "table": _session.table,
    "empty": _session.empty,
    "values": _session.values,
    "query": _session.query,
    "register_view": _session.register_view,
    "sql": _session.sql,
    "alias": _session.alias,
    "select": _session.select,
    "filter": _session.filter,
    "with_column": _session.with_column,
    "with_column_renamed": _session.with_column_renamed,
    "drop": _session.drop,
    "join": _session.join,
    "group_by_agg": _session.group_by_agg,
    "union_by_name": _session.union_by_name,
    "except": _session.except_,
    "distinct": _session.distinct,
    "drop_duplicates": _session.drop_duplicates,
    "order_by": _session.order_by,
    "limit": _session.limit,
    "schema": _session.schema,
    "to_sql": _session.to_sql,
    "create_table_as": _session.create_table_as,
    "create_table": _session.create_table,
    "alter_table": _session.alter_table,
    "parse_types": _session.parse_types,
    "transpile": _session.transpile,
}


def call(op, args_json):
    """Entry point for the JVM: execute operation `op` with the arguments given as JSON object"""
    try:
        fn = _OPS.get(op)
        if fn is None:
            raise ValueError(f"Unknown operation {op}")
        return json.dumps({"result": fn(**json.loads(args_json))})
    except Exception as e:
        return json.dumps({"error": f"{type(e).__name__}: {e}", "traceback": traceback.format_exc()})
