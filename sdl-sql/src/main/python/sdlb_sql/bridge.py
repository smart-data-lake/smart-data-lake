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

Column expressions are passed as Spark SQL text (SQLGlot dialect databricks, i.e. Spark SQL with ANSI casts), they
are created by the Scala class SQLColumn.

Tables of the database are registered with their schema under a placeholder name `__sdlb_t<n>`, so that the SQLGlot
schema needs no nesting of catalogs and databases. The placeholders are replaced by the real table names when
rendering the SQL statement.

Identifiers are resolved case-insensitively, unless they are quoted in a dialect where quoted identifiers are case
sensitive, e.g. `"Name"` in postgres. For this, all identifiers of the ASTs are normalized to lower case, and their
spelling is kept separately:
- The spelling of an identifier as written is stored in its meta `sdlb_spelling`.
- The columns of database tables are registered with their spelling in the database.
- When a statement is rendered, every identifier gets a form `(spelling, kind)` in its meta `sdlb_form`: a column
  reference gets the form of the column it references, i.e. the spelling of the database for a table column, and a
  new name, e.g. an alias, gets its spelling. The kind decides how the identifier is quoted, see `_quote`: a column
  of the database is quoted if the database would not resolve it unquoted, a new name is quoted only if it contains
  special characters or is a reserved word, so that the database normalizes its case as usual.
If the session is case sensitive, see `set_case_sensitive`, identifiers are not normalized, resolved exactly and
always quoted.

There is a single entry point for the JVM: `call(op, args_json)`, taking the name of an operation and its arguments as
JSON object, and returning a JSON object with either `result` or `error` and `traceback`.
"""

import json
import re
import traceback
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.errors import OptimizeError
from sqlglot.optimizer import optimize
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.optimizer import RULES
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.optimizer.simplify import simplify
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.schema import MappingSchema

# canonicalize is left out: it rewrites expressions based on the types inferred by SQLGlot, e.g. it removes casts it
# considers redundant, but the database might infer different types (e.g. DECIMAL instead of DOUBLE for 1.5).
_OPTIMIZER_RULES = tuple(rule for rule in RULES if rule is not canonicalize)

# dialect of the column expressions created by SQLColumn: Spark SQL with ANSI casts. The spark dialect of SQLGlot
# parses CAST as TRY_CAST.
_COLUMN_DIALECT = "databricks"

_PLACEHOLDER_PREFIX = "__sdlb_t"
_INPUT_PLACEHOLDER_PREFIX = "__sdlb_in"
_ROW_NUMBER_COLUMN = "__sdlb_rn"

# meta keys of identifiers, see module documentation
_SPELLING = "sdlb_spelling"
_CASE_SENSITIVE = "sdlb_case_sensitive"
_FORM = "sdlb_form"

# kinds of identifier forms, see Session._quote
_KIND_DB = "db"  # a column of the database, with the spelling of the database
_KIND_NEW = "new"  # a new name, e.g. an alias
_KIND_EXACT = "exact"  # a new name quoted in a case-sensitive dialect

# reserved words of standard SQL which are no valid unquoted identifiers in most databases
_RESERVED_WORDS = {
    "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST", "CHECK", "COLUMN", "CONSTRAINT",
    "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DELETE",
    "DESC", "DISTINCT", "DROP", "ELSE", "END", "EXCEPT", "EXISTS", "FALSE", "FETCH", "FOR", "FOREIGN", "FROM", "FULL",
    "GRANT", "GROUP", "HAVING", "IN", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT",
    "NATURAL", "NOT", "NULL", "OFFSET", "ON", "OR", "ORDER", "OUTER", "PRIMARY", "REFERENCES", "RIGHT", "SELECT",
    "SESSION_USER", "SET", "SOME", "TABLE", "THEN", "TO", "TRUE", "UNION", "UNIQUE", "UPDATE", "USER", "USING",
    "VALUES", "WHEN", "WHERE", "WINDOW", "WITH",
}

_CASE_INSENSITIVE_STRATEGIES = (NormalizationStrategy.CASE_INSENSITIVE, NormalizationStrategy.CASE_INSENSITIVE_UPPERCASE)

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
    # column names with their spelling, as reported to Scala
    columns: list
    # normalized column names, as used in expr
    keys: list
    # normalized alias
    alias: str
    # True if expr is a join whose inputs can still be referenced by their alias, e.g. `"_t1"."id"`.
    # Projections and filters are then applied to expr directly instead of wrapping it into a subquery, see
    # Session._in_join_scope.
    join_scope: bool = False

    def spelling(self, key):
        return self.columns[self.keys.index(key)]


def _col(key, table=None):
    return exp.column(key, table=table, quoted=True)


def _node_name(name):
    """Name of a lineage node or column without quotes, e.g. `a.x` for `"a"."x"`"""
    return name.replace('"', "")


def _is_simple_identifier(name):
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is not None


def _named_form(ident):
    """Form of an identifier naming something new, e.g. an alias, with its spelling as written"""
    return ident.meta.get(_SPELLING, ident.name), _KIND_EXACT if ident.meta.get(_CASE_SENSITIVE) else _KIND_NEW


def _is_qualifier(ident):
    """True if the identifier is a table alias or the table qualifier of a column. They are internal names, which are
    rendered with their normalized name, so that all references to a table alias are rendered the same way."""
    parent = ident.parent
    if isinstance(parent, exp.TableAlias):
        return parent.this is ident
    if isinstance(parent, exp.Column):
        return parent.this is not ident
    return isinstance(parent, exp.Table)


def _output_selects(node):
    """Projections defining the output columns of a query, i.e. of the leftmost select of a set operation"""
    while isinstance(node, (exp.SetOperation, exp.Subquery)):
        node = node.left if isinstance(node, exp.SetOperation) else node.this
    return node.selects if isinstance(node, exp.Select) else []


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
    if data_type.this == exp.DataType.Type.TIMESTAMPNTZ:
        # TIMESTAMP_NTZ of Spark is TIMESTAMP in the default dialect
        data_type = exp.DataType(this=exp.DataType.Type.TIMESTAMP, expressions=data_type.expressions)
    return {"type": data_type.sql()}


class Session:
    def __init__(self):
        self._case_sensitive = False
        self.reset()

    def reset(self):
        self._counter = 0
        self._dfs = {}
        self._tables = {}
        self._spellings = {}
        self._views = {}
        self._schema = MappingSchema(normalize=False)
        return None

    def set_case_sensitive(self, case_sensitive):
        """If true, identifiers are resolved exactly and always quoted, see Environment.caseSensitive"""
        self._case_sensitive = case_sensitive
        return None

    # identifiers

    def _key(self, name):
        """Normalized name of an identifier, used for resolution"""
        return name if self._case_sensitive else name.lower()

    def _ident(self, name):
        """Identifier for a new name, e.g. an alias, keeping its spelling"""
        ident = exp.to_identifier(self._key(name), quoted=True)
        ident.meta[_SPELLING] = name
        return ident

    def _alias(self, expression, name):
        return exp.alias_(expression, self._ident(name))

    def _normalize(self, expression, dialect):
        """Normalize all identifiers of a parsed expression, and keep their spelling. A quoted identifier of a dialect
        which resolves quoted identifiers case sensitive is marked as case sensitive."""
        exact = Dialect.get_or_raise(dialect).normalization_strategy not in _CASE_INSENSITIVE_STRATEGIES
        for ident in expression.find_all(exp.Identifier):
            ident.meta[_SPELLING] = ident.this
            if ident.quoted and exact:
                ident.meta[_CASE_SENSITIVE] = True
            ident.set("this", self._key(ident.this))
            ident.set("quoted", True)
        return expression

    def _parse(self, sql):
        return self._normalize(sqlglot.parse_one(sql, read=_COLUMN_DIALECT), _COLUMN_DIALECT)

    def _parse_ordered(self, sql):
        return self._normalize(exp.maybe_parse(sql, into=exp.Ordered, dialect=_COLUMN_DIALECT), _COLUMN_DIALECT)

    def _schema_columns(self, placeholder, columns):
        """Register the columns given as list of [name, type] for a placeholder table, and return their keys"""
        keys = [self._key(c) for c, _ in columns]
        duplicates = sorted({k for k in keys if keys.count(k) > 1})
        if duplicates:
            raise ValueError(f"Column names are ambiguous if resolved case-insensitively: {', '.join(duplicates)}")
        self._schema.add_table(placeholder, {k: exp.DataType.build(t, udt=True) for k, (_, t) in zip(keys, columns)})
        self._spellings[placeholder] = {k: c for k, (c, _) in zip(keys, columns)}
        return keys

    # resolution of spellings

    def _source_form(self, scope, column):
        """Form of a column reference, taken from the column of the source it references"""
        key, table = column.name, column.table
        source = None
        while scope is not None and source is None:
            source = scope.sources.get(table) if table else None
            if not table and isinstance(scope.expression, exp.Select):
                # a reference to a projection, e.g. in ORDER BY
                projection = next((p for p in scope.expression.selects if isinstance(p, exp.Alias) and p.alias == key), None)
                return projection.args["alias"].meta.get(_FORM) if projection is not None else None
            scope = scope.parent
        if isinstance(source, exp.Table):
            spelling = self._spellings.get(source.name, {}).get(key)
            return (spelling, _KIND_DB) if spelling is not None else None
        if isinstance(source, Scope):
            node = source.expression
            table_alias = node.args.get("alias") if isinstance(node, exp.Values) \
                else node.parent.args.get("alias") if isinstance(node.parent, (exp.Subquery, exp.CTE)) else None
            if isinstance(table_alias, exp.TableAlias) and table_alias.columns:
                ident = next((c for c in table_alias.columns if c.name == key), None)
                return (ident.meta.get(_FORM) or _named_form(ident)) if ident is not None else None
            for projection in _output_selects(node):
                if projection.alias_or_name == key:
                    ident = projection.args.get("alias") if isinstance(projection, exp.Alias) else projection.find(exp.Identifier)
                    return ident.meta.get(_FORM) if ident is not None else None
        return None

    def _resolve(self, expr, check=False):
        """Set the form of the column references and projection aliases of a qualified expression. If `check` is true,
        a column reference which is case sensitive must match the spelling of its column exactly.
        Returns the output columns of the expression as list of (key, spelling)."""
        for scope in traverse_scope(expr):
            columns = [c for c in scope.columns if isinstance(c.this, exp.Identifier)]
            for column in [c for c in columns if c.table]:
                form = self._source_form(scope, column)
                ident = column.this
                if check and not self._case_sensitive and ident.meta.get(_CASE_SENSITIVE) and form is not None \
                        and form[0] != ident.meta.get(_SPELLING):
                    raise ValueError(f"Column '{ident.meta.get(_SPELLING)}' could not be resolved, it is quoted and "
                                     f"therefore case sensitive, but the column is spelled '{form[0]}'")
                if form is not None:
                    ident.meta[_FORM] = form
            if isinstance(scope.expression, exp.Select):
                for projection in scope.expression.selects:
                    alias = projection.args.get("alias") if isinstance(projection, exp.Alias) else None
                    if alias is None or _FORM in alias.meta:
                        continue
                    inner = projection.this
                    if _SPELLING in alias.meta:
                        alias.meta[_FORM] = _named_form(alias)
                    elif isinstance(inner, exp.Column) and inner.name == alias.name and _FORM in inner.this.meta:
                        # a column passed through keeps its form
                        alias.meta[_FORM] = inner.this.meta[_FORM]
                    else:
                        alias.meta[_FORM] = (alias.name, _KIND_NEW)
            for column in [c for c in columns if not c.table]:
                form = self._source_form(scope, column)
                if form is not None:
                    column.this.meta[_FORM] = form
        result = []
        for projection in _output_selects(expr):
            ident = projection.args.get("alias") if isinstance(projection, exp.Alias) else projection.find(exp.Identifier)
            form = ident.meta.get(_FORM) if ident is not None else None
            result.append((projection.alias_or_name, form[0] if form else projection.alias_or_name))
        return result

    def _quote(self, spelling, kind, dialect):
        if self._case_sensitive or kind == _KIND_EXACT:
            return True
        if not _is_simple_identifier(spelling) or spelling.upper() in _RESERVED_WORDS:
            return True
        # a column of the database is quoted if the database would resolve it to a different spelling unquoted
        return kind == _KIND_DB and dialect.case_sensitive(spelling)

    def _apply_forms(self, expr, dialect):
        """Set spelling and quoting of all identifiers for rendering in `dialect`"""
        d = Dialect.get_or_raise(dialect)
        for ident in list(expr.find_all(exp.Identifier)):
            form = ident.meta.get(_FORM)
            if form is None:
                if _is_qualifier(ident) or isinstance(ident.parent, exp.Column):
                    form = (ident.name, _KIND_NEW)
                else:
                    form = _named_form(ident)
            spelling, kind = form
            ident.set("this", spelling)
            ident.set("quoted", self._quote(spelling, kind, d))
        return expr

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
        """Register a DataFrame. `columns` are its output columns as list of (key, spelling), they are resolved from
        `expr` if not given."""
        if columns is None:
            qualified = qualify(expr.copy(), schema=self._schema, validate_qualify_columns=True)
            columns = self._resolve(qualified, check=True)
        df_id = self._next_id()
        df = _DataFrame(expr, [s for _, s in columns], [k for k, _ in columns], alias or f"_t{df_id}", join_scope)
        self._dfs[df_id] = df
        return {"id": df_id, "columns": df.columns, "alias": df.alias}

    def _info(self, df_id):
        df = self._df(df_id)
        return {"id": df_id, "columns": df.columns, "alias": df.alias}

    @staticmethod
    def _columns(d):
        return list(zip(d.keys, d.columns))

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
        """Create a DataFrame reading the database table `name` with `columns` as list of [name, type], with names
        spelled as in the database"""
        placeholder = f"{_PLACEHOLDER_PREFIX}{self._next_id()}"
        real = exp.to_table(name, dialect=dialect)
        self._tables[placeholder] = real
        keys = self._schema_columns(placeholder, columns)
        alias = self._key(real.name)
        source = exp.Table(this=exp.to_identifier(placeholder), alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
        expr = exp.select(*[_col(k, alias) for k in keys]).from_(source)
        return self._register(expr, columns=list(zip(keys, [c for c, _ in columns])))

    def query(self, query, columns, dialect=None, alias="q"):
        """Create a DataFrame reading the result of the SQL query `query` in the dialect of the database, with `columns`
        as list of [name, type]. It is used for DataObjects defined by a query instead of a table."""
        placeholder = f"{_PLACEHOLDER_PREFIX}{self._next_id()}"
        parsed = sqlglot.parse_one(query, read=dialect)
        if not isinstance(parsed, exp.Query):
            raise ValueError(f"SQL statement must be a query, but is {type(parsed).__name__}: {query}")
        self._tables[placeholder] = parsed
        keys = self._schema_columns(placeholder, columns)
        alias = self._key(alias)
        source = exp.Table(this=exp.to_identifier(placeholder), alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
        expr = exp.select(*[_col(k, alias) for k in keys]).from_(source)
        return self._register(expr, columns=list(zip(keys, [c for c, _ in columns])))

    def empty(self, columns):
        """Create an empty DataFrame with `columns` as list of [name, type]"""
        projections = [self._alias(exp.cast(exp.null(), exp.DataType.build(t, udt=True)), c) for c, t in columns]
        expr = exp.select(*projections).where(exp.false())
        return self._register(expr, columns=[(self._key(c), c) for c, _ in columns])

    def values(self, rows, columns):
        """Create a DataFrame from `rows` given as list of lists of SQL literals, with `columns` as list of [name, type]"""
        if not rows:
            return self.empty(columns)
        alias = exp.TableAlias(this=exp.to_identifier("_v", quoted=True), columns=[self._ident(c) for c, _ in columns])
        source = exp.Values(expressions=[exp.Tuple(expressions=[self._parse(v) for v in row]) for row in rows], alias=alias)
        projections = [self._alias(exp.cast(_col(self._key(c), "_v"), exp.DataType.build(t, udt=True)), c) for c, t in columns]
        return self._register(exp.select(*projections).from_(source), columns=[(self._key(c), c) for c, _ in columns])

    def register_view(self, name, df):
        self._views[name.lower()] = self._df(df)
        return None

    def sql(self, query, dialect=None):
        """Parse the SQL query `query` in `dialect`, and replace the registered temporary views by their DataFrame"""
        parsed = sqlglot.parse_one(query, read=dialect)
        if not isinstance(parsed, exp.Query):
            raise ValueError(f"SQL statement must be a query, but is {type(parsed).__name__}: {query}")
        parsed = self._normalize(parsed, dialect)
        cte_names = {cte.alias_or_name.lower() for cte in parsed.find_all(exp.CTE)}
        for table in list(parsed.find_all(exp.Table)):
            if not isinstance(table.this, exp.Identifier):
                continue  # e.g. a table function
            view = self._views.get(table.name.lower()) if not table.db and not table.catalog else None
            if view is not None:
                alias = table.args.get("alias") or exp.TableAlias(this=table.this.copy())
                table.replace(exp.Subquery(this=view.expr.copy(), alias=alias.copy()))
            elif table.db or table.name.lower() not in cte_names:
                raise ValueError(f"Table or view not found: {'.'.join(p.meta.get(_SPELLING, p.name) for p in table.parts)}")
        return self._register(parsed)

    # transforming DataFrames

    def alias(self, df, alias):
        d = self._df(df)
        return self._register(d.expr, alias=self._key(alias), join_scope=d.join_scope, columns=self._columns(d))

    def select(self, df, columns):
        d = self._df(df)
        return self._in_join_scope(
            d,
            lambda: self._register(d.expr.copy().select(*[self._parse(c) for c in columns], append=False)),
            lambda: self._register(exp.select(*[self._parse(c) for c in columns]).from_(self._sub(d))))

    def filter(self, df, condition):
        d = self._df(df)
        return self._in_join_scope(
            d,
            lambda: self._register(d.expr.copy().where(self._parse(condition)), join_scope=True),
            lambda: self._register(exp.select("*").from_(self._sub(d)).where(self._parse(condition)), columns=self._columns(d)))

    def with_column(self, df, name, column):
        d = self._df(df)
        key = self._key(name)

        def direct():
            projection = self._alias(self._parse(column), name)
            existing = d.expr.expressions
            if key in d.keys:
                projections = [projection if p.alias_or_name == key else p.copy() for p in existing]
            else:
                projections = [p.copy() for p in existing] + [projection]
            return self._register(d.expr.copy().select(*projections, append=False), join_scope=True)

        def wrapped():
            projection = self._alias(self._parse(column), name)
            projections = [projection if k == key else _col(k) for k in d.keys]
            if key not in d.keys:
                projections.append(projection)
            return self._register(exp.select(*projections).from_(self._sub(d)))

        return self._in_join_scope(d, direct, wrapped)

    def with_column_renamed(self, df, name, new_name):
        d = self._df(df)
        key = self._key(name)
        if key not in d.keys:
            return self._info(df)
        projections = [self._alias(_col(k), new_name) if k == key else _col(k) for k in d.keys]
        return self._register(exp.select(*projections).from_(self._sub(d)))

    def drop(self, df, names):
        d = self._df(df)
        keys = {self._key(n) for n in names}
        remaining = [(k, s) for k, s in self._columns(d) if k not in keys]
        if len(remaining) == len(d.keys):
            return self._info(df)
        return self._register(exp.select(*[_col(k) for k, _ in remaining]).from_(self._sub(d)), columns=remaining)

    def join(self, df, other, how="inner", on=None, condition=None):
        left, right = self._df(df), self._df(other)
        if left.alias == right.alias:
            raise ValueError(f"Both sides of the join have the same alias '{left.alias}', use as(...) to give them distinct aliases")
        join_type = _JOIN_TYPES.get(how.lower())
        if join_type is None:
            raise ValueError(f"Unsupported join type {how}, supported are {', '.join(_JOIN_TYPES)}")
        if on is not None:
            on = [self._key(c) for c in on]
            for c in on:
                if c not in left.keys or c not in right.keys:
                    raise ValueError(f"Join column {c} does not exist on both sides of the join")
            cond = exp.and_(*[exp.EQ(this=_col(c, left.alias), expression=_col(c, right.alias)) for c in on]) if on else None
        else:
            cond = self._parse(condition) if condition else None
        if cond is None and join_type not in ("cross", "inner"):
            raise ValueError(f"Join type {how} needs join columns or a condition")
        if join_type in ("semi", "anti"):
            exists = exp.Exists(this=exp.select("1").from_(self._sub(right)).where(cond))
            expr = exp.select(*[_col(c, left.alias) for c in left.keys]).from_(self._sub(left)) \
                .where(exists if join_type == "semi" else exp.not_(exists))
            return self._register(expr, columns=self._columns(left))
        if on is not None:
            key_side = {"right": right}.get(join_type, left)
            keys = [self._alias(exp.Coalesce(this=_col(c, left.alias), expressions=[_col(c, right.alias)]), left.spelling(c))
                    if join_type == "full" else _col(c, key_side.alias) for c in on]
            projections = keys + [_col(c, left.alias) for c in left.keys if c not in on] \
                + [_col(c, right.alias) for c in right.keys if c not in on]
            columns = [(c, key_side.spelling(c)) for c in on] + [(k, s) for k, s in self._columns(left) if k not in on] \
                + [(k, s) for k, s in self._columns(right) if k not in on]
        else:
            projections = [_col(c, left.alias) for c in left.keys] + [_col(c, right.alias) for c in right.keys]
            columns = self._columns(left) + self._columns(right)
        join_type = "cross" if cond is None else join_type
        expr = exp.select(*projections).from_(self._sub(left)).join(self._sub(right), on=cond, join_type=join_type)
        return self._register(expr, join_scope=True, columns=columns)

    def group_by_agg(self, df, group_columns, aggregate_columns):
        d = self._df(df)
        groups = [self._parse(c) for c in group_columns]
        expr = exp.select(*groups, *[self._parse(c) for c in aggregate_columns]).from_(self._sub(d))
        if groups:
            expr = expr.group_by(*[g.unalias() for g in groups])
        return self._register(expr)

    def _select_columns(self, d, columns):
        return exp.select(*[_col(k) if k in d.keys else self._alias(exp.null(), s) for k, s in columns]).from_(self._sub(d))

    def union_by_name(self, df, other, allow_missing_columns=False):
        left, right = self._df(df), self._df(other)
        if not allow_missing_columns and set(left.keys) != set(right.keys):
            raise ValueError(f"unionByName needs the same columns on both sides, but got {left.columns} and {right.columns}")
        columns = self._columns(left) + [(k, s) for k, s in self._columns(right) if k not in left.keys]
        expr = exp.union(self._select_columns(left, columns), self._select_columns(right, columns), distinct=False)
        return self._register(expr, columns=columns)

    def except_(self, df, other):
        left, right = self._df(df), self._df(other)
        columns = self._columns(left)
        expr = exp.except_(self._select_columns(left, columns), self._select_columns(right, columns), distinct=True)
        return self._register(expr, columns=columns)

    def distinct(self, df):
        d = self._df(df)
        return self._register(exp.select("*").from_(self._sub(d)).distinct(), columns=self._columns(d))

    def drop_duplicates(self, df, columns):
        d = self._df(df)
        if not columns:
            return self.distinct(df)
        keys = [_col(self._key(c)) for c in columns]
        row_number = exp.Window(this=exp.RowNumber(), partition_by=keys,
                                order=exp.Order(expressions=[exp.Ordered(this=k.copy()) for k in keys]))
        inner = exp.select("*", row_number.as_(_ROW_NUMBER_COLUMN, quoted=True)).from_(self._sub(d)) \
            .subquery(exp.to_identifier("_dedup", quoted=True))
        expr = exp.select(*[_col(k) for k in d.keys]).from_(inner).where(exp.EQ(this=_col(_ROW_NUMBER_COLUMN), expression=exp.Literal.number(1)))
        return self._register(expr, columns=self._columns(d))

    def order_by(self, df, columns):
        d = self._df(df)
        return self._register(exp.select("*").from_(self._sub(d)).order_by(*[self._parse_ordered(c) for c in columns]), columns=self._columns(d))

    def limit(self, df, n):
        d = self._df(df)
        return self._register(exp.select("*").from_(self._sub(d)).limit(n), columns=self._columns(d))

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

    def _optimize(self, expr):
        """Optimize an expression, and resolve the forms of its identifiers. The projection aliases are resolved before
        optimizing, as the optimizer creates new aliases when merging subqueries."""
        expr = qualify(expr, schema=self._schema, validate_qualify_columns=False)
        self._resolve(expr)
        expr = optimize(expr, schema=self._schema, rules=_OPTIMIZER_RULES)
        self._resolve(expr)
        return expr

    def _render(self, expr, dialect, optimized=True):
        if optimized:
            expr = self._optimize(expr)
        else:
            self._resolve(expr)
        self._apply_forms(expr, dialect)
        return self._replace_placeholders(expr)

    def schema(self, df):
        """Return the fields of the DataFrame as list of {name, type} with type inferred by SQLGlot"""
        d = self._df(df)
        expr = optimize(d.expr.copy(), schema=self._schema, rules=_OPTIMIZER_RULES)
        return [{"name": name, "type": _type_json(s.type)} for name, s in zip(d.columns, expr.selects)]

    def to_sql(self, df, dialect=None, optimized=True, pretty=False):
        return self._render(self._df(df).expr.copy(), dialect, optimized).sql(dialect=dialect, pretty=pretty)

    def create_table_as(self, df, table, dialect=None, with_data=True):
        """Create a `CREATE TABLE <table> AS <query>` statement for the DataFrame. `table` is given in the dialect of the
        database. If `with_data` is false, the table is created empty. The names of new columns are rendered unquoted if
        possible, so that the database normalizes their case as for unquoted identifiers, see `_quote`."""
        d = self._df(df)
        expr = d.expr.copy()
        if not with_data:
            expr = exp.select("*").from_(expr.subquery(exp.to_identifier("_ctas", quoted=True))).where(exp.false())
        expr = self._render(expr, dialect)
        create = exp.Create(this=exp.to_table(table, dialect=dialect), kind="TABLE", expression=expr)
        return create.sql(dialect=dialect)

    def create_view(self, query, view, dialect=None, exists=False):
        """Create a statement creating or replacing a view, which keeps the grants on an existing view. `query` and
        `view` are given in the dialect of the database, e.g. the query rendered by `to_sql` for a DataFrame, which is
        exported by a dry-run to create the view later with CatalogSchemaUpdater.

        `CREATE OR REPLACE VIEW` keeps the grants for most databases, e.g. postgres, oracle and mysql, and tsql uses
        `CREATE OR ALTER VIEW`. Snowflake drops them unless `COPY GRANTS` is given, and Databricks unless an existing
        view is changed with `ALTER VIEW ... AS`."""
        dialect_name = (dialect or "").lower()
        target = exp.to_table(view, dialect=dialect)
        query_expr = sqlglot.parse_one(query, dialect=dialect)
        if exists and dialect_name in ("databricks", "spark", "hive"):
            return f"ALTER VIEW {target.sql(dialect=dialect)} AS {query_expr.sql(dialect=dialect)}"
        properties = exp.Properties(expressions=[exp.CopyGrantsProperty()]) if dialect_name == "snowflake" else None
        create = exp.Create(this=target, kind="VIEW", replace=True, expression=query_expr, properties=properties)
        return create.sql(dialect=dialect)

    # dialects supporting materialized views, and how an existing one is replaced: "recreate" drops and creates it
    # again, as the database has no `CREATE OR REPLACE MATERIALIZED VIEW`, "replace" uses the latter.
    _MATERIALIZED_VIEW_REPLACE = {"postgres": "recreate", "redshift": "recreate", "oracle": "recreate",
                                  "snowflake": "replace", "databricks": "replace"}

    def _materialized_view_dialect(self, dialect):
        dialect_name = (dialect or "").lower()
        if dialect_name not in self._MATERIALIZED_VIEW_REPLACE:
            raise ValueError(f"materialized views are not supported for SQL dialect '{dialect}', supported dialects are "
                             f"{', '.join(sorted(self._MATERIALIZED_VIEW_REPLACE))}")
        return dialect_name

    def check_materialized_view(self, dialect=None):
        """Raise an error if materialized views are not supported for the dialect"""
        self._materialized_view_dialect(dialect)
        return True

    def create_materialized_view(self, query, view, dialect=None, exists=False):
        """Create the statements creating or replacing a materialized view as {drop, create}. `drop` is None unless an
        existing materialized view must be dropped first, because the database has no `CREATE OR REPLACE`, e.g.
        postgres and oracle. The caller must then keep its grants. Snowflake keeps them with `COPY GRANTS`.

        The statement is assembled as text, as SQLGlot drops the schema of a materialized view for snowflake."""
        dialect_name = self._materialized_view_dialect(dialect)
        target = exp.to_table(view, dialect=dialect).sql(dialect=dialect)
        query_sql = sqlglot.parse_one(query, dialect=dialect).sql(dialect=dialect)
        if self._MATERIALIZED_VIEW_REPLACE[dialect_name] == "replace":
            copy_grants = " COPY GRANTS" if dialect_name == "snowflake" else ""
            return {"drop": None, "create": f"CREATE OR REPLACE MATERIALIZED VIEW {target}{copy_grants} AS {query_sql}"}
        drop = f"DROP MATERIALIZED VIEW {target}" if exists else None
        return {"drop": drop, "create": f"CREATE MATERIALIZED VIEW {target} AS {query_sql}"}

    def refresh_materialized_view(self, view, dialect=None):
        """Create the statement refreshing a materialized view, or None if the database refreshes it automatically,
        i.e. snowflake."""
        dialect_name = self._materialized_view_dialect(dialect)
        target = exp.to_table(view, dialect=dialect).sql(dialect=dialect)
        if dialect_name == "snowflake":
            return None
        if dialect_name == "oracle":
            return f"BEGIN DBMS_MVIEW.REFRESH('{target.replace(chr(39), chr(39) * 2)}'); END;"
        return f"REFRESH MATERIALIZED VIEW {target}"

    def normalize_query(self, query, dialect=None):
        """Normalize the query of a view, to compare the definition of an existing view with a new one. `query` can
        also be a `CREATE VIEW` statement, as some databases return the definition of a view like that. Databases
        reformat the query of a view, so identifiers are normalized and unquoted if the quotes don't matter, and the
        expression is simplified, e.g. to remove superfluous parentheses. Note that some databases rewrite the query
        more, e.g. add casts, then the definitions are not equal."""
        d = Dialect.get_or_raise(dialect)
        expr = sqlglot.parse_one(query, dialect=dialect)
        if isinstance(expr, exp.Create):
            expr = expr.expression
        expr = normalize_identifiers(expr, dialect=dialect)
        for identifier in expr.find_all(exp.Identifier):
            if identifier.quoted and d.normalize_identifier(exp.to_identifier(identifier.name)).name == identifier.name:
                identifier.set("quoted", False)
        return simplify(expr, dialect=dialect).sql(dialect=dialect)

    def _column_identifier(self, name, kind, dialect, quote_names):
        quoted = quote_names or self._quote(name, kind, Dialect.get_or_raise(dialect))
        return exp.to_identifier(name, quoted=quoted)

    def create_table(self, table, columns, dialect=None, quote_names=False):
        """Create a `CREATE TABLE` statement with `columns` as list of [name, type, nullable]"""
        column_defs = [
            exp.ColumnDef(this=self._column_identifier(name, _KIND_NEW, dialect, quote_names),
                          kind=exp.DataType.build(tpe, udt=True),
                          constraints=[] if nullable else [exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())])
            for name, tpe, nullable in columns
        ]
        create = exp.Create(this=exp.Schema(this=exp.to_table(table, dialect=dialect), expressions=column_defs), kind="TABLE")
        return create.sql(dialect=dialect)

    def alter_table(self, table, changes, dialect=None, quote_names=False):
        """Create `ALTER TABLE` statements for schema changes, given as list of objects with keys `change` (add, type
        or nullable), `column`, `type` (new type for add and type, current type for nullable) and `nullable`.
        The column of a type or nullable change is an existing column, given with the spelling of the database.
        SQLGlot renders them for most dialects. Changes of nullability are not supported by SQLGlot for some dialects,
        and are created here, as well as type changes for Oracle."""
        dialect_name = (dialect or "").lower()
        target = exp.to_table(table, dialect=dialect).sql(dialect=dialect)
        statements = []
        for change in changes:
            name = change["column"]
            kind = change["change"]
            identifier = self._column_identifier(name, _KIND_NEW if kind == "add" else _KIND_DB, dialect, quote_names)
            column = identifier.sql(dialect=dialect)
            data_type = exp.DataType.build(change["type"], udt=True) if change.get("type") else None
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

    def column_lineage(self, df, inputs, max_description_length=200):
        """Column level lineage of the DataFrame with respect to the input DataFrames, given as list of
        [dataObjectId, df]. The output DataFrame contains a copy of the query of each input DataFrame it is created
        from, which is replaced by a placeholder table per input before following the lineage with SQLGlot.

        Only DIRECT lineage is reported, like the other engines of SDLB: columns used only in a join, filter, group by,
        sort or window partition condition are not reported as input of a column.
        Returns an object with `fields` (column, inputs as list of [dataObjectId, column, identity], description of
        the transformation and expression of a constant), `unresolved` (columns which could not be traced back to an
        input), `dead_ends` per unresolved column, `inputs` with the columns of the inputs and the ones not used,
        and the `plan`, i.e. the SQL the lineage was read from.
        Lineage is followed with the normalized column names, the columns reported have the spelling of their
        DataFrame, while descriptions and the plan use the normalized names."""
        d = self._df(df)
        schema = MappingSchema(self._schema.mapping, normalize=False)
        # placeholder per distinct input query, an input can belong to more than one DataObject
        input_placeholders = {}
        placeholder_by_query = []
        for data_object_id, input_df in inputs:
            input_expr = self._df(input_df).expr
            placeholder = next((p for q, p in placeholder_by_query if q == input_expr), None)
            if placeholder is None:
                placeholder = f"{_INPUT_PLACEHOLDER_PREFIX}{len(placeholder_by_query)}"
                placeholder_by_query.append((input_expr, placeholder))
                schema.add_table(placeholder, {k: exp.DataType.build("UNKNOWN") for k in self._df(input_df).keys})
            input_placeholders.setdefault(placeholder, []).append(data_object_id)
        input_dfs = {data_object_id: self._df(input_df) for data_object_id, input_df in inputs}
        if not inputs:
            return {"fields": [], "unresolved": [], "dead_ends": {}, "inputs": [], "plan": []}

        # replace the copies of the input queries by their placeholder
        root = exp.select("*").from_(d.expr.copy().subquery(exp.to_identifier("_lineage", quoted=True)))
        for subquery in list(root.find_all(exp.Subquery)):
            if subquery.parent is None and subquery is not root:
                continue  # detached by a previous replacement
            placeholder = next((p for q, p in placeholder_by_query if subquery.this == q), None)
            if placeholder is not None:
                alias = subquery.args.get("alias") or exp.to_identifier(placeholder)
                if not isinstance(alias, exp.TableAlias):
                    alias = exp.TableAlias(this=alias)
                subquery.replace(exp.Table(this=exp.to_identifier(placeholder), alias=alias.copy()))
        expr = optimize(root, schema=schema, rules=_OPTIMIZER_RULES)

        def describe(expression):
            description = expression.unalias().sql(normalize_functions="lower")
            if len(description) > max_description_length:
                description = description[:max_description_length - 3] + "..."
            return description

        def direct_column_names(expression):
            # columns of window partitions and orderings are not part of the value, they are INDIRECT lineage
            expression = expression.copy()
            for window in list(expression.find_all(exp.Window)):
                window.set("partition_by", None)
                window.set("order", None)
            return {_node_name(c.sql()) for c in expression.find_all(exp.Column)}

        fields, unresolved, dead_ends, used_input_columns = [], [], {}, set()
        for key, column in sorted(self._columns(d), key=lambda c: c[1]):
            input_fields = {}
            column_dead_ends = []
            description = None
            constant_expression = None

            def walk(node, path, identity):
                nonlocal description, constant_expression
                path = path + [node.name]
                source = node.source
                if not node.downstream:
                    if isinstance(source, exp.Table) and source.name in input_placeholders:
                        input_key = _node_name(node.name).split(".")[-1]
                        for data_object_id in input_placeholders[source.name]:
                            input_df = input_dfs[data_object_id]
                            input_column = input_df.spelling(input_key) if input_key in input_df.keys else input_key
                            item = (data_object_id, input_column)
                            input_fields[item] = input_fields.get(item, True) and identity
                            used_input_columns.add(item)
                    elif isinstance(node.expression, exp.Table) or not isinstance(source, (exp.Select, exp.SetOperation)) \
                            or next(node.expression.find_all(exp.Column), None) is not None:
                        # a column of a source which is not an input
                        column_dead_ends.append({"attribute": node.name, "path": path, "producedBy": type(source).__name__,
                                                 "producedByNode": source.sql()[:max_description_length]})
                    elif constant_expression is None:
                        # an expression without columns, e.g. a constant or count(*)
                        constant_expression = describe(node.expression)
                    return
                expression = node.expression
                is_identity = isinstance(expression.unalias(), exp.Column)
                if not is_identity and description is None:
                    description = describe(expression)
                direct = direct_column_names(expression) if isinstance(expression, exp.Expression) else set()
                children = [child for child in node.downstream
                            # only children for column references are filtered, other children are e.g. the branches of a union
                            if "." not in child.name or _node_name(child.name) in direct]
                if not children and constant_expression is None:
                    # only INDIRECT input columns, e.g. count(*) over a window
                    constant_expression = describe(expression)
                for child in children:
                    walk(child, path, identity and is_identity)

            try:
                # the column is given as quoted identifier, otherwise SQLGlot normalizes its name to lower case
                node = sqlglot_lineage(exp.column(exp.to_identifier(key, quoted=True)), expr, schema=schema)
                walk(node, [], True)
            except Exception as e:
                column_dead_ends.append({"attribute": column, "path": [column], "producedBy": type(e).__name__, "producedByNode": str(e)[:max_description_length]})
            if column_dead_ends:
                unresolved.append(column)
                dead_ends[column] = column_dead_ends
            else:
                inputs_list = [[k[0], k[1], identity] for k, identity in sorted(input_fields.items())]
                expression = constant_expression if not inputs_list else None
                fields.append({"column": column, "inputs": inputs_list,
                               "description": description if any(not i[2] for i in inputs_list) else None, "expression": expression})
        inputs_info = [{"dataObjectId": data_object_id, "columns": input_df.columns,
                        "columnsNotInPlan": [c for c in input_df.columns if (data_object_id, c) not in used_input_columns]}
                       for data_object_id, input_df in input_dfs.items()]
        return {"fields": fields, "unresolved": unresolved, "dead_ends": dead_ends, "inputs": inputs_info,
                "plan": expr.sql(pretty=True).splitlines()}

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
    "set_case_sensitive": _session.set_case_sensitive,
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
    "create_view": _session.create_view,
    "normalize_query": _session.normalize_query,
    "check_materialized_view": _session.check_materialized_view,
    "create_materialized_view": _session.create_materialized_view,
    "refresh_materialized_view": _session.refresh_materialized_view,
    "alter_table": _session.alter_table,
    "parse_types": _session.parse_types,
    "column_lineage": _session.column_lineage,
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
