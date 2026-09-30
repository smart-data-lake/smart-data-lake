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

"""Tests for the SQLGlot DataFrame bridge of the SDLB SQL engine, run with `uv run pytest` in sdl-sql."""

import json

import pytest

from sdlb_sql import bridge


class BridgeError(Exception):
    pass


def call(op, **args):
    response = json.loads(bridge.call(op, json.dumps(args)))
    if "error" in response:
        raise BridgeError(response["error"])
    return response["result"]


@pytest.fixture(autouse=True)
def reset():
    call("set_case_sensitive", case_sensitive=False)
    call("reset")


def table(name="db.test_table", columns=(("a", "INT"), ("b", "INT"), ("c", "TEXT"))):
    return call("table", name=name, columns=[list(c) for c in columns])


def to_sql(df, dialect=None, **kwargs):
    return call("to_sql", df=df["id"], dialect=dialect, **kwargs)


def test_table_uses_real_table_name():
    df = table()
    assert df["columns"] == ["a", "b", "c"]
    assert to_sql(df) == 'SELECT test_table.a AS a, test_table.b AS b, test_table.c AS c FROM db.test_table AS test_table'


def test_transformations_are_merged_by_optimizer():
    # the example of issue #866 with sqlframe
    df = table()
    df = call("with_column", df=df["id"], name="d", column='a * 2')
    call("register_view", name="test_table_int", df=df["id"])
    df = call("sql", query="select *, d * 2 as e from test_table_int", dialect="spark")
    df = call("with_column", df=df["id"], name="x", column='e * 2')
    assert df["columns"] == ["a", "b", "c", "d", "e", "x"]
    assert to_sql(df, "postgres") == (
        'SELECT test_table.a AS a, test_table.b AS b, test_table.c AS c, '
        'test_table.a * 2 AS d, test_table.a * 4 AS e, test_table.a * 8 AS x '
        'FROM db.test_table AS test_table')


def test_unoptimized_sql_keeps_subqueries():
    df = call("filter", df=table()["id"], condition='a > 1')
    assert to_sql(df, optimized=False) == (
        'SELECT * FROM (SELECT test_table.a, test_table.b, test_table.c FROM db.test_table AS test_table) '
        'AS _t2 WHERE a > 1')


def test_dialects():
    df = call("limit", df=table()["id"], n=3)
    assert to_sql(df, "tsql").startswith("SELECT TOP 3 test_table.a AS a")
    assert to_sql(df, "postgres").endswith("LIMIT 3")


def test_sql_translates_dialect():
    call("register_view", name="v", df=table()["id"])
    df = call("sql", query="select top 3 a from v", dialect="tsql")
    assert to_sql(df, "postgres") == 'SELECT test_table.a AS a FROM db.test_table AS test_table LIMIT 3'


def test_sql_unknown_table():
    with pytest.raises(BridgeError, match="Table or view not found: nope"):
        call("sql", query="select * from nope")


def test_sql_with_cte():
    call("register_view", name="v", df=table()["id"])
    df = call("sql", query="with x as (select a from v) select a from x")
    assert df["columns"] == ["a"]


def test_unknown_column():
    with pytest.raises(BridgeError, match="could not be resolved"):
        call("select", df=table()["id"], columns=['unknown'])


def test_select_drop_rename():
    df = table()
    assert call("select", df=df["id"], columns=['a', 'b + 1 AS b1'])["columns"] == ["a", "b1"]
    assert call("drop", df=df["id"], names=["b"])["columns"] == ["a", "c"]
    assert call("drop", df=df["id"], names=["x"])["id"] == df["id"]
    assert call("with_column_renamed", df=df["id"], name="a", new_name="z")["columns"] == ["z", "b", "c"]
    assert call("with_column", df=df["id"], name="a", column='b')["columns"] == ["a", "b", "c"]


def test_join_on_columns():
    left, right = table(), table("other", (("a", "INT"), ("z", "INT")))
    df = call("join", df=left["id"], other=right["id"], how="full_outer", on=["a"])
    assert df["columns"] == ["a", "b", "c", "z"]
    assert 'COALESCE(test_table.a, other.a) AS a' in to_sql(df)
    assert "FULL JOIN" in to_sql(df)


def test_join_condition_and_select_by_alias():
    left = call("alias", df=table()["id"], alias="l")
    right = call("alias", df=table("other", (("a", "INT"), ("z", "INT")))["id"], alias="r")
    df = call("join", df=left["id"], other=right["id"], how="left", condition='l.a = r.a')
    df = call("filter", df=df["id"], condition='r.z > 0')
    df = call("select", df=df["id"], columns=['l.a', 'r.z'])
    assert df["columns"] == ["a", "z"]
    assert to_sql(df) == (
        'SELECT test_table.a AS a, other.z AS z FROM db.test_table AS test_table '
        'LEFT JOIN other AS other ON other.a = test_table.a WHERE other.z > 0')


def test_join_same_alias_fails():
    df = table()
    with pytest.raises(BridgeError, match="same alias"):
        call("join", df=df["id"], other=df["id"], on=["a"])


def test_semi_join():
    left, right = table(), table("other", (("a", "INT"), ("z", "INT")))
    df = call("join", df=left["id"], other=right["id"], how="left_semi", on=["a"])
    assert df["columns"] == ["a", "b", "c"]


def test_group_by_and_schema():
    df = call("group_by_agg", df=table()["id"], group_columns=['c'], aggregate_columns=['COUNT(*) AS cnt', 'MAX(a) AS m'])
    assert call("schema", df=df["id"]) == [
        {"name": "c", "type": {"type": "TEXT"}},
        {"name": "cnt", "type": {"type": "BIGINT"}},
        {"name": "m", "type": {"type": "INT"}},
    ]
    assert to_sql(df).endswith('GROUP BY test_table.c')


def test_nested_schema():
    df = table(columns=(("s", "STRUCT<x INT, y ARRAY<TEXT>>"), ("m", "MAP<TEXT, INT>")))
    assert call("schema", df=df["id"]) == [
        {"name": "s", "type": {"struct": [{"name": "x", "type": {"type": "INT"}}, {"name": "y", "type": {"array": {"type": "TEXT"}}}]}},
        {"name": "m", "type": {"map": [{"type": "TEXT"}, {"type": "INT"}]}},
    ]


def test_union_by_name():
    left, right = table(), table("other", (("b", "INT"), ("a", "INT")))
    with pytest.raises(BridgeError, match="same columns"):
        call("union_by_name", df=left["id"], other=right["id"])
    df = call("union_by_name", df=left["id"], other=right["id"], allow_missing_columns=True)
    assert df["columns"] == ["a", "b", "c"]
    assert 'UNION ALL SELECT other.a AS a, other.b AS b, NULL AS c' in to_sql(df)


def test_except_distinct_order_by():
    df = table()
    assert " EXCEPT " in to_sql(call("except", df=df["id"], other=df["id"]))
    assert to_sql(call("distinct", df=df["id"])).startswith("SELECT DISTINCT")
    assert to_sql(call("order_by", df=df["id"], columns=['a DESC'])).endswith('ORDER BY test_table.a DESC')


def test_drop_duplicates():
    df = call("drop_duplicates", df=table()["id"], columns=["a"])
    assert df["columns"] == ["a", "b", "c"]
    assert "ROW_NUMBER() OVER (PARTITION BY" in to_sql(df)


def test_empty():
    df = call("empty", columns=[["a", "INT"]])
    assert to_sql(df, "postgres") == 'SELECT CAST(NULL AS INT) AS a WHERE FALSE'


def test_release():
    df = table()
    call("release", ids=[df["id"]])
    with pytest.raises(BridgeError, match="not found"):
        to_sql(df)


def test_unknown_operation():
    with pytest.raises(BridgeError, match="Unknown operation"):
        call("nope")


def test_transpile():
    assert call("transpile", sql="SELECT TOP 1 a FROM x", read="tsql", write="postgres") == "SELECT a FROM x LIMIT 1"


def test_values():
    df = call("values", rows=[["1", "'a'"], ["2", "NULL"]], columns=[["num", "INT"], ["str", "TEXT"]])
    assert df["columns"] == ["num", "str"]
    assert to_sql(df, "tsql") == (
        "SELECT CAST(_v.num AS INTEGER) AS num, CAST(_v.str AS VARCHAR(MAX)) AS str "
        "FROM (VALUES (1, 'a'), (2, NULL)) AS _v(num, str)")
    assert call("schema", df=df["id"]) == [{"name": "num", "type": {"type": "INT"}}, {"name": "str", "type": {"type": "TEXT"}}]


def test_values_keep_casts():
    # SQLGlot considers the cast of 1.5 to DOUBLE redundant, but databases might type 1.5 as DECIMAL
    df = call("values", rows=[["1.5"]], columns=[["d", "DOUBLE"]])
    assert "CAST(" in to_sql(df, "duckdb")


def test_unqualified_join_column_after_join_on_columns():
    left, right = table(), table("other", (("a", "INT"), ("z", "INT")))
    df = call("join", df=left["id"], other=right["id"], how="inner", on=["a"])
    df = call("filter", df=df["id"], condition='a > 1')
    df = call("select", df=df["id"], columns=['a', 'z'])
    assert df["columns"] == ["a", "z"]
    assert to_sql(df).startswith('SELECT test_table.a AS a, other.z AS z')


def test_query():
    df = call("query", query="select top 1 a from db.x", columns=[["a", "INT"]], dialect="tsql")
    df = call("filter", df=df["id"], condition='a > 1')
    assert to_sql(df, "postgres") == 'SELECT q.a AS a FROM (SELECT a FROM db.x LIMIT 1) AS q WHERE q.a > 1'


def test_create_table_as():
    df = call("with_column", df=table()["id"], name="My Col", column='a * 2')
    assert call("create_table_as", df=df["id"], table="db.tgt", dialect="postgres") == (
        'CREATE TABLE db.tgt AS SELECT test_table.a AS a, test_table.b AS b, test_table.c AS c, '
        'test_table.a * 2 AS "My Col" FROM db.test_table AS test_table')
    assert call("create_table_as", df=df["id"], table="db.tgt", dialect="tsql", with_data=False).startswith("SELECT ")
    assert call("create_table_as", df=df["id"], table="db.tgt", dialect="postgres", with_data=False).endswith("WHERE FALSE")


def test_create_table():
    assert call("create_table", table="db.tgt", columns=[["a", "INT", False], ["My Col", "TEXT", True]], dialect="tsql") == \
        "CREATE TABLE db.tgt (a INTEGER NOT NULL, [My Col] VARCHAR(MAX))"


def test_parse_types():
    assert call("parse_types", types=[["int4", 10, 0], ["numeric", 10, 2], ["varchar", 20, 0], ["INTEGER[]", 0, 0], ["geometry", 0, 0]], dialect="postgres") == [
        # database specific types are passed through
        {"type": "INT"}, {"type": "DECIMAL(10, 2)"}, {"type": "VARCHAR"}, {"array": {"type": "INT"}}, {"type": "GEOMETRY"}]


def test_alter_table():
    changes = [
        {"change": "add", "column": "c", "type": "DECIMAL(10, 2)"},
        {"change": "type", "column": "My Col", "type": "BIGINT"},
        {"change": "nullable", "column": "c", "type": "INT", "nullable": True},
    ]
    assert call("alter_table", table="db.t", changes=changes, dialect="duckdb") == [
        "ALTER TABLE db.t ADD COLUMN c DECIMAL(10, 2)",
        'ALTER TABLE db.t ALTER COLUMN "My Col" SET DATA TYPE BIGINT',
        "ALTER TABLE db.t ALTER COLUMN c DROP NOT NULL",
    ]
    assert call("alter_table", table="db.t", changes=changes, dialect="tsql") == [
        "ALTER TABLE db.t ADD c NUMERIC(10, 2)",
        "ALTER TABLE db.t ALTER COLUMN [My Col] BIGINT",
        "ALTER TABLE db.t ALTER COLUMN c INTEGER NULL",
    ]
    assert call("alter_table", table="db.t", changes=changes, dialect="oracle") == [
        "ALTER TABLE db.t ADD c NUMBER(10, 2)",
        'ALTER TABLE db.t MODIFY ("My Col" INT)',
        'ALTER TABLE db.t MODIFY ("c" NULL)',
    ]
    assert call("alter_table", table="db.t", changes=changes[2:], dialect="mysql") == ["ALTER TABLE db.t MODIFY COLUMN c INT NULL"]


def test_column_lineage():
    src = table()
    call("register_view", name="v", df=src["id"])
    df = call("sql", query="select a as id, upper(c) as c, 'k' as const, count(*) over (partition by b) as cnt from v", dialect="spark")
    result = call("column_lineage", df=df["id"], inputs=[["src1", src["id"]]])
    fields = {f["column"]: f for f in result["fields"]}
    assert fields["id"]["inputs"] == [["src1", "a", True]]
    assert fields["c"]["inputs"] == [["src1", "c", False]]
    assert fields["c"]["description"] == 'upper("v"."c")'
    # a constant and a window aggregation without direct input columns
    assert fields["const"]["inputs"] == [] and fields["const"]["expression"] == "'k'"
    assert fields["cnt"]["inputs"] == [] and "count(*)" in fields["cnt"]["expression"]
    assert result["unresolved"] == []
    assert result["inputs"] == [{"dataObjectId": "src1", "columns": ["a", "b", "c"], "columnsNotInPlan": ["b"]}]


def test_column_lineage_unresolved():
    src, other = table(), table("other", (("x", "INT"),))
    df = call("join", df=src["id"], other=other["id"], how="cross")
    result = call("column_lineage", df=df["id"], inputs=[["src1", src["id"]]])
    assert result["unresolved"] == ["x"]
    assert result["dead_ends"]["x"][0]["path"][-1] == result["dead_ends"]["x"][0]["attribute"]


def mixed_case_table():
    return table("db.Tab", (("Name", "TEXT"), ("CODE", "INT"), ("low", "INT"), ("My Col", "INT")))


def test_case_insensitive_resolution():
    df = mixed_case_table()
    assert df["columns"] == ["Name", "CODE", "low", "My Col"]
    # column references resolve case-insensitively, and keep the spelling of the database
    df = call("select", df=df["id"], columns=["name", "Code", "LOW", "`MY COL`"])
    assert df["columns"] == ["Name", "CODE", "low", "My Col"]
    assert call("schema", df=df["id"])[0] == {"name": "Name", "type": {"type": "TEXT"}}
    assert call("drop", df=df["id"], names=["NAME"])["columns"] == ["CODE", "low", "My Col"]
    assert call("with_column_renamed", df=df["id"], name="code", new_name="Id")["columns"] == ["Name", "Id", "low", "My Col"]


def test_ambiguous_columns():
    with pytest.raises(BridgeError, match="ambiguous"):
        table(columns=(("a", "INT"), ("A", "INT")))


def test_database_columns_are_quoted_only_if_needed():
    df = mixed_case_table()
    assert to_sql(df, "postgres") == \
        'SELECT tab."Name" AS "Name", tab."CODE" AS "CODE", tab.low AS low, tab."My Col" AS "My Col" FROM db.Tab AS tab'
    assert to_sql(df, "snowflake") == \
        'SELECT tab."Name" AS "Name", tab.CODE AS CODE, tab."low" AS "low", tab."My Col" AS "My Col" FROM db.Tab AS tab'
    assert to_sql(df, "duckdb") == \
        'SELECT tab.Name AS Name, tab.CODE AS CODE, tab.low AS low, tab."My Col" AS "My Col" FROM db.Tab AS tab'
    assert to_sql(df, "tsql") == \
        'SELECT tab.Name AS Name, tab.CODE AS CODE, tab.low AS low, tab.[My Col] AS [My Col] FROM db.Tab AS tab'


def test_new_names_are_unquoted_and_keep_spelling():
    df = call("select", df=mixed_case_table()["id"], columns=["code + 1 AS TownName", "low AS `Order`"])
    assert df["columns"] == ["TownName", "Order"]
    assert to_sql(df, "postgres") == 'SELECT tab."CODE" + 1 AS TownName, tab.low AS "Order" FROM db.Tab AS tab'
    assert to_sql(df, "snowflake") == 'SELECT tab.CODE + 1 AS TownName, tab."low" AS "Order" FROM db.Tab AS tab'


def test_spelling_is_kept_through_subqueries():
    df = call("with_column", df=mixed_case_table()["id"], name="Rank", column="row_number() over (order by name)")
    df = call("filter", df=df["id"], condition="rank = 1")
    df = call("select", df=df["id"], columns=["NAME", "RANK", "`my col`"])
    assert df["columns"] == ["Name", "Rank", "My Col"]
    # the window function prevents merging the subquery, references to it have the same spelling as its columns
    assert to_sql(df, "postgres") == (
        'WITH _t3 AS (SELECT tab."Name" AS "Name", tab."My Col" AS "My Col", ROW_NUMBER() OVER (ORDER BY tab."Name" NULLS FIRST) AS Rank '
        'FROM db.Tab AS tab) SELECT _t3."Name" AS "Name", _t3.Rank AS Rank, _t3."My Col" AS "My Col" FROM _t3 AS _t3 WHERE _t3.Rank = 1')


def test_quoted_identifiers_of_case_sensitive_dialects():
    call("register_view", name="v", df=mixed_case_table()["id"])
    # unquoted identifiers are case-insensitive
    assert call("sql", query="select NAME, code as Id from v", dialect="postgres")["columns"] == ["Name", "Id"]
    # quoted identifiers are case sensitive in postgres
    assert call("sql", query='select "Name" as "Big" from v', dialect="postgres")["columns"] == ["Big"]
    with pytest.raises(BridgeError, match="could not be resolved"):
        call("sql", query='select "name" from v', dialect="postgres")
    # but not in spark
    df = call("sql", query="select `NAME` as `Big` from v", dialect="spark")
    assert df["columns"] == ["Big"]
    assert to_sql(df, "postgres") == 'SELECT tab."Name" AS Big FROM db.Tab AS tab'
    # a quoted alias of a case-sensitive dialect stays quoted
    df = call("sql", query='select name as "Big" from v', dialect="postgres")
    assert to_sql(df, "snowflake") == 'SELECT tab."Name" AS "Big" FROM db.Tab AS tab'


def test_case_sensitive_session():
    call("set_case_sensitive", case_sensitive=True)
    df = mixed_case_table()
    with pytest.raises(BridgeError, match="could not be resolved"):
        call("select", df=df["id"], columns=["name"])
    df = call("select", df=df["id"], columns=["Name", "CODE AS x"])
    assert df["columns"] == ["Name", "x"]
    assert to_sql(df, "snowflake") == 'SELECT "Tab"."Name" AS "Name", "Tab"."CODE" AS "x" FROM db.Tab AS "Tab"'


def test_create_table_as_with_mixed_case():
    df = call("with_column", df=mixed_case_table()["id"], name="TownName", column="upper(name)")
    df = call("drop", df=df["id"], names=["my col", "low"])
    assert call("create_table_as", df=df["id"], table="db.tgt", dialect="postgres") == (
        'CREATE TABLE db.tgt AS SELECT tab."Name" AS "Name", tab."CODE" AS "CODE", UPPER(tab."Name") AS TownName FROM db.Tab AS tab')


def test_column_lineage_with_spelling():
    src = mixed_case_table()
    df = call("select", df=src["id"], columns=["name AS Id", "code"])
    result = call("column_lineage", df=df["id"], inputs=[["src1", src["id"]]])
    assert [(f["column"], f["inputs"]) for f in result["fields"]] == [("CODE", [["src1", "CODE", True]]), ("Id", [["src1", "Name", True]])]
    assert result["inputs"][0]["columnsNotInPlan"] == ["low", "My Col"]
