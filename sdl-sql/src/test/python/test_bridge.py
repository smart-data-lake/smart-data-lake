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
    call("reset")


def table(name="db.test_table", columns=(("a", "INT"), ("b", "INT"), ("c", "TEXT"))):
    return call("table", name=name, columns=[list(c) for c in columns])


def to_sql(df, dialect=None, **kwargs):
    return call("to_sql", df=df["id"], dialect=dialect, **kwargs)


def test_table_uses_real_table_name():
    df = table()
    assert df["columns"] == ["a", "b", "c"]
    assert to_sql(df) == 'SELECT "test_table"."a" AS "a", "test_table"."b" AS "b", "test_table"."c" AS "c" FROM db.test_table AS "test_table"'


def test_transformations_are_merged_by_optimizer():
    # the example of issue #866 with sqlframe
    df = table()
    df = call("with_column", df=df["id"], name="d", column='"a" * 2')
    call("register_view", name="test_table_int", df=df["id"])
    df = call("sql", query="select *, d * 2 as e from test_table_int", dialect="spark")
    df = call("with_column", df=df["id"], name="x", column='"e" * 2')
    assert df["columns"] == ["a", "b", "c", "d", "e", "x"]
    assert to_sql(df, "postgres") == (
        'SELECT "test_table"."a" AS "a", "test_table"."b" AS "b", "test_table"."c" AS "c", '
        '"test_table"."a" * 2 AS "d", "test_table"."a" * 4 AS "e", "test_table"."a" * 8 AS "x" '
        'FROM db.test_table AS "test_table"')


def test_unoptimized_sql_keeps_subqueries():
    df = call("filter", df=table()["id"], condition='"a" > 1')
    assert to_sql(df, optimized=False) == (
        'SELECT * FROM (SELECT "test_table"."a", "test_table"."b", "test_table"."c" FROM db.test_table AS "test_table") '
        'AS "_t2" WHERE "a" > 1')


def test_dialects():
    df = call("limit", df=table()["id"], n=3)
    assert to_sql(df, "tsql").startswith("SELECT TOP 3 [test_table].[a] AS [a]")
    assert to_sql(df, "postgres").endswith("LIMIT 3")


def test_sql_translates_dialect():
    call("register_view", name="v", df=table()["id"])
    df = call("sql", query="select top 3 a from v", dialect="tsql")
    assert to_sql(df, "postgres") == 'SELECT "test_table"."a" AS "a" FROM db.test_table AS "test_table" LIMIT 3'


def test_sql_unknown_table():
    with pytest.raises(BridgeError, match="Table or view not found: nope"):
        call("sql", query="select * from nope")


def test_sql_with_cte():
    call("register_view", name="v", df=table()["id"])
    df = call("sql", query="with x as (select a from v) select a from x")
    assert df["columns"] == ["a"]


def test_unknown_column():
    with pytest.raises(BridgeError, match="could not be resolved"):
        call("select", df=table()["id"], columns=['"unknown"'])


def test_select_drop_rename():
    df = table()
    assert call("select", df=df["id"], columns=['"a"', '"b" + 1 AS "b1"'])["columns"] == ["a", "b1"]
    assert call("drop", df=df["id"], names=["b"])["columns"] == ["a", "c"]
    assert call("drop", df=df["id"], names=["x"])["id"] == df["id"]
    assert call("with_column_renamed", df=df["id"], name="a", new_name="z")["columns"] == ["z", "b", "c"]
    assert call("with_column", df=df["id"], name="a", column='"b"')["columns"] == ["a", "b", "c"]


def test_join_on_columns():
    left, right = table(), table("other", (("a", "INT"), ("z", "INT")))
    df = call("join", df=left["id"], other=right["id"], how="full_outer", on=["a"])
    assert df["columns"] == ["a", "b", "c", "z"]
    assert 'COALESCE("test_table"."a", "other"."a") AS "a"' in to_sql(df)
    assert "FULL JOIN" in to_sql(df)


def test_join_condition_and_select_by_alias():
    left = call("alias", df=table()["id"], alias="l")
    right = call("alias", df=table("other", (("a", "INT"), ("z", "INT")))["id"], alias="r")
    df = call("join", df=left["id"], other=right["id"], how="left", condition='"l"."a" = "r"."a"')
    df = call("filter", df=df["id"], condition='"r"."z" > 0')
    df = call("select", df=df["id"], columns=['"l"."a"', '"r"."z"'])
    assert df["columns"] == ["a", "z"]
    assert to_sql(df) == (
        'SELECT "test_table"."a" AS "a", "other"."z" AS "z" FROM db.test_table AS "test_table" '
        'LEFT JOIN other AS "other" ON "other"."a" = "test_table"."a" WHERE "other"."z" > 0')


def test_join_same_alias_fails():
    df = table()
    with pytest.raises(BridgeError, match="same alias"):
        call("join", df=df["id"], other=df["id"], on=["a"])


def test_semi_join():
    left, right = table(), table("other", (("a", "INT"), ("z", "INT")))
    df = call("join", df=left["id"], other=right["id"], how="left_semi", on=["a"])
    assert df["columns"] == ["a", "b", "c"]


def test_group_by_and_schema():
    df = call("group_by_agg", df=table()["id"], group_columns=['"c"'], aggregate_columns=['COUNT(*) AS "cnt"', 'MAX("a") AS "m"'])
    assert call("schema", df=df["id"]) == [
        {"name": "c", "type": {"type": "TEXT"}},
        {"name": "cnt", "type": {"type": "BIGINT"}},
        {"name": "m", "type": {"type": "INT"}},
    ]
    assert to_sql(df).endswith('GROUP BY "test_table"."c"')


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
    assert 'UNION ALL SELECT "other"."a" AS "a", "other"."b" AS "b", NULL AS "c"' in to_sql(df)


def test_except_distinct_order_by():
    df = table()
    assert " EXCEPT " in to_sql(call("except", df=df["id"], other=df["id"]))
    assert to_sql(call("distinct", df=df["id"])).startswith("SELECT DISTINCT")
    assert to_sql(call("order_by", df=df["id"], columns=['"a" DESC'])).endswith('ORDER BY "test_table"."a" DESC')


def test_drop_duplicates():
    df = call("drop_duplicates", df=table()["id"], columns=["a"])
    assert df["columns"] == ["a", "b", "c"]
    assert "ROW_NUMBER() OVER (PARTITION BY" in to_sql(df)


def test_empty():
    df = call("empty", columns=[["a", "INT"]])
    assert to_sql(df, "postgres") == 'SELECT CAST(NULL AS INT) AS "a" WHERE FALSE'


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
        "SELECT [_v].[num] AS [num], CAST([_v].[str] AS VARCHAR(MAX)) AS [str] "
        "FROM (VALUES (1, 'a'), (2, NULL)) AS [_v]([num], [str])")
    assert call("schema", df=df["id"]) == [{"name": "num", "type": {"type": "INT"}}, {"name": "str", "type": {"type": "TEXT"}}]
