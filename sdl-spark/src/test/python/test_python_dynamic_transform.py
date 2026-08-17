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

"""Test the Python dynamic parameter mapping code shipped in PythonDynamicTransform.scala.

Extracts the Scala string literals, strips the stripMargin '|' prefixes and executes them
against a fake DataFrame/entrypoint environment.
"""
import re
import sys

SCALA_FILE = sys.argv[1]

src = open(SCALA_FILE).read()


def extract(val_name):
    # find the val definition and take everything until the closing triple quote of the last block
    idx = src.index(val_name)
    rest = src[idx:]
    blocks = re.findall(r'"""(.*?)"""', rest, re.S)
    assert blocks, "no code block found for " + val_name
    return blocks


def strip_margin(block):
    lines = []
    for line in block.split("\n"):
        stripped = line.lstrip()
        if stripped.startswith("|"):
            lines.append(stripped[1:])
        elif stripped == "":
            lines.append("")
        else:
            lines.append(line)
    return "\n".join(lines)


mapping_code = strip_margin(extract("private val parameterMappingCode")[0])
df_postlude = mapping_code + "\n" + strip_margin(extract("val dfPostludeCode")[0])
dfs_postlude = mapping_code + "\n" + strip_margin(extract("val dfsPostludeCode")[0])

failures = []


def check(name, cond):
    if cond:
        print("ok   - " + name)
    else:
        failures.append(name)
        print("FAIL - " + name)


class FakeDf:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "FakeDf(%s)" % self.name


# --- 1:1 case: parameter names not matching the DataObjectId
env = {}
out = {}
env["inputDf"] = FakeDf("src")
env["dataObjectId"] = "src"
env["options"] = {"factor": "3"}
env["setOutputDf"] = lambda df: out.__setitem__("df", df)
code = "def transform(myFrame, factor: int = 2):\n    assert factor == 3, factor\n    return myFrame\n"
exec(code + "\n" + df_postlude, env)
check("1:1 binds single input regardless of name, converts int option", out.get("df") is env["inputDf"])

# --- 1:1 case: parameter named df + dataObjectId + default option
env = {}
out = {}
env["inputDf"] = FakeDf("src")
env["dataObjectId"] = "src"
env["options"] = {}
env["setOutputDf"] = lambda df: out.__setitem__("df", df)
code = "def transform(df, dataObjectId, flag: bool = True):\n    assert dataObjectId == 'src', dataObjectId\n    assert flag is True\n    return df\n"
exec(code + "\n" + df_postlude, env)
check("1:1 binds df, dataObjectId option and default value", out.get("df") is env["inputDf"])

# --- 1:1 case: no transform function defined -> classic behaviour, nothing happens
env = {}
out = {}
env["inputDf"] = FakeDf("src")
env["dataObjectId"] = "src"
env["options"] = {}
env["setOutputDf"] = lambda df: out.__setitem__("df", df)
code = "setOutputDf(inputDf)\n"
exec(code + "\n" + df_postlude, env)
check("1:1 without transform function keeps setOutputDf behaviour", out.get("df") is env["inputDf"])

# --- n:m case: DataFrames by name, tolerant matching, df prefix
env = {}
out = {}
dfa, dfb = FakeDf("a"), FakeDf("b")
env["inputDfs"] = {"src-a": dfa, "src_b": dfb}
env["options"] = {"factor": "2"}
env["setOutputDfs"] = lambda dfs: out.__setitem__("dfs", dfs)
code = ("def transform(dfSrcA, dfSrcB, factor: int = 1):\n"
        "    assert factor == 2, factor\n"
        "    return {'tgt': dfSrcB}\n")
exec(code + "\n" + dfs_postlude, env)
check("n:m maps DataFrames by tolerant name with df prefix", out.get("dfs") == {"tgt": dfb})

# --- n:m case: dfs and options parameters
env = {}
out = {}
env["inputDfs"] = {"src": dfa}
env["options"] = {"outputDataObjectId": "tgt"}
env["setOutputDfs"] = lambda dfs: out.__setitem__("dfs", dfs)
code = ("def transform(dfs, options):\n"
        "    assert set(dfs.keys()) == {'src'}\n"
        "    assert options['outputDataObjectId'] == 'tgt'\n"
        "    return dfs['src']\n")
exec(code + "\n" + dfs_postlude, env)
check("n:m maps dfs/options and single return uses outputDataObjectId", out.get("dfs") == {"tgt": dfa})

# --- n:m case: missing parameter raises a helpful error
env = {}
env["inputDfs"] = {"src": dfa, "src2": dfb}
env["options"] = {}
env["setOutputDfs"] = lambda dfs: None
code = "def transform(dfUnknown):\n    return {}\n"
try:
    exec(code + "\n" + dfs_postlude, env)
    check("n:m missing parameter raises error", False)
except ValueError as e:
    check("n:m missing parameter raises error", "dfUnknown" in str(e))

# --- n:m case: list and float option conversion
env = {}
out = {}
env["inputDfs"] = {"src": dfa}
env["options"] = {"names": "a, b ,c", "ratio": "1.5", "flag": "true"}
env["setOutputDfs"] = lambda dfs: out.__setitem__("dfs", dfs)
code = ("def transform(dfSrc, names: list = [], ratio: float = 0.0, flag: bool = False):\n"
        "    assert names == ['a','b','c'], names\n"
        "    assert ratio == 1.5, ratio\n"
        "    assert flag is True\n"
        "    return {'tgt': dfSrc}\n")
exec(code + "\n" + dfs_postlude, env)
check("n:m converts list, float and bool options", out.get("dfs") == {"tgt": dfa})

print()
if failures:
    print("%d test(s) FAILED: %s" % (len(failures), failures))
    sys.exit(1)
print("all python dynamic transform tests passed")
