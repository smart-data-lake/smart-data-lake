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

"""Test the Python code shipped in MLflowPythonCode.scala.

Extracts the Scala string literals, strips the stripMargin '|' prefixes, checks that every snippet
compiles, and runs the prelude helpers against a fake mlflow module and a fake py4j entry point.
No real mlflow installation is needed.
"""
import re
import sys
import types

SCALA_FILE = sys.argv[1]

src = open(SCALA_FILE).read()


def extract(val_name):
    idx = src.index(val_name)
    rest = src[idx:]
    match = re.search(r'"""(.*?)"""', rest, re.S)
    assert match, "no code block found for " + val_name
    return strip_margin(match.group(1))


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


failures = []


def check(name, cond):
    if cond:
        print("ok   - " + name)
    else:
        failures.append(name)
        print("FAIL - " + name)


SNIPPETS = ["preludeCode", "getOrCreateExperimentCode", "trainPreludeCode", "trainPostludeCode",
            "predictCode", "getLatestRunInfoCode"]
code = {name: extract("val " + name + ":") for name in SNIPPETS}

# --- every snippet must be syntactically valid python
for name in SNIPPETS:
    try:
        compile(code[name], name, "exec")
        check("%s compiles" % name, True)
    except SyntaxError as e:
        check("%s compiles (%s)" % (name, e), False)


# --- fakes for the prelude helpers
class FakeJavaMap(dict):
    pass


class FakeJvm:
    class java:
        class util:
            HashMap = FakeJavaMap


class FakeGateway:
    jvm = FakeJvm


class FakeEntryPoint:
    def __init__(self):
        self.results = None

    def setResults(self, r):
        self.results = r


class FakeRunInfo:
    def __init__(self, run_id="run-1", experiment_id="7", run_name="tasteful-lamb",
                 start_time=1_700_000_000_000, end_time=1_700_000_012_500):
        self.run_id = run_id
        self.experiment_id = experiment_id
        self.run_name = run_name
        self.start_time = start_time
        self.end_time = end_time
        self.status = "FINISHED"


class FakeRunData:
    def __init__(self, tags):
        self.tags = tags


class FakeRun:
    def __init__(self, tags, outputs=None):
        self.info = FakeRunInfo()
        self.data = FakeRunData(tags)
        self.outputs = outputs


class FakeExperiment:
    def __init__(self, experiment_id):
        self.experiment_id = experiment_id


class FakeClient:
    def __init__(self):
        self.registered = []
        self.aliases = []

    def search_registered_models(self, filter_string=None):
        return list(self.registered)

    def create_registered_model(self, name, description=""):
        self.registered.append(name)

    def update_registered_model(self, name, description):
        pass

    def set_registered_model_alias(self, name, alias, version):
        self.aliases.append((name, alias, version))


class FakeModelVersion:
    version = "4"


def build_mlflow(existing_experiment=None):
    mlflow = types.ModuleType("mlflow")
    mlflow.created = []
    mlflow.registered = []

    def get_experiment_by_name(name):
        return FakeExperiment(existing_experiment) if existing_experiment else None

    def create_experiment(name, artifact_location=None):
        mlflow.created.append((name, artifact_location))
        return "42"

    def register_model(model_uri, name):
        mlflow.registered.append((model_uri, name))
        return FakeModelVersion()

    mlflow.set_tracking_uri = lambda uri: None
    mlflow.set_registry_uri = lambda uri: None
    mlflow.get_experiment_by_name = get_experiment_by_name
    mlflow.create_experiment = create_experiment
    mlflow.register_model = register_model
    mlflow.MlflowClient = FakeClient
    return mlflow


def run_prelude(options, existing_experiment=None):
    mlflow = build_mlflow(existing_experiment)
    entry_point = FakeEntryPoint()
    env = {"gateway": FakeGateway, "entryPoint": entry_point, "options": options}
    sys.modules["mlflow"] = mlflow
    exec(code["preludeCode"], env)
    return env, mlflow, entry_point


OPTIONS = {"trackingUri": "http://localhost:5000", "experimentName": "test-experiment",
           "modelName": "price-regressor", "envManager": "local"}

# --- experiment is created when it does not exist, reused otherwise
env, mlflow, _ = run_prelude(OPTIONS)
check("experiment created when missing", env["_sdlb_get_or_create_experiment"]("test-experiment") == "42")
check("create_experiment called", mlflow.created == [("test-experiment", None)])

env, mlflow, _ = run_prelude(OPTIONS, existing_experiment="13")
check("existing experiment reused", env["_sdlb_get_or_create_experiment"]("test-experiment") == "13")
check("create_experiment not called", mlflow.created == [])

# --- artifactLocation is passed on when the experiment is created
env, mlflow, _ = run_prelude(dict(OPTIONS, artifactLocation="file:///tmp/artifacts"))
env["_sdlb_get_or_create_experiment"]("test-experiment")
check("artifactLocation passed to create_experiment",
      mlflow.created == [("test-experiment", "file:///tmp/artifacts")])

# --- run info collected from the MLflow 2 log-model history tag
env, _, _ = run_prelude(OPTIONS)
run = FakeRun({"mlflow.log-model.history": '[{"artifact_path": "model"}]', "estimator_name": "LinearRegression"})
info = env["_sdlb_collect_run_info"](run)
check("runId collected", info["runId"] == "run-1")
check("experimentId collected", info["experimentId"] == "7")
check("experimentName from options", info["experimentName"] == "test-experiment")
check("modelName from options", info["modelName"] == "price-regressor")
check("modelUri built from run", info["modelUri"] == "runs:/run-1/model")
check("artifactPath collected", info["artifactPath"] == "model")
check("estimatorName collected", info["estimatorName"] == "LinearRegression")
check("duration in seconds", info["duration"] == 12.5)
check("date is iso utc", info["date"].startswith("2023-11-14T") and info["date"].endswith("+00:00"))

# every key of MLflowRunInfo.fields must be present
EXPECTED_KEYS = {"experimentId", "experimentName", "modelName", "runId", "runName", "duration", "date",
                 "artifactPath", "modelUri", "estimatorName"}
check("run info has all keys", set(info.keys()) == EXPECTED_KEYS)

# --- run info from an MLflow 3 logged model
class FakeModelOutput:
    model_id = "m-123"


class FakeOutputs:
    model_outputs = [FakeModelOutput()]


info3 = env["_sdlb_collect_run_info"](FakeRun({}, outputs=FakeOutputs()))
check("mlflow 3 model uri", info3["modelUri"] == "models:/m-123")

# --- a run without a model gives a helpful error
try:
    env["_sdlb_collect_run_info"](FakeRun({}))
    check("missing model raises", False)
except Exception as e:
    check("missing model raises", "did not log a model" in str(e))

# --- dict is converted to a java map with string values
java_map = env["_sdlb_to_java_map"]({"a": 1, "b": None})
check("java map converts values to string", java_map == {"a": "1", "b": ""})
check("java map is a jvm HashMap", isinstance(java_map, FakeJavaMap))

# --- register model creates the registered model and sets the alias
env, mlflow, _ = run_prelude(OPTIONS)
version = env["_sdlb_register_model"]("runs:/run-1/model", "price-regressor", "a model", "champion")
check("model registered", mlflow.registered == [("runs:/run-1/model", "price-regressor")])
check("alias set", env["_sdlb_client"].aliases == [("price-regressor", "champion", "4")])
check("version returned", version == "4")

try:
    env["_sdlb_register_model"]("", "price-regressor", None, None)
    check("register without model uri raises", False)
except Exception as e:
    check("register without model uri raises", "did not produce a model uri" in str(e))

if failures:
    print("%d check(s) failed: %s" % (len(failures), ", ".join(failures)))
    sys.exit(1)
print("all checks passed")
