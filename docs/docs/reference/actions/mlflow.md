---
id: mlflow
title: MLflow Actions
---

MLflowTrainAction and MLflowPredictAction train a machine learning model and apply it to data, using
[MLflow](https://mlflow.org/) as the experiment tracker and model registry.
Both talk to MLflow through python, so no model artifact is ever stored by SDLB itself.

The two Actions share an **MLflowDataObject**, which holds the connection information and names the MLflow experiment.
The training Action names it as `outputMlflowId` and writes the information about its run into it, the prediction
Action names it as `inputMlflowId` and reads the model from it. That shared DataObject is what makes the prediction
Action depend on the training Action in the DAG.

Both are regular [DataFrame Actions](../executionEngines): their data side is a SparkSubFeed like any other Action,
and the MLflowDataObject is connected as an *additional* input respectively output which carries no DataFrame but
the key/values about the run as a [ParameterSubFeed](../executionEngines). They therefore support
[execution modes](../executionModes), [expectations](../dataQuality), save mode options, DataFrame caching and
transformers just like a CopyAction.

## Prerequisites

* a python environment reachable from the SDLB process, with `mlflow` installed, plus whatever your model code
  needs, e.g. `scikit-learn`. SDLB ships one as `sdl-spark/pyproject.toml`, to be created with
  [uv](https://docs.astral.sh/uv):

  ```bash
  cd sdl-spark && uv sync
  export PYSPARK_PYTHON=$PWD/.venv/bin/python
  ```

  SDLB starts the interpreter itself, through Spark's `PythonRunner`, which takes it from `PYSPARK_DRIVER_PYTHON`
  or `PYSPARK_PYTHON` and otherwise falls back to `python3` on the `PATH`. So either export that variable or
  activate the environment before running SDLB.
* MLflow **2.9 or newer** - the Actions address models by alias, and MLflow *stages* are deprecated since 2.9 and
  removed in MLflow 3
* somewhere for MLflow to keep its data. Either a tracking server, e.g. `mlflow server --host 127.0.0.1 --port 5000`,
  or - with no server at all - a local database, which is what `MLflowEndToEndTest` uses:

  ```
  trackingUri = "sqlite:///path/to/mlflow.db"
  artifactLocation = "file:///path/to/artifacts"
  ```

  Set `artifactLocation` with a database backend, otherwise MLflow puts the artifacts in `./mlruns` relative to the
  working directory. MLflow's file backend (`./mlruns` as `trackingUri`) is **not** an option: since MLflow 3 it
  raises unless `MLFLOW_ALLOW_FILE_STORE=true` is set, and MLflow recommends a database backend instead.

These Actions run on the classic Spark engine only. `mlflow.pyfunc.spark_udf` needs a Spark session inside the SDLB
process, so they cannot be used with Spark Connect or Snowpark.

## Example

```
dataObjects {
  int-listings { ... }

  mlflow-price-model {
    type = MLflowDataObject
    trackingUri = "http://localhost:5000"
    experimentName = "price-prediction"
  }

  int-listings-predicted { ... }
}

actions {
  train-price-model {
    type = MLflowTrainAction
    inputId = int-listings
    outputMlflowId = mlflow-price-model
    modelName = "price-regressor"
    registerModel = true
    modelAlias = "champion"
    pythonModelCode = """
      from sklearn.linear_model import LinearRegression
      pdf = df.toPandas()
      LinearRegression().fit(pdf[["reviews_per_month", "minimum_nights"]], pdf["price"])
    """
  }

  predict-price {
    type = MLflowPredictAction
    inputId = int-listings
    inputMlflowId = mlflow-price-model
    outputId = int-listings-predicted
    modelName = "price-regressor"
    modelAlias = "champion"
    featureColumns = [reviews_per_month, minimum_nights]
  }
}
```

## MLflowTrainAction

The training data of `inputId` is available to the python code as the PySpark DataFrame `df`.
The code runs inside an MLflow run with `mlflow.autolog()` enabled, so for the supported frameworks the parameters,
metrics and the model are logged without any further code - the example above never calls MLflow explicitly.
Use `pythonModelFile` instead of `pythonModelCode` to keep the model code in its own `.py` file.

With `registerModel = true` the logged model is registered in the MLflow model registry under `modelName`, and
`modelAlias` sets an alias such as `champion` on the newly created version.

No model is trained in Init phase, so a [dry-run](../commandLine) creates no MLflow runs.

`outputId` is optional. Configure it to write the training data on to a DataObject, which makes the Action behave
like a CopyAction that also trains a model - useful to keep the exact data a model was trained on, and needed if you
want to apply `transformers` to prepare the features. Without it the Action only trains, and the MLflowDataObject is
its only output.

:::caution
`mlflow.spark.autolog` is disabled, because a model logged through it cannot call back into py4j from a Spark session
driven by the JVM. Spark ML models are therefore not autologged - log them explicitly with `mlflow.spark.log_model`.
:::

### Run information

After a successful run, the following key/values are passed on to the MLflowDataObject and to subsequent Actions:

| Key | Description |
| --- | ----------- |
| experimentId | id of the MLflow experiment |
| experimentName | name of the MLflow experiment, as configured on the MLflowDataObject |
| modelName | name of the model, as configured on the Action |
| runId | id of the MLflow run |
| runName | name of the MLflow run, as generated by MLflow |
| duration | duration of the run in seconds |
| date | start of the run as an ISO-8601 timestamp in UTC |
| artifactPath | path of the model within the run's artifacts |
| modelUri | uri of the logged model, e.g. `runs:/<runId>/model` |
| estimatorName | name of the estimator class, as recorded by autolog |

## MLflowPredictAction

The model is loaded with `mlflow.pyfunc.spark_udf` and applied to the DataFrame of `inputId`. The prediction is added
as an additional column - `prediction` by default, configurable with `predictionColumn` and `resultType` - and the
result is written to `outputId`.

By default all columns of the input are passed to the model. Set `featureColumns` to select and order them explicitly,
which is normally what you want once the input carries more than the model's features.

`transformers` are applied to the input before the model, so the features can be prepared in the same Action.

The model to apply is resolved in this order:

1. `modelUri`, if configured explicitly
2. `models:/{modelName}@{modelAlias}`
3. `models:/{modelName}/{modelVersion}`
4. the model of the latest run of the experiment - this is the model just trained by an MLflowTrainAction in the
   same job, and it also works for a model that was never registered

The model is not loaded in Init phase, so a dry-run neither contacts MLflow nor downloads any model artifact.

### Environment of the model

`envManager` on the MLflowDataObject decides how MLflow restores the environment the model was trained in:

* `local` (default) - the environment running SDLB must already satisfy the model's requirements. Fast, and the right
  choice when the model was trained by an MLflowTrainAction in the same pipeline, or when SDLB runs in a container
  built for the model.
* `virtualenv` / `conda` - MLflow builds an environment on every executor. Use it when the model was trained
  somewhere else, e.g. in a data scientist's notebook, and you cannot guarantee that the environment running SDLB
  matches. It is considerably slower.

`local` is also MLflow's own default for `spark_udf`, and it makes MLflow log this warning on every prediction:

```
WARNING mlflow.pyfunc: Calling `spark_udf()` with `env_manager="local"` does not recreate the same environment
that was used during training, which may lead to errors or inaccurate predictions. ...
```

It is unconditional and says nothing about your model. What *does* say something is that MLflow compares the model's
pip requirements against the current environment just before it, and logs a second, specific warning per mismatched
package. If you see only the generic warning above and no mismatch warnings, the environment satisfies the model and
`local` is doing its job. Switch to `virtualenv` if mismatches are reported.

### Serialization of the model

`mlflow.autolog()` stores scikit-learn models with `cloudpickle`, which makes MLflow log:

```
WARNING mlflow.sklearn: Saving scikit-learn models in the pickle or cloudpickle format requires exercising caution
because these formats rely on Python's object serialization mechanism, which can execute arbitrary code during
deserialization. The recommended safe alternative is the 'skops' format. ...
```

This is autolog's default, not a choice of SDLB. Note what it implies for MLflowPredictAction: loading a pickled
model **executes code from the model artifact**, on the driver and on every Spark executor. The MLflow tracking
server and model registry are therefore part of your trust boundary - only apply models from a registry you control.

To store a model in the safer `skops` format instead, override autolog or log the model explicitly in your
`pythonModelCode`, which runs after SDLB has called `mlflow.autolog()`:

```python
import mlflow.sklearn
mlflow.sklearn.autolog(serialization_format="skops")
```

Note that `skops` cannot serialize custom functions or classes that are not defined at the top level; MLflow raises
an explicit error in that case, and cloudpickle remains the fallback.

## Limitations

Neither Action supports [simulation runs](../testing): a simulation must not train a model or execute python.

MLflowTrainAction without an `outputId` has no DataFrame output, so the MLflowDataObject becomes its main output.
Execution modes which compare the main input against the main output - `PartitionDiffMode`, `DataFrameIncrementalMode` -
then have nothing to compare against and need `alternativeOutputId` to point at a real DataObject. Configuring
`outputId` avoids this.

Streaming execution modes are not supported, as the python model code and `spark_udf` need a batch DataFrame.

A model applied with `spark_udf` is downloaded into a temporary directory which has to survive the python process, as
the DataFrame is only evaluated later in the JVM. These directories are not cleaned up automatically; remove them
periodically if SDLB runs long-lived outside a container.

## See also

* [Actions overview](../actions.md)
* [Transformations](../transformations) for running python code without MLflow
* [Execution Engines](../executionEngines)
