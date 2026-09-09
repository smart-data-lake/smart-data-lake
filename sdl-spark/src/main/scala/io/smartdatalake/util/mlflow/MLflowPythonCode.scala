/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.smartdatalake.util.mlflow

/**
 * Python code executed by [[MLflowPythonUtil]] to talk to MLflow.
 *
 * Note that no configuration value is interpolated into this code. Everything is read from the `options` dict, which
 * py4j provides from [[io.smartdatalake.util.spark.PythonSparkEntryPoint.getOptions]]. This keeps the code static and
 * therefore testable, and avoids quoting problems with values like a model name containing a quote.
 *
 * The only exception is the user's model code, which [[MLflowPythonUtil.train]] appends to [[trainPreludeCode]].
 *
 * The code assumes the variables provided by [[io.smartdatalake.util.spark.PythonUtil]]'s init code, namely `gateway`,
 * `entryPoint`, `session`, `sqlContext`, `options` and `DataFrame`.
 */
private[smartdatalake] object MLflowPythonCode {

  /**
   * Connects to MLflow and defines the helper functions used by all other snippets.
   */
  val preludeCode: String =
    """
      |import mlflow
      |
      |class SDLBMLflowError(Exception):
      |    pass
      |
      |mlflow.set_tracking_uri(options['trackingUri'])
      |if options.get('registryUri'):
      |    mlflow.set_registry_uri(options['registryUri'])
      |_sdlb_client = mlflow.MlflowClient()
      |
      |def _sdlb_to_java_map(d):
      |    # py4j does not convert a python dict to a java Map on its own
      |    java_map = gateway.jvm.java.util.HashMap()
      |    for k, v in d.items():
      |        java_map[k] = '' if v is None else str(v)
      |    return java_map
      |
      |def _sdlb_get_or_create_experiment(name):
      |    experiment = mlflow.get_experiment_by_name(name)
      |    if experiment is not None:
      |        return experiment.experiment_id
      |    return mlflow.create_experiment(name)
      |
      |def _sdlb_model_info(run):
      |    '''Return (artifact_path, model_uri) of the model logged by the given run.'''
      |    import json
      |    tags = run.data.tags or {}
      |    history_json = tags.get('mlflow.log-model.history')
      |    if history_json:
      |        history = json.loads(history_json)
      |        if history:
      |            artifact_path = history[0].get('artifact_path') or ''
      |            return artifact_path, 'runs:/{}/{}'.format(run.info.run_id, artifact_path)
      |    # MLflow 3 logs models as LoggedModel linked to the run through its outputs
      |    outputs = getattr(run, 'outputs', None)
      |    model_outputs = getattr(outputs, 'model_outputs', None) if outputs is not None else None
      |    if model_outputs:
      |        return '', 'models:/{}'.format(model_outputs[0].model_id)
      |    raise SDLBMLflowError(
      |        'MLflow run {} did not log a model. Make sure your model code logs one, either by leaving '
      |        'mlflow.autolog() enabled for a supported framework or by calling mlflow.<flavor>.log_model(...) '
      |        'explicitly.'.format(run.info.run_id))
      |
      |def _sdlb_collect_run_info(run):
      |    from datetime import datetime, timezone
      |    artifact_path, model_uri = _sdlb_model_info(run)
      |    start_time = int(run.info.start_time or 0)
      |    end_time = int(run.info.end_time or start_time)
      |    return {
      |        'experimentId': run.info.experiment_id,
      |        'experimentName': options['experimentName'],
      |        'modelName': options.get('modelName', ''),
      |        'runId': run.info.run_id,
      |        'runName': run.info.run_name,
      |        'duration': (end_time - start_time) / 1000.0,
      |        'date': datetime.fromtimestamp(start_time / 1000.0, tz=timezone.utc).isoformat(),
      |        'artifactPath': artifact_path,
      |        'modelUri': model_uri,
      |        'estimatorName': (run.data.tags or {}).get('estimator_name', ''),
      |    }
      |
      |def _sdlb_register_model(model_uri, model_name, description, alias):
      |    if not model_uri:
      |        raise SDLBMLflowError('Cannot register model {}: the run did not produce a model uri.'.format(model_name))
      |    if len(_sdlb_client.search_registered_models(filter_string="name = '{}'".format(model_name))) == 0:
      |        _sdlb_client.create_registered_model(name=model_name, description=description or '')
      |    elif description:
      |        _sdlb_client.update_registered_model(name=model_name, description=description)
      |    model_version = mlflow.register_model(model_uri=model_uri, name=model_name)
      |    print('MLflow: registered model {} as version {}'.format(model_name, model_version.version))
      |    if alias:
      |        _sdlb_client.set_registered_model_alias(name=model_name, alias=alias, version=model_version.version)
      |        print('MLflow: model {} version {} got alias {}'.format(model_name, model_version.version, alias))
      |    return model_version.version
      |""".stripMargin

  /**
   * Resolves the experiment id, creating the experiment if it does not exist yet.
   */
  val getOrCreateExperimentCode: String =
    """
      |entryPoint.setResults(_sdlb_to_java_map({'experimentId': _sdlb_get_or_create_experiment(options['experimentName'])}))
      |""".stripMargin

  /**
   * Starts an MLflow run and provides the training DataFrame as `df`. The user's model code is appended to this.
   */
  val trainPreludeCode: String =
    """
      |mlflow.set_experiment(experiment_id=_sdlb_get_or_create_experiment(options['experimentName']))
      |try:
      |    # autolog for spark models does not work, because java cannot call back into py4j again
      |    mlflow.spark.autolog(disable=True)
      |except Exception as e:
      |    print('MLflow: could not disable spark autolog ({}), continuing'.format(e))
      |mlflow.autolog()
      |# the training DataFrame provided by SDLB
      |df = DataFrame(entryPoint.getInputDf(), sqlContext)
      |mlflow.start_run()
      |run = mlflow.active_run()
      |print('MLflow: run started (run_id={})'.format(run.info.run_id))
      |""".stripMargin

  /**
   * Ends the run, collects its information and optionally registers the model.
   */
  val trainPostludeCode: String =
    """
      |mlflow.end_run()
      |run = mlflow.get_run(run.info.run_id)
      |print('MLflow: run finished (run_id={} | status={})'.format(run.info.run_id, run.info.status))
      |run_info = _sdlb_collect_run_info(run)
      |if options.get('registerModel') == 'true':
      |    _sdlb_register_model(run_info['modelUri'], options['modelName'],
      |                         options.get('modelDescription'), options.get('modelAlias'))
      |entryPoint.setResults(_sdlb_to_java_map(run_info))
      |""".stripMargin

  /**
   * Loads the model and applies it to the input DataFrame.
   */
  val predictCode: String =
    """
      |import atexit
      |import shutil
      |from pyspark.sql.functions import col, struct
      |
      |model_uri = options['modelUri']
      |feature_columns = [c for c in options['featureColumns'].split(',') if c]
      |print('MLflow: loading model {}'.format(model_uri))
      |predict_udf = mlflow.pyfunc.spark_udf(session, model_uri=model_uri, result_type=options['resultType'],
      |                                      env_manager=options['envManager'])
      |df = DataFrame(entryPoint.getInputDf(), sqlContext)
      |df_predict = df.withColumn(options['predictionColumn'], predict_udf(struct(*[col(c) for c in feature_columns])))
      |# The model is downloaded into a temporary directory which python removes in an atexit hook. The DataFrame is
      |# only evaluated later in the JVM, so that directory has to survive this python process.
      |atexit.unregister(shutil.rmtree)
      |entryPoint.setOutputDf(df_predict._jdf)
      |""".stripMargin

  /**
   * Reads the information of the latest run of the experiment, if there is one.
   */
  val getLatestRunInfoCode: String =
    """
      |from mlflow.entities import ViewType
      |experiment = mlflow.get_experiment_by_name(options['experimentName'])
      |if experiment is not None:
      |    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], filter_string='',
      |                              run_view_type=ViewType.ACTIVE_ONLY, max_results=1,
      |                              order_by=['start_time DESC'], output_format='list')
      |    if runs:
      |        entryPoint.setResults(_sdlb_to_java_map(_sdlb_collect_run_info(runs[0])))
      |""".stripMargin
}
