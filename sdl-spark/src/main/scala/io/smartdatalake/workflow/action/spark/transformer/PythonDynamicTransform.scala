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
package io.smartdatalake.workflow.action.spark.transformer

/**
 * Python code to call a transform function defined by the users Python code dynamically, mapping its parameters by
 * name to the input DataFrames and options.
 *
 * This is the Python counterpart of the dynamic transform method of the Scala transformer interfaces, see
 * [[io.smartdatalake.workflow.action.generic.customlogic.DynamicTransform]]. If the Python code does not define a
 * function named `transform`, nothing happens and the output DataFrame(s) have to be set with
 * `setOutputDf`/`setOutputDfs` as before.
 */
private[smartdatalake] object PythonDynamicTransform {

  /**
   * Helper functions to map the parameters of a Python transform function.
   * Parameters are mapped as follows:
   * - `options` gets the options of the transformation as dict
   * - `dfs` or `inputDfs` gets all input DataFrames as dict
   * - a parameter with the name of an input DataFrame gets this DataFrame. A `df` prefix is stripped from the
   *   parameter name before the lookup, and the lookup is case-insensitive and ignores `-` and `_`.
   * - a parameter with the name of an option gets its value, converted according to the type annotation or the type
   *   of its default value.
   * - if there is only one input DataFrame and no parameter was mapped to it, it is mapped to the first parameter
   *   without a default value.
   * - remaining parameters use their default value, otherwise an error is raised.
   */
  private val parameterMappingCode =
    """
      |import inspect as _sdlb_inspect
      |
      |def _sdlb_tolerant_key(key):
      |    return key.lower().replace('-','').replace('_','')
      |
      |def _sdlb_convert_option(value, param):
      |    annotation = param.annotation
      |    default = param.default
      |    empty = _sdlb_inspect.Parameter.empty
      |    if annotation is bool or (annotation is empty and isinstance(default, bool)):
      |        return value.strip().lower() == 'true'
      |    if annotation is int or (annotation is empty and isinstance(default, int) and not isinstance(default, bool)):
      |        return int(value)
      |    if annotation is float or (annotation is empty and isinstance(default, float)):
      |        return float(value)
      |    if annotation is list or (annotation is empty and isinstance(default, list)):
      |        return [x.strip() for x in value.split(',') if x.strip()]
      |    return value
      |
      |def _sdlb_map_parameters(fn, dfs, options, single_input):
      |    empty = _sdlb_inspect.Parameter.empty
      |    tolerant_dfs = dict((_sdlb_tolerant_key(k), v) for k, v in dfs.items())
      |    args = {}
      |    unmapped_required = []
      |    df_mapped = False
      |    for name, param in _sdlb_inspect.signature(fn).parameters.items():
      |        lookup = name[2:] if name.startswith('df') and len(name) > 2 else name
      |        if name in ('dfs', 'inputDfs'):
      |            args[name] = dfs
      |        elif name == 'options':
      |            args[name] = options
      |        elif _sdlb_tolerant_key(lookup) in tolerant_dfs:
      |            args[name] = tolerant_dfs[_sdlb_tolerant_key(lookup)]
      |            df_mapped = True
      |        elif single_input and name in ('df', 'inputDf'):
      |            args[name] = list(dfs.values())[0]
      |            df_mapped = True
      |        elif name in options:
      |            args[name] = _sdlb_convert_option(options[name], param)
      |        elif param.default is not empty:
      |            pass
      |        else:
      |            unmapped_required.append(name)
      |    # if there is only one input DataFrame and it was not mapped yet, map it to the first required parameter
      |    if single_input and not df_mapped and len(unmapped_required) > 0:
      |        args[unmapped_required[0]] = list(dfs.values())[0]
      |        unmapped_required = unmapped_required[1:]
      |    if len(unmapped_required) > 0:
      |        raise ValueError("No value found for parameter(s) %s of transform function. DataFrames available are %s, options available are %s"
      |            % (unmapped_required, list(dfs.keys()), list(options.keys())))
      |    return args
      |
      |def _sdlb_has_transform_function():
      |    fn = globals().get('transform')
      |    return fn is not None and callable(fn)
      |""".stripMargin

  /**
   * Code to append after the users Python code for a 1:1 transformation.
   * Expects the variables `inputDf`, `dataObjectId` and `options` and the function `setOutputDf` to be defined.
   */
  val dfPostludeCode: String =
    parameterMappingCode +
      """
        |if _sdlb_has_transform_function():
        |    _sdlb_dfs = {dataObjectId: inputDf}
        |    _sdlb_options = dict(options)
        |    _sdlb_options['dataObjectId'] = dataObjectId
        |    _sdlb_args = _sdlb_map_parameters(transform, _sdlb_dfs, _sdlb_options, True)
        |    _sdlb_result = transform(**_sdlb_args)
        |    if _sdlb_result is None:
        |        raise ValueError("Python transform function must return a DataFrame")
        |    setOutputDf(_sdlb_result)
        |""".stripMargin

  /**
   * Code to append after the users Python code for a n:m transformation.
   * Expects the variables `inputDfs` and `options` and the function `setOutputDfs` to be defined.
   */
  val dfsPostludeCode: String =
    parameterMappingCode +
      """
        |if _sdlb_has_transform_function():
        |    _sdlb_args = _sdlb_map_parameters(transform, inputDfs, options, len(inputDfs) == 1)
        |    _sdlb_result = transform(**_sdlb_args)
        |    if _sdlb_result is None:
        |        raise ValueError("Python transform function must return a DataFrame or a dict of DataFrames")
        |    if isinstance(_sdlb_result, dict):
        |        setOutputDfs(_sdlb_result)
        |    elif 'outputDataObjectId' in options:
        |        setOutputDfs({options['outputDataObjectId']: _sdlb_result})
        |    else:
        |        raise ValueError("Python transform function returned a single DataFrame, but outputDataObjectId is ambiguous."
        |            + " Modify Action to have only one outputIds entry, or return a dict of DataFrames from your transform function.")
        |""".stripMargin
}
