---
id: dataQuality
title: Data Quality
---

Data quality is an important topic in data governance. To monitoring and improvement data quality, building data pipelines with implementing data quality measures is an important measure.

SDLB provides the following features to improve data quality:
- Runtime **metrics** to monitor data pipeline output and track it over time
- Row-level **constraints** to stop before writing wrong data to an output
- **Expectations** on dataset level to stop or warn on unplausible data

## Metrics
Every SDLB job collects metrics for each Action and output-DataObject. They are logged with the following log statements:

`2020-07-21 11:36:34 INFO  CopyAction:105 - (Action~a) finished writing to DataObject~tgt1: job_duration=PT0.906S records_written=1 bytes_written=1142 num_tasks=1 stage=save`

Metrics are also stored in the state file, and if you want to sync them to monitoring system in real-time, the StateListener can be implemented. It gets notified about action new events and metrics as soon as they are available. To configure state listeners set config attribute `global.stateListeners = [{className = ...}]`.

## Constraints
Constraints can be defined on DataObjects to validate data on row-level. They work similar as database constraints and are validated when by an Action when writing data into a DataObject. If a constraints validation fails, an Exception is thrown and the Action stops. No further data is written to the DataObject, and if the DataObject implements transactional write (Hive, DeltaLake, Jdbc, ...), no data at all is stored in the output DataObject.

To define a constraint an arbitrary SQL expression is evaluated for each row, if it returns false the constraint validation fails. To return a meaningful error message you should configure a useful name and the columns that should be included in the text. See the following example:
```
dataObjects {
  testDataObject {
    ...
    constraints [{
      name = A shold be smaller than B
      description = "If A is bigger than B we have a problem because of ..."
      expression = "a < b"
      errorMsgCols = [id, a, b]
    }]
}
```

## Expectations
Expectations can be defined on DataObjects to monitor and validate a dataset after it has been written to the DataObject. An expectation collects a custom metric and compares its result against a given expectation condition. If the condition fails a warning can be logged or an error created which stops further processing. 

Using the `type = SQLExpectation`, a simple aggregation SQL expression is evaluated over the dataset. Further, an arbitrary SQL expression can be configured as expectation condition, which is compared against the metric value. If no expectation condition is given, the custom metric value is just logged in the `finished writing to DataObject~xyz:...` log message, see example in [Metrics](#metrics) section.

SDLB supports other expectation types, see [Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html) for a list.

By default, the expectation is evaluated against the currently processed dataset (scope=Job), which may consist of multiple partition values. Using the option `scope=JobPartition`, the scope can be changed to evalute against *each* partition value. The option `scope=All` would take all data in the output DataObject into account. Note that expectations with scope!=Job need reading the data from the output again after it has been written, while expectations with scope=Job can be calculated on the fly when using Spark as execution engine.


```
dataObjects {
  testDataObject {
    ...
    expectations = [{
      type = SQLExpectation
      name = NoDataPct
      description = "percentage of records having no data should be less than 0.1"
      aggExpression = "count(data) / count(*)"
      expectation = "< 0.1"
    }
}
```