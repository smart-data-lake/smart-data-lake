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
Every SDLB job collects metrics for each Action and DataObject written. They are logged with the following log statements:

`2020-07-21 11:36:34 INFO  CopyAction:105 - (Action~a) finished writing to DataObject~tgt1: job_duration=PT0.906S count=1 records_written=1 bytes_written=1142 num_tasks=1 stage=save`

`job_duration` is always recorded. For DataFrame based Actions, the number of records written is recorded as `count`, and the number of records read as `count#<dataObjectId>`. Further metrics are recorded depending on the DataObject type, e.g. `rows_inserted/updated/deleted` for merge statements. And it's possible to record custom metrics, see chapter "Expectations" below.

Metrics are also stored in the state file, and if you want to sync them to monitoring system in real-time, the StateListener can be implemented. It gets notified about action new events and metrics as soon as they are available. To configure state listeners set config attribute `global.stateListeners = [{className = ...}]`.

## Constraints
Constraints can be defined on DataObjects to validate data on row-level. They work similar as database constraints and are validated by an Action when writing data into a DataObject. If a constraints validation fails, an Exception is thrown and the Action stops. No further data is written to the DataObject, and if the DataObject implements transactional write (Hive, DeltaLake, Jdbc, ...), no data at all is stored in the output DataObject.

To define a constraint an arbitrary SQL expression is evaluated for each row, if it returns false the constraint validation fails. To return a meaningful error message you should configure a useful name and the columns that should be included in the text. See the following example:
```
dataObjects {
  testDataObject {
    ...
    constraints = [{
      name = A should be smaller than B
      description = "If A is bigger than B we have a problem because of ..."
      expression = "a < b"
      errorMsgCols = [id, a, b]
    }]
}
```

## Expectations on DataObjects
Expectations can be defined on DataObjects (and Actions, see below) to monitor and validate a dataset after it has been written to the DataObject. An expectation collects a custom metric and compares its result against a given expectation condition. If the condition fails a warning can be logged or an error created which stops further processing. 

Using the `type = SQLExpectation`, a simple aggregation SQL expression is evaluated over the dataset. Further, an arbitrary SQL expression can be configured as expectation condition, which is compared against the metric value. If no expectation condition is given, the custom metric value is just logged in the `finished writing to DataObject~xyz:...` log message, see example in [Metrics](#metrics) section.

SDLB supports other expectation types, see [Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html) for a list. A special type is the UniqueKeyExpectation, which can be used to validate primary keys, or just report its uniqueness as metric.

By default, the expectation is evaluated against the currently processed dataset (scope=Job), which may consist of multiple partition values. Using `scope=Job` results in one metric for the processed dataset. Using the option `scope=JobPartition`, the scope can be changed to evalute against *each* partition value in the dataset processed by the job. This results in one metric per processed partition. The option `scope=All` would take all data in the output DataObject into account, and create one metric for it. Note that expectations with scope!=Job need reading the data from the output again after it has been written, while expectations with scope=Job can be calculated on the fly when using Spark as execution engine.


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
    }]
}
```

## Expectations on Actions
It's also possible to define certain expectations on the Action. They are used to measure and validate metrics about the transformation process, e.g. transfer rate (output/input count) and completeness (total output count/total input count).

```
dataObjects {
  testAction {
    ...
    expectations = [{
      type = CompletnessExpectation
      name = CompletnessPct
      description = "Count of records in output DataObject vs count of records in input DataObject. We want to ensure that all records are present in the output DataObject after every execution."
      expectation = "= 1"
    }]
}
```
