---
id: metrics
title: Metrics
---

:::warning
This page is under review and currently not visible in the menu.
:::

## Metrics
While the DAG is running, metrics are gathered for each Action and output-DataObject. They can be found in log statements and are written to the state file.

Sample log message:
`2020-07-21 11:36:34 INFO  CopyAction:105 - (Action~a) finished writing DataFrame to DataObject~tgt1: duration=PT0.906S records_written=1 bytes_written=1142 num_tasks=1 stage=save`

A fail condition can be specified on Actions to fail execution if a certain condition is not met.
The condition must be specified as spark sql expression, which is evaluated as where-clause against a dataframe of metrics. Available columns are dataObjectId, key, value.
To fail above sample log in case there are no records written, specify `"dataObjectId = 'tgt1' and key = 'records_written' and value = 0"`.

By implementing interface StateListener  you can get notified about action results & metrics. To configure state listeners set config attribute `global.stateListeners = [{className = ...}]`.
