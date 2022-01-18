---
id: streaming
title: Streaming
---

:::warning
This page is under review and currently not visible in the menu.
:::

## Streaming
You can execute any DAG in streaming mode by using commandline option `--streaming`.
In streaming mode SDL executes the Exec-phase of the same DAG continuously, processing your data incrementally.
SDL discerns between synchronous and asynchronous actions:
- Synchronous actions are executed one after another in the DAG, they are synchronized with their predecessors and successors.
- Asynchronous actions have their own rhythm. They are executed not synchronized from the other actions, except for the first increment. In the first increment their start is synchronized with their predecessors and the first execution is waited for before starting their successors. This allows to maintain execution order for initial loads, where tables and directories might need to be created one after another.

You can mix synchronous and asynchronous actions in the same DAG. Asynchronous actions are started in the first increment. Synchronous actions are executed in each execution of the DAG.

Whether an action is synchronous or asynchronous depends on the execution engine used. For now only "Spark Structured Streaming" is an asynchronous execution engine. It is configured by setting execution mode SparkStreamingMode to an action.

You can control the minimum delay between synchronous streaming runs by setting configuration `global.synchronousStreamingTriggerIntervalSec` to a certain amount of seconds.
For asynchronous streaming actions this is controlled by the corresponding streaming mode, e.g. SparkStreamingMode.
