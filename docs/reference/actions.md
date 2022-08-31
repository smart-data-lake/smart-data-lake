---
id: actions
title: Actions
---

:::warning
This page is under review and currently not visible in the menu.
:::


Actions describe dependencies between dataObjects and necessary actions to connec them. There are predefined actions which can be fine tuned and general templates. 

## recursiveInputIds
In general we want to avoid cyclic graph of action. This option enables updating dataObjects. Therewith, the dataObject is input and output at the same time. It needs to be specified in as output, but not as input.

Example: assuming an object `stg-src`, which data should be added to an growing table `int-tgt`.

```
  action1 {
    type = CustomDataFrameAction
    inputIds = [stg-src]
    outputIds = [int-tgt]
    recursiveInputIds = [int-tgt]
    ...
```

<!-- TODO describe more action facts


-->
