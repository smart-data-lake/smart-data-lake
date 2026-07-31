---
id: housekeeping
title: Housekeeping
---

:::warning
This page is under review and currently not visible in the menu.
:::

## Housekeeping
SmartDataLakeBuilder supports housekeeping for DataObjects by specifying the HousekeepingMode.

The following HousekeepingModes are currently implemented:
* PartitionRetentionMode: Define partitions to keep by configuring a retentionCondition.
  retentionCondition is a spark sql expression working with the attributes of PartitionExpressionData returning a boolean with value true if the partition should be kept.
* PartitionArchiveCompactionMode: Archive and compact old partitions.
    * Archive partition reduces the number of partitions in the past by moving older partitions into special "archive partitions".
      archiveCondition defines a spark sql expression working with the attributes of PartitionExpressionData returning archive partition values as Map\[String,String\].
      If return value is the same as input partition values, the partition is not touched. Otherwise all files of the partition are moved to the corresponding partition.
      Be aware that the value of the partition columns changes for these files/records.
    * Compact partition reduces the number of files in a partition by rewriting them with Spark.
      compactPartitionExpression defines a sql expression working with the attributes of PartitionExpressionData returning true if this partition should be compacted.
      Once a partition is compacted, it is marked as compacted and will not be compacted again. It is therefore ok to return true for all partitions which should be compacted, regardless if they have been compacted already.

Example - cleanup partitions with partition layout dt=&ltyyyymmdd&gt after 90 days:
```
housekeepingMode = {
  type = PartitionRetentionMode
  retentionCondition = "datediff(now(), to_date(elements['dt'], 'yyyyMMdd')) &lt= 90"
}
```
