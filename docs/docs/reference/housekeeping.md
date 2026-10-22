---
id: housekeeping
title: Housekeeping
---

:::warning
This page is under review and currently not visible in the menu.
:::

## Housekeeping
SmartDataLakeBuilder supports housekeeping for DataObjects by specifying the HousekeepingMode.
It is applied after every write, and works on the partitions of the DataObject, so partition columns need to be defined.

The following HousekeepingModes are currently implemented:
* PartitionRetentionMode: Define partitions to keep by configuring a retentionCondition.
  retentionCondition is a spark sql expression working with the attributes of PartitionExpressionData returning a boolean with value true if the partition should be kept.
  All other partitions are deleted, e.g. to implement a rolling time window.
* PartitionArchiveMode: Archive old partitions by moving them into special "archive partitions".
  This reduces the number of partitions in the past, while no data is deleted.
  archivePartitionExpression defines a spark sql expression working with the attributes of PartitionExpressionData returning archive partition values as Map\[String,String\].
  If the return value is the same as the input partition values, the partition is not touched. Otherwise all records of the partition are moved to the corresponding partition.
  Be aware that the value of the partition columns changes for these files/records.

Example - cleanup partitions with partition layout dt=&ltyyyymmdd&gt after 90 days:
```
housekeepingMode = {
  type = PartitionRetentionMode
  retentionCondition = "datediff(now(), to_date(elements['dt'], 'yyyyMMdd')) &lt= 90"
}
```

### Supported DataObjects
Housekeeping is available for partitioned file based DataObjects (e.g. CsvFileDataObject, ParquetFileDataObject, ...),
for DeltaLakeTableDataObject and IcebergTableDataObject, and for JdbcTableDataObject.
PartitionArchiveMode additionally needs moving of partitions to be implemented, which is currently not the case
for IcebergTableDataObject.

For JdbcTableDataObject housekeeping works on the `virtualPartitions` of the table.
Partitions are listed with a "select distinct" query, deleted with a "delete" statement and archived with an "update"
statement changing the value of the virtual partition columns.
Make sure there is an index on the virtual partition columns, as housekeeping queries the table on every write.
Note that no data is moved physically when archiving; if the archive partition exists already, the records are merged into it.
