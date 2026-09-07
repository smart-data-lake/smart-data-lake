---
id: columnLineage
title: Column Lineage
---

SDLB knows which DataObject an Action reads and writes, see [DAG](dag.md). This is lineage on the level of
whole datasets. Column level lineage goes one step further and tells from which *columns* of which input
DataObjects a *column* of an output DataObject is created, and how. It answers questions like

- which transformations have been applied to create a specific column of a reporting table
- where and how is a column of a source system used further downstream

## Exporting the column lineage

The column lineage is analyzed in the init-phase of a dry-run, so no data is read and nothing is written:

```bash
sdlb --config config/ --feed-sel '.*' --test dry-run-with-lineage-export
```

The export writes one Json document per output DataObject to `global.dataObjectsSchemaSource`, the same
location the schema export uses, see [Schema](schema.md):

```hocon
global {
  dataObjectsSchemaSource = "file:./schema"
}
```

## Format

A document contains the Action which created the DataObject and the lineage of its columns, in the format of
the [OpenLineage column lineage facet](https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet).
Using an established format makes the export consumable by existing lineage tools, e.g. Marquez or DataHub,
without writing a converter first.

```json
{
  "actionId": "computeCityStatistics",
  "dataObjectId": "cityStatistics",
  "columnLineage": {
    "fields": {
      "city": {
        "inputFields": [
          {
            "namespace": "sdlb",
            "name": "cities",
            "field": "name",
            "transformations": [{"type": "DIRECT", "subtype": "IDENTITY", "masking": false}]
          }
        ]
      },
      "population": {
        "inputFields": [
          {
            "namespace": "sdlb",
            "name": "cities",
            "field": "inhabitants",
            "transformations": [
              {"type": "DIRECT", "subtype": "TRANSFORMATION", "description": "sum(inhabitants)", "masking": false}
            ]
          }
        ]
      }
    }
  }
}
```

`name` is the id of the input DataObject, and `field` the name of its column. Note that `namespace` and `name`
do not identify the physical dataset, as the same DataObject can be backed by different storage locations in
different environments.

`subtype` is `IDENTITY` if the value of the input column is taken over unchanged, e.g. by a select or a rename,
and `TRANSFORMATION` if it is modified. For a transformation, `description` holds the expression which creates
the column, cut off if it is very long.

## Columns without a source, and columns SDLB could not trace

A column which is not created from an input column at all, e.g. a constant or `count(*)`, is exported with an
empty `inputFields` list and the expression which creates it. A column whose lineage could **not** be traced
back completely is listed in `unresolvedFields` instead, and left out of `fields`:

```json
{
  "actionId": "computeCityStatistics",
  "dataObjectId": "cityStatistics",
  "columnLineage": {
    "fields": {
      "loadedAt": {"inputFields": [], "expression": "current_timestamp()"}
    }
  },
  "unresolvedFields": ["externalRating"]
}
```

The distinction matters when reading the export: an empty `inputFields` means SDLB knows the column has no
source column, while `unresolvedFields` means SDLB could not find out. A column missing from both never
existed in the output DataObject.

The lineage of one run covers one Action each. End-to-end lineage over a whole pipeline is assembled by
following the columns from Action to Action: the input fields of a DataObject written by one Action are the
output columns of the DataObject read by the next one.

## Caching

An Action can hand its output DataFrame to the next Action instead of letting it read the DataObject again
(`cacheOutput=true`). The DataFrame then cumulates the transformations of several Actions, but the exported
lineage still respects the Action boundaries: a column of an input DataObject is a leaf of the lineage of the
Action reading it. How the input DataObject got that column is described by the export of the Action which
wrote it. There is no need to turn caching off to export lineage.

`cacheInput` has no effect on the lineage at all, as it only materializes DataFrames in the exec-phase.

One case does degrade. If an Action reads both the cached output of a previous Action and a DataObject that
previous Action passed through unchanged, both inputs share the same Spark columns. Spark's analyzer then
replaces the column ids of one side of the join, so the columns of that side cannot be traced back and are
reported in `unresolvedFields`. Columns of the other side are reported for both input DataObjects, as they
belong to both.

## Limitations

Column lineage is analyzed for the Spark engine only, and it is best-effort: a column which can not be traced
back to an input DataObject is reported as unresolved rather than with a wrong source. This is the case for

- columns read from the same DataObject twice, e.g. in a self-join, where only one of the two occurrences is
  traced back, as Spark replaces the duplicated expression ids. The same happens for the cached pass-through
  described above.
- transformations which break the DataFrame lineage, e.g. a custom transformer which reads data itself

Columns which are used in a join, filter, group by, sort or window condition influence the output without
being part of its value. They are not reported yet - OpenLineage calls this `INDIRECT` lineage and collects it
in the `dataset` list of the facet. An aggregation over all rows such as `count(*)` belongs there too, and is
reported without input columns until then.

For a typed Dataset transformation, e.g. a `map` over a case class, the Scala function is opaque. Every output
column of such a transformation is therefore reported to depend on every column it reads.
