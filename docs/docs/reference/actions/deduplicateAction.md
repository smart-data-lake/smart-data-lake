---
id: deduplicateAction
title: DeduplicateAction (renamed)
unlisted: true
---

**DeduplicateAction was renamed to [UpsertAction](upsertAction.md) in version 3.0.0.**

The old name was misleading: the Action does not deduplicate its input data, it keeps the latest version of every record identified by the primary key of the output table (Slowly Changing Dimension Type 1).
Use a `DeduplicateTransformer` if you need to make the input data unique.

Existing configurations using `type = DeduplicateAction` keep working, but the name is deprecated.
Change it to `type = UpsertAction`, all parameters stay the same:

```
actions {
  upsert-airports {
    type = UpsertAction
    inputId = stg-airports
    outputId = int-airports
  }
}
```

See [UpsertAction](upsertAction.md) for the documentation.
