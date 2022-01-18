---
id: transformations
title: Transformations
---

:::warning
This page is under review and currently not visible in the menu.
:::

## Spark Transformations
To implement custom transformation logic, define the **transformers** attribute of an Action. It allows you to chain several transformation in a linear process,
where output SubFeeds from one transformation are use as input for the next.  
Note that the definition of the transformations looks different for:
* **1-to-1** transformations (\*DfTransformer): One input DataFrame is transformed into one output DataFrame. This is the case for CopyAction, DeduplicateAction and HistorizeAction.
* **many-to-many** transformations (\*DfsTransformer): Many input DataFrames can be transformed into many output DataFrames. This is the case for CustomSparkAction.

The configuration allows you to use predefined standard transformations or to define custom transformation in various languages.

**Deprecation Warning**: there has been a refactoring of transformations in Version 2.0.5. The attribute **transformer** is deprecated and will be removed in future versions. Use **transformers** instead.

### Predefined Transformations
Predefined transformations implement generic logic to be reused in different actions. The following Transformers exist:
* FilterTransformer (1-to-1): Filter DataFrame with expression
* BlacklistTransformer (1-to-1): Apply a column blacklist to a DataFrame
* WhitelistTransformer (1-to-1): Apply a column whitelist to a DataFrame
* AdditionalColumnsTransformer (1-to-1): Add additional columns to the DataFrame by extracting information from the context
* StandardizeDatatypesTransformer (1-to-1): Standardize datatypes of a DataFrame
* DfTransformerWrapperDfsTransformer (many-to-many): use 1-to-1 transformer as many-to-many transformer by specifying the SubFeeds it should be applied to

### Custom Transformations
Custom transformers provide an easy way to define your own spark logic in various languages.

You can pass static **options** and **runtimeOptions** to custom transformations. runtimeOptions are extracted at runtime from the context.
Specifying options allows to reuse a transformation in different settings.

#### Java/Scala
You can use Spark Dataset API in Java/Scala to define custom transformations.
If you have a Java project, create a class that extends CustomDfTransformer or CustomDfsTransformer and implement `transform` method.
Then use **type = ScalaClassDfTransformer** or **type = ScalaClassDfsTransformer** and configure **className** attribute.

If you work without Java project, it's still possible to define your transformation in Java/Scala and compile it at runtime.
For a 1-to-1 transformation use **type = ScalaCodeDfTransformer** and configure **code** or **file** as a function that takes `session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectName: String` as parameters and returns a `DataFrame`.
For many-to-many transformations use **type = ScalaCodeDfsTransformer** and configure **code** or **file** as a function that takes `session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]` with DataFrames per input DataObject as parameter, and returns a `Map[String,DataFrame]` with the DataFrame per output DataObject.

See [sdl-examples](https://github.com/smart-data-lake/sdl-examples) for details.

#### SQL
You can use Spark SQL to define custom transformations.
Input dataObjects are available as tables to select from. Use tokens %{&ltkey&gt} to replace with runtimeOptions in SQL code.
For a 1-to-1 transformation use **type = SQLDfTransformer** and configure **code** as your SQL transformation statement.
For many-to-many transformations use **type = SQLDfsTransformer** and configure **code** as a Map of "&ltoutputDataObjectId&gt, &ltSQL transformation statement&gt".

Example - using options in sql code for 1-to-1 transformation:
```
transformers = [{
  type = SQLDfTransformer
  name = "test run"
  description = "description of test run..."
  sqlCode = "select id, cnt, '%{test}' as test, %{run_id} as last_run_id from dataObject1"
  options = {
    test = "test run"
  }
  runtimeOptions = {
    last_run_id = "runId - 1" // runtime options are evaluated as spark SQL expressions against DefaultExpressionData
  }
}]
```

Example - defining a many-to-many transformation:
```
transformers = [{
  type = SQLDfsTransformer
  code = {
    dataObjectOut1 = "select id,cnt from dataObjectIn1 where group = 'test1'",
    dataObjectOut2 = "select id,cnt from dataObjectIn1 where group = 'test2'"
  }
}
```

See [sdl-examples](https://github.com/smart-data-lake/sdl-examples) for details.

#### Python
It's also possible to use Python to define a custom Spark transformation.
For a 1-to-1 transformation use **type = PythonCodeDfTransformer** and configure **code** or **file** as a python function.
PySpark session is initialize and available under variables `sc`, `session`, `sqlContext`.
Other variables available are
* `inputDf`: Input DataFrame
* `options`: Transformation options as Map[String,String]
* `dataObjectId`: Id of input dataObject as String

Output DataFrame must be set with `setOutputDf(df)`.

For now using Python for many-to-many transformations is not possible, although it would be not so hard to implement.

Example - apply some python calculation as udf:
```
transformers = [{
  type = PythonCodeDfTransformer 
  code = """
    |from pyspark.sql.functions import *
    |udf_multiply = udf(lambda x, y: x * y, "int")
    |dfResult = inputDf.select(col("name"), col("cnt"))\
    |  .withColumn("test", udf_multiply(col("cnt").cast("int"), lit(2)))
    |setOutputDf(dfResult)
  """
}]
```

Requirements:
* Spark 2.4.x:
    * Python version &gt= 3.4 an &lt= 3.7
    * PySpark package matching your spark version
* Spark 3.x:
    * Python version &gt= 3.4
    * PySpark package matching your spark version

See Readme of [sdl-examples](https://github.com/smart-data-lake/sdl-examples) for a working example and instructions to setup python environment for IntelliJ

How it works: under the hood a PySpark DataFrame is a proxy for a Java Spark DataFrame. PySpark uses Py4j to access Java objects in the JVM.
