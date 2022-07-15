---
id: transformations
title: Transformations
---

:::warning
This page is under review 
:::

## Spark Transformations
To implement custom transformation logic, specify **transformers** attribute of an Action. 
It allows you to chain several transformations in a linear process, where output SubFeeds from one transformation are use as input for the next.  

Depending on your Action type the transformations have different format (described later). The two types are:

* **1-to-1** transformations (\*DfTransformer): One input DataFrame is transformed into one output DataFrame. This is the case for *CopyAction*, *DeduplicateAction* and *HistorizeAction*.
* **many-to-many** transformations (\*DfsTransformer): Many input DataFrames can be transformed into many output DataFrames. This is the case for *CustomDataFrameAction*.

The configuration allows you to use [*predefined standard transformations*](#predefined-transformations) or to define [*custom transformation*](#custom-transformations) in various languages.

:::warning Deprecation Warning
 there has been a refactoring of transformations in Version 2.0.5. The attribute **transformer** is deprecated and will be removed in future versions. Use **transformers** instead.
:::


### Predefined Transformations
Predefined transformations implement generic logic to be reused in different actions. 
Depending on the transformer there are a couple of option to specify properties, see [Configuration Schema Viewer](../../JsonSchemaViewer).
The following Transformers exist:

* AdditionalColumnsTransformer (1-to-1): Add additional columns to the DataFrame by extracting information from the context
* BlacklistTransformer (1-to-1): Apply a column blacklist to a DataFrame
* ColNamesLowercaseTransformer (1-to-1): change column name to lower case column names in output
* DataValidationTransformer (1-to-1): validates DataFrame with user defined set of rules and creates column with potential error messages
* FilterTransformer (1-to-1): Filter DataFrame with expression
* StandardizeDatatypesTransformer (1-to-1): Standardize data types of a DataFrame
* WhitelistTransformer (1-to-1): Apply a column whitelist to a DataFrame
* SparkRepartitionTransformer (1-to-1): Repartions a DataFrame

* DfTransformerWrapperDfsTransformer (many-to-many): use 1-to-1 transformer as many-to-many transformer by specifying the SubFeeds it should be applied to
<!-- TODO show one example of an predefined transformation -->


### Custom Transformations
*Custom transformers* provide an easy way to define your own data transformation logic in SQL, Scala/Java, and Python.
The transformation can be defined within the configuration file or in a separate code file. 

Additionally, static **options** and **runtimeOptions** can be defined within the custom transformers. *runtimeOptions* are extracted at runtime from the context.
Specifying options allows to reuse a transformation in different settings. For an example see SQL example below.

#### Scala/Java
In general, Scala/Java transformations can be provided within the configuration file or in seperate source files. 
You can use Spark Dataset API in Java/Scala to define custom transformations.
If you have a Java project, create a class that extends CustomDfTransformer or CustomDfsTransformer and implement `transform` method.
Then use **type = ScalaClassSparkDfTransformer** or **type = ScalaClassSparkDfsTransformer** and configure **className** attribute.

If you work without Java project, it's still possible to define your transformation in Java/Scala and compile it at runtime.
For a 1-to-1 transformation use **type = ScalaCodeSparkDfTransformer** and configure **code** or **file** as a function that takes `session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectName: String` as parameters and returns a `DataFrame`.
For many-to-many transformations use **type = ScalaCodeSparkDfsTransformer** and configure **code** or **file** as a function that takes `session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]` with DataFrames per input DataObject as parameter, and returns a `Map[String,DataFrame]` with the DataFrame per output DataObject.

Examples within the configuration file:
Example 1-to-1: select 2 specific columns (`col1` and `col2`) from `stg-tbl1` into `int-tbl`:
```
  myactionName {
    metadata.feed = myfeedName
    type = CopyAction
    inputId = stg-tbl1
    outputId = int-tbl1
    transformer = {
      scalaCode = """
        import org.apache.spark.sql.{DataFrame, SparkSession}
        import org.apache.spark.sql.functions.explode
        (session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) => {
          import session.implicits._
          df.select("col1","col2")
        }
      """
    }
    ...
```

Example many-to-many: joining `stg-tbl1` and `stg-tbl2` using two indexes, note the mapping of DataFrames:

```
  myactionName {
    metadata.feed = myfeedName
    type = CustomDataFrameAction
    inputIds = [stg-tbl1, stg-tbl2]
    outputIds = [int-tab12]
    transformers = [{
      type = ScalaCodeSparkDfsTransformer
      code = """
        import org.apache.spark.sql.{DataFrame, SparkSession}
        // define the function, with this fixed argument types)
        (session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) => {
          import session.implicits._
          val df_in1 = dfs("stg-tbl1")         // here we map the input DataFrames
          val df_in2 = dfs("stg-tbl2")
          val df_res = df_in1.join(df_in2, $"tbl1_id" === $"tbl2_id", "left").drop("tbl2_id")
        Map("int-tbl12" -> df_res)             // map output DataFrame
        }
      """
    }]
  }
```

See more examples at [sdl-examples](https://github.com/smart-data-lake/sdl-examples).

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
    * Python version >= 3.4 an <= 3.7
    * PySpark package matching your spark version
* Spark 3.x:
    * Python version >= 3.4
    * PySpark package matching your spark version

See Readme of [sdl-examples](https://github.com/smart-data-lake/sdl-examples) for a working example and instructions to setup python environment for IntelliJ

How it works: under the hood a PySpark DataFrame is a proxy for a Java Spark DataFrame. PySpark uses Py4j to access Java objects in the JVM.
