---
id: transformations
title: Transformations
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview
On all Spark related actions, you can add one or multiple transformers that are used to transform data before handing 
it over to the next action or writing the Data Object.
If you define multiple transformers for one action, they are processed sequentially (output of one transformer becomes input of the next).
This page describes the many out-of-the-box transformations and explains how to write your own. 

In all cases, two types of transformers need to be distinguished:
* **1-to-1**: One input Data Object is transformed into one output Data Object. 
* **many-to-many**: One or more input Data Objects are transformed into (potentially) many output Data Object.

:::info
Although it's possible to create many output Data Object, it's encouraged to only create actions with one output Data Object.
The resulting lineage of the pipelines will be clearer and impact analysis easier.
:::

#### Example
As a simple example, we add a WhitelistTransformer to a CopyAction.
This will result in only the given columns being selected from the input Data Object and any other columns being discarded.

```
SampleWhitelistAction {
    type = CopyAction
    inputId = someInput
    outputId = someOutput
    transformers = [{
        type = WhitelistTransformer
        columnWhitelist = [includedColumn1,anotherColumn2]
    }]
}
```
Note the square brackets: _transformers_ is an array as you can define multiple transformers.

The parameter `columnWhitelist` is dependent on the type of the transformer.
For details about given parameters / options, please see the [Configuration Schema Viewer](../../json-schema-viewer).

## Predefined Transformations
SDLB comes with commonly used transformers out-of-the-box.

The following predefined 1-to-1 transformations are supported:

| Transformer | Description                                                                                           |
| ----------- |-------------------------------------------------------------------------------------------------------|
| AdditionalColumnsTransformer | Add additional columns to the DataFrame by extracting information from the context  or derive new columns from existing columns  |
| BlacklistTransformer | Apply a column blacklist to discard columns                                                           |
| DataValidationTransformer | validates DataFrame with a user defined set of rules and creates column with potential error messages |
| DecryptColumnsTransformer | Decrypts specified columns using AES/GCM algorithm                                                    |
| EncryptColumnsTransformer | Encrypts specified columns using AES/GCM algorithm                                                    |
| FilterTransformer | Filter DataFrame with expression                                                                      |
| SparkRepartitionTransformer | Repartitons a DataFrame                                                                                |
| StandardizeColNamesTransformer | Used to standardize column names according to configurable rules                                      |
| StandardizeSparkDatatypesTransformer| Standardize data types of a DataFrame (decimal to corresponding int / float)                          |
| WhitelistTransformer | Apply a column whitelist to a DataFrame                                                               |


## Custom Transformations
When these predefined transformations are not enough, you can easily write your own _Custom Transformations_.
SDLB currently supports SQL, Scala and Python transformations, depending on the complexity and needed libraries.

The following custom transformations are available.

| Transformer                      | Description                                               |
|----------------------------------|-----------------------------------------------------------|
| SQLDfTransformer                 | SQL Transformation 1-to-1                                 |
| SQLDfsTransformer                | SQL Transformation many-to-many                           |
| ScalaClassGenericDfTransformer   | Spark DataFrame transformation in Scala 1-to-1            |
| ScalaClassGenericDfsTransformer  | Spark DataFrame transformation in Scala many-to-many      |
| ScalaClassSnowparkDfTransformer  | Snowpark (Snowflake) transformation in Scala 1-to-1       |
| ScalaClassSnowparkDfsTransformer | Snowpark (Snowflake) transformation in Scala many-to-many |
| ScalaClassSparkDfTransformer     | Spark DataFrame transformation in Scala 1-to-1            |
| ScalaClassSparkDfsTransformer    | Spark DataFrame transformation in Scala many-to-many      |
| ScalaClassSparkDsTransformer     | Spark DataSet transformation in Scala 1-to-1              |
| ScalaClassSparkDsNTo1Transformer | Spark DataFrame transformation in Scala many-to-one       |
| ScalaNotebookSparkDfTransformer  | Loads custom code from a Notebook                         | 
| PythonCodeSparkDfTransformer | Spark DataFrame transformation in Python 1-to-1 (using PySpark)  |
| PythonCodeSparkDfsTransformer | Spark DataFrame transformation in Python many-to-many (using PySpark) |

There are usually two variants, one for 1-to-1 transformations called _DfTransformer_ and one
for many-to-many transformations called _DfsTransformer_.

:::info
The type of the transformer needs to match your action. 
This is also apparent in the [Configuration Schema Viewer](../../json-schema-viewer):  
1-to-1 transformers are listed under 1-to-1 actions, i.e. CopyAction.  
Many-to-many transformers are only listed under many-to-many actions, i.e. CustomDataFrameAction.
:::

### SQL
Spark SQL is probably the easiest way to write a custom transformation, directly in your HOCON configuration.
All input Data Objects are available in the select statement with following naming:
- special characters are replaced by an underscore
- a postfix `_sdltemp` is added.
So an input Data Object called `table-with-hyphen` becomes `table_with_hyphen_sdltemp` inside the SQL query.
To simplify this you can also use the special token `%{inputViewName}` for 1-to-1 transformations, or `${inputViewName_<inputDataObjectName>}` for n-to-m transformations, that will be replaced with the correct name at runtime.

##### SQL 1-to-1
1-to-1 transformations use type SQLDfTransformer. 

Let's assume we have an input Data Object called dataObject1. 
We can then write a SQL transformation directly in our HOCON configuration:
```
transformers = [{
  type = SQLDfTransformer
  sqlCode = "select id, count(*) from %{inputViewName} group by id"
}]
```

The SQL code gets executed in Spark SQL so you can use all available functions.

##### SQL many-to-many
Many-to-many transformations use SQLDfsTransformer (note the additional s in Dfs). 
Now that we have multiple output Data Objects, we need to declare which SQL statements belongs to 
which Data Object. 
Therefore, we now have a map of objectIds and corresponding SQL statements:
```
transformers = [{
  type = SQLDfsTransformer
  code = {
    dataObjectOut1 = "select id, cnt from %{inputViewName_dataObjectIn1} where group = 'test1'",
    dataObjectOut2 = "select id, cnt from %{inputViewName_dataObjectIn1} where group = 'test2'"
  }
}
```

### Scala
Once transformations get more complex, it's more convenient to implement them in Scala code. 
In custom Scala code, the whole Spark Dataset API is available. 
It's of course also possible to include additional libraries for your code, 
so anything you can do in Spark Scala, you can do with SDLB. 

##### As Scala class 1-to-1
If you have a Java/Scala project, it usually makes sense to create separate classes for your custom Scala code.
Any classes in your classpath are picked up and can be referenced. 

To transform data in a 1-to-1 action, i.e. CopyAction:
```
  my_action {
    type = CopyAction
    inputId = stg_input
    outputId = int_output
    transformers = [{
      type = ScalaClassSparkDfTransformer
      className = io.package.MyFirstTransformer
    }]
  }
```
Now SDLB expects to find a class _MyFirstTransformer_ in the Scala package _io.package_. 
The class needs to extend CustomDfTransformer and with that, overwrite the transform method from it:
```
class MyFirstTransformer extends CustomDfTransformer {
    override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame {
        // manipulate df
        val dfExtended = df.withColumn("newColumn", when($"df.desc".isNotNull, $"df.desc").otherwise(lit("na")))
        dfExtended
    }
}
```
The _transform_ method you need to overwrite receives a single DataFrame called _df_.
You can manipulate this DataFrame any way you want.
In the end, you simply need to return a DataFrame back to SDLB.

Because a CopyAction is 1-to-1 only, the transformer also needs to extend the 1-to-1 `CustomDfTransformer`.

##### As Scala class many-to-many
If you have a many-to-many action and want to write a custom Scala transformer, you need to switch to a `CustomDataFrameAction`. 
```
my_many_to_many_action {
  type = CustomDataFrameAction
  inputIds = [obj1,obj2]
  outputIds = [out]
  transformers = [{
    type = ScalaClassSparkDfsTransformer
    class-name = io.package.MySecondTransformer
  }]
}
```

In this case, your Scala class also needs to extend the many-to-many `CustomDfsTransformer`. You can either overwrite the respective transform method as shown later, 
or define any transform method that suits you best (starting from SDLB version 2.6.x).

If you choose to implement any transform method, this method is called dynamically by looking for the parameter values in the input DataFrames and Options.
The limitation is that the parameter types must be chosen from the following list:
- `SparkSession`
- `Map[String,String]`
- `DataFrame`
- `Dataset[<Product>]`
- any primitive data type (`String`, `Boolean`, `Int`, ...)

Primitive value parameters are assigned looking up the parameter name in the Map of Options and converted to the target data type.
Data types for primitive values might also use default values or be enclosed in an Option[...] to mark it as non required.

DataFrame parameters are assigned looking up the parameter name in the Map of DataFrames. A potential `df` prefix is removed from the parameter name before the lookup.

All lookups of parameters are done case-insensitive, also dash and underscores are removed.

If the Action has only one output DataObject, the return type can also be defined as a simple DataFrame instead of a `Map[String,DataFrame]`.

```
class MySecondTransformer extends CustomDfsTransformer {
    def transform(session: SparkSession, dfObj1: DataFrame, dfObj2: DataFrame, parameter123: Boolean = true) : DataFrame {
        // now you have multiple input DataFrames and can potentially return multiple DataFrames
        val dfCombined = dfObj1.join(dfObj2, $"dfObj1.id"==$"dfObj2.fk", "left")
        dfCombined // use a Map[String,DataFrame] to return multiple DataFrames.
    }
}
```

Overwriting the standard transform (traditional way) is done as follows:
```
class MySecondTransformer extends CustomDfsTransformer {
    override def transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) : Map[String,DataFrame] {
        // now you have multiple input DataFrames and can potentially return multiple DataFrames
        val dfObj1 = dfs("obj1")
        val dfObj2 = dfs("obj2")
        val dfCombined = dfObj1.join(dfObj2, $"dfObj1.id"==$"dfObj2.fk", "left")
        Map("out"->dfCombined)
    }
}
```
The _transform_ method now gets a map with all your input DataFrames called _dfs_. 
The key in _dfs_ contains the dataObjectId, in this example we have two inputs called _obj1_ and _obj2_, 
we use it to extract the two DataFrames.
Again, you can manipulate all DataFrames as needed and this time, return a map with all output Data Objects.
As noted, it's best practice to only return one Data Object (many-to-one action) as in our example.


:::info
One thing that might be confusing at this point:

CustomDataFrameAction is the type of the **action** itself. 
Take a look at the  [Configuration Schema Viewer](../../json-schema-viewer): 
You will see CustomDataFrameAction as a direct action type, similar to CopyAction or HistorizeAction.

ScalaClassSparkDfsTransformer is the type of your **transformer**. 
It needs to correspond to the action type. A 1-to-1 action expects a 1-to-1 transformer, a many-to-many action expects a many-to-many transformer.
:::

##### In Configuration
If you don't work with a full Java/Scala project, it's still possible to define your transformations 
in your HOCON configuration and compile it at runtime.
In this case, use the transformer type ScalaCodeSparkDfTransformer.

To include the Scala code inline:
```
myActionName {
  type = CopyAction
  inputId = stg-tbl1
  outputId = int-tbl1
  transformers = [{
    type = ScalaCodeSparkDfTransformer
    code = """
      import org.apache.spark.sql.{DataFrame, SparkSession}
      import org.apache.spark.sql.functions.explode
      (session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) => {
        import session.implicits._
        df.select("col1","col2")
      }
    """
  }]
```
Check the method signature in CustomDfTransformer and CustomDfsTransformer according to your requirements.


### Python
The transformer needs to use type `PythonCodeSparkDfTransformer` (1-to-1) or `PythonCodeSparkDfsTransformer` (many-to-many). 
You can either provide it as separate file or inline as Python code again.

Inline 1-to-1 example:
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

PySpark is initialized automatically and the PySpark session is available under the variables
`sc`, `session` or `sqlContext`. 
Some additional variables are also available:
* `inputDf`: Input DataFrame
* `dataObjectId`: Id of input DataObject as string

The output DataFrame needs to be set with `setOutputDf(df)`.

And many-to-many inline example:
```
transformers = [{
  type = PythonCodeDfsTransformer 
  code = """
    |dfResult = inputDfs[df1].join(inputDfs[df2], inputDfs[df1].id == inputDfs[df2].fk, "left") \
    |  .select(inputDfs[df1].id, inputDfs[df1].desc, inputDfs[df2].desc)
    |setOutputDf({"outputId": dfResult})
  """
}]
```

In this case, `inputDfs` is a dictionary (dict) and `setOutputDf` expects a dictionary.

##### Requirements
Running Python transformations needs some additional setup. 
In general, Python >= 3.4 is required and PySpark package needs to be installed with a version matching your SDL spark version. Further environment variable PYTHONPATH needs to be set to your python environment `.../Lib/site-packages` directory, and pyspark command needs to be accessible from the PATH environment variable.


### Options / RuntimeOptions
So far these transformations have been quite static: 
Code written can probably only be used for one specific action. 

For custom transformers, you can therefore provide additional options:
* `options`: static options provided in your HOCON configuration
* `runtimeOptions`: extracted at runtime from the context. 



##### In SQL
If you want to use options in SQL, the syntax is %\{key}:
```
transformers = [{
  type = SQLDfTransformer
  sqlCode = "select id, cnt, '%{test}' as test, %{last_run_id} as last_run_id from dataObject1"
  options = {
    test = "test run"
  }
  runtimeOptions = {
    last_run_id = "runId - 1" // runtime options are evaluated as spark SQL expressions against DefaultExpressionData
  }
}]
```

##### In Scala
If you check the signature of the `transform` method again, you can see that you get more than just the DataFrames
you can manipulate.
You also get a Map[String,String] called `options`. 
This Map contains the combined `options` and `runtimeOptions`. 
So in your custom class, you can read all options and runtimeOptions and use them accordingly to parametrize your code.

##### In Python
Similarly in Python, in addition to the variables `inputDf` and `dataObjectId` (resp. `inputsDfs`), you get a variable called `options`
containing all `options` and `runtimeOptions`.