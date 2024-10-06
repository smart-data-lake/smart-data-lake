---
title: Get Airports
---

## Goal

In this step, we will download airports master data from the website described in [Inputs](../get-input-data) using SDLB.
Because this step is very similar to the previous one, we will make some "mistake" on purpose to demonstrate how to deal with config errors.

Just like in the previous step, we need one action and two DataObjects.
Except for the object and action names, the config to add here is almost identical to the previous step.

You are welcome to try to implement it yourself before continuing. 
Just as in the previous step, you can use "download" as feed name.

## Solution
You should now have an airports.conf file similar to [this](https://github.com/smart-data-lake/getting-started/tree/master/config/airports.conf.part-1a-solution) one.
The only notable difference is that you had to use the type **CsvFileDataObject** for the airports.csv file,
since this is what the second webservice answers with. 
Note that you would not get an error at this point if you had chosen another file format. 
Since we use *FileTransferAction* in both cases, the files are copied without the content being interpreted yet.

You can start the same `./startJob.sh` command as before, and you should see that both directories
*stg-airports* and *stg-departures* have new files now.
Notice that since both actions have the same feed, the option `--feed-sel download` executes both of them.

## Mess Up the Solution
Now let's see what happens when things don't go as planned. 
For that, replace your config file with the contents of [this](https://github.com/smart-data-lake/getting-started/tree/master/config/airports.conf.part-1b-solution) file.
When you start the `./startJob.sh` command again, you will see two errors:

1. The name of the DataObject "NOPEext-departures" does not match with the inputId of the action download-departures.
   This is a very common error and the stacktrace should help you to quickly find and correct it
   ```
       Exception in thread "main" io.smartdatalake.config.ConfigurationException: (Action~download-departures) [] key not found: DataObject~ext-departures
   ```
   As noted before, SDLB will normally use Action-IDs and DataObject-IDs to communicate where to look in your configuration files.

2. An unknown DataObject type was used. In this example, stg-airports was assigned the type UnicornFileDataObject, which does not exist.
   ```
       Exception in thread "main" io.smartdatalake.config.ConfigurationException: (DataObject~stg-airports) ClassNotFoundException: io.smartdatalake.workflow.dataobject.UnicornFileDataObject
   ```
   Internally, the types you choose are represented by Scala Classes.
   These classes define all characteristics of a DataObject and all it's parameters, i.e. the url we defined in our WebserviceFileDataObject.
   This also explains why you get a *ClassNotFoundException* in this case.

## Try fixing it

Try to fix one of the errors and keep the other one to see what happens: Nothing.
Why is that? 

SDLB validates your configuration file(s) before executing its contents.
If the configuration does not make sense, it will abort before executing anything to minimize the chance that you'll end up in an inconsistent state.

:::tip
During validation, the whole configuration is checked, not just the parts you are trying to execute.
If you have large configuration files, it can sometimes be confusing to see an error and realize that 
it's not on the part you are currently working on but in a different section.
:::

SDLB is built to detect configuration errors as early as possible (early-validation). It does this by going through several phases. 
1. Validate configuration  
validate superfluous attributes, missing mandatory attributes, attribute content and consistency when referencing other configuration objects.
2. *Prepare* phase  
validate preconditions, e.g. connections and existence of tables and directories.
3. *Init* phase  
executes the whole feed *without any data* to spot incompatibilities between the Data Objects that cannot be spotted 
by just looking at the config file. For example a column which doesn't exist but is referenced in a later Action will cause the init phase to fail.
4. *Exec* phase  
only if all previous phases have been passed successfully, execution is started.

When running SDLB, you can clearly find "prepare", "init" and "exec" steps for every Action in the logs.
See [Execution Phases](/docs/reference/executionPhases) for a detailed description.

Now is a good time to fix both errors in your configuration file and execute the action again.

Early-validation is a core feature of SDLB and will become more and more valuable with the increasing complexity of your data pipelines.
Speaking of increasing complexity: In the next step, we will begin transforming our data.



