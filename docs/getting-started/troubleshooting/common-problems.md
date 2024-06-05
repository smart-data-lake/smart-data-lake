---
id: common-problems
title: Common Problems
---
This page lists a couple of common pitfalls that you may encounter in this guide as well as their solutions.

## Missing files / DataObject schema is undefined
If you encounter an error that looks like this:
```
Exception in thread "main" io.smartdatalake.util.dag.TaskFailedException: Task select-airport-cols failed. Root cause is 'IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A sche
ma must be defined if there are no existing files.'
Caused by: java.lang.IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A schema must be defined if there are no existing files.
```
The init-phase will require to know the schema for all Data Objects to check for inconsistencies. If downloaded files already exist, the schema can be inferred. 
In SDL version *< 2.3.0* the *download* action was typically defined in a separate feed, separated from further transformation actions. This download feeds needed to be run upfront, before running further feeds like *compute*. Once the downloaded files are present, the schema can be inferred. 

To work around this issue, execute the feed `download` again. After that feed was successfully executed, the execution of
the feed `.*` or `compute` will work.
One way to prevent this problem is to explicitly provide the schema for the JSON and for the CSV-File.

This issue should not occur in SDL versions *> 2.3.0*.

## download-departures fails because of a Timeout
If you encounter an error that looks like this:
```
                                        ┌─────┐
                                        │start│
                                        └─┬─┬─┘
                                        │ │
                                        │ └────────────────────┐
                                        │                      │
                                        v                      v
    ┌──────────────────────────────────────┐ ┌──────────────────────────────────────┐
    │download-departures FAILED PT5.183334S│ │download-airports SUCCEEDED PT1.91309S│
    └──────────────────────────────────────┘ └──────────────────────────────────────┘
    [main]
    Exception in thread "main" io.smartdatalake.util.dag.TaskFailedException: Task download-departures failed. Root cause is 'WebserviceException: Read timed out'
```
Since both web servers are freely available on the internet, they might be overloaded by traffic. If the download fails because of a timeout, either increase readTimeoutMs or wait a couple of minutes and try again. If the download still won't work (or if you just get empty files), you can copy the contents of the folder data-fallback-download into your data folder.

## How to kill SDLB if it hangs

In case you run into issues when executing your pipeline and you want to terminate the process
you can use this docker command to list the running containers:
```
docker ps
```
While your feed-execution is running, the output of this command will contain
an execution with the image name *sdl-spark:latest*.
Use the container id to stop the container by typing:
```
docker containter stop <container id>
```
## ERROR 08001: java.net.ConnectException : Error connecting to server localhost on port 1527

This likely happens during or after part 2 if you forget to add `--pod getting-started` to your podman command.
Remember that SDL now needs to communicate with the Metastore that we are starting. 
To allow this communication, you need to make sure that SDL and the metastore are running in the same pod.