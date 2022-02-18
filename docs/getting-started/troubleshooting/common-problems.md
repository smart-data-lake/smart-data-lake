---
id: common-problems
title: Common Problems
---
This page lists a couple of common pitfalls that you may encounter in this guide as well as their solutions.

## Missing files / DataObject schema is undefined
If you encounter an error that looks like this:

    Exception in thread "main" io.smartdatalake.util.dag.TaskFailedException: Task select-airport-cols failed. Root cause is 'IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A sche
    ma must be defined if there are no existing files.'
    Caused by: java.lang.IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A schema must be defined if there are no existing files.

This is likely due to the fact that you did not execute the `download` feed from the previous step.
Executing the `compute` feed will only work if you already have some files under *data/stg-airports* and data/stg-departures.
This is because in the first step, we download files of which SDL doesn't know the schema of in advance.
The init-phase will require that for all Data Objects, the schema is known so that it can check for inconsistencies.
When we already have some files, it will infer the schema based on the files.

To work around this, execute the feed `download` again. After that feed was successfully executed, the execution of
the feed `.*` or `compute` will work.

One way to prevent this problem is to explicitly provide the schema for the JSON and for the CSV-File,
which is out of the scope for this part of the tutorial.

## download-departures fails because of a Timeout
If you encounter an error that looks like this:

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

Since both web servers are freely available on the internet, they might be overloaded by traffic. If the download fails because of a timeout, either increase readTimeoutMs or wait a couple of minutes and try again. If the download still won't work (or if you just get empty files), you can copy the contents of the folder data-fallback-download into your data folder.

## How to kill SDLB if it hangs

In case you run into issues when executing your pipeline and you want to terminate the process
you can use this docker command to list the running containers:

    docker ps

While your feed-execution is running, the output of this command will contain
an execution with the image name *sdl-spark:latest*.
Use the container id to stop the container by typing:

    docker containter stop <container id>

