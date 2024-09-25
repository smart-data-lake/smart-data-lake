---
id: common-problems
title: Common Problems
---
This page lists a couple of common pitfalls that you may encounter in this guide as well as their solutions.

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