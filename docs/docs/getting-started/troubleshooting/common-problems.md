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

Since both web servers are freely available on the internet, they might be overloaded by traffic. If the download fails because of a timeout, either increase readTimeoutMs or wait a couple of minutes and try again. If the download still won't work (or if you just get empty files), you can copy the contents of the folders `data/stg-airports-fallback` and `data/stg-departures-fallback` into `data/stg-airports` and `data/stg-departures` respectively.

## download-departures fails with 403 "You cannot access historical flights"

Without an account, opensky-network.org limits queries by *recency* and by *interval length*: the time window
has to start within the last few days, and an interval of 12 hours is still served while 18 hours is not.
The `departures.conf` files of part 1 and 2 carry a fixed time window from 2021, which is refused for both
reasons.

In part 1 and 2, let `prepare.sh` rewrite the window in your `config/departures.conf` to the last 6 hours:
```
./prepare.sh --fix-timestamps
```
It only touches the activated configuration file, never the `part-*` solution files. `./prepare.sh 2` and
`./prepare.sh 3` do it as part of seeding, unless you pass `--keep-timestamps`.

From part 3 onwards `CustomWebserviceDataObject` computes the window itself and keeps it inside the limits,
so no adjustment is needed there.

## Connection~default-engine not found in instance registry

Since SDLB 3.0.0 the Spark session is configured by an engine connection, which this project defines in
`config/global.conf`. You see this error if that file is not part of the configuration passed with `--config`,
for example when a single configuration file is passed instead of the whole `config` directory.
See [Execution Engines](/docs/reference/executionEngines).

## Configuration objects defined in multiple locations

When executing SDLB, you might get the following exception:
```
Exception in thread "main" io.smartdatalake.config.ConfigurationException: 
Configuration parsing failed because of configuration objects defined in multiple locations: 
Action~download-departures=HadoopConfigFile;
HadoopConfigFile DataObject~ext-departures=HadoopConfigFile;
HadoopConfigFile DataObject~stg-departures=HadoopConfigFile;
```
Note that we are starting SDLB in this getting started guide with the option `--config /mnt/config ` which means the whole directory.
SDLB will therefore read any `.conf` file in this directory and attempt to parse it.
If you define an action in two different files, you will get this error as SDLB can not figure out, 
which file takes precedence.

To solve the problem, either remove the `.conf` file extension or move one of the files.



## How to kill SDLB if it hangs

In case you run into issues when executing your pipeline and you want to terminate the process
you can use this podman command to list the running containers:
```
podman ps
```
While your feed-execution is running, the output of this command will contain
an execution with the image name *sdl-spark:latest*.
Use the container id to stop the container by typing:
```
podman containter stop <container id>
```
