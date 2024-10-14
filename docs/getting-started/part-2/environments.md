---
title: Configure for different environments
---

## Goal

Industrializing data pipelines also means running them in different environments, e.g. DEV (development), INTE (Integration) and PROD (Production).
Different environments normally need parametrized settings for connections and secrets.
In this step we will introduce SDLB approach for environment configuration, create a configuration for the DEV environment, and discuss how it can be adapted for PROD.

## SDLB approach for environment configuration

As SDLB configuration files use HOCON format, environment configuration can be implemented using [HOCON substitution](https://github.com/lightbend/config/blob/master/HOCON.md#substitutions). 

SDLB conventions suggest to use a separate configuration file per environment located in a special folder `envConfig`.
Let's call these files "environment configuration files". The `envConfig` folder is a separate folder from the `config` folder.

When starting an SDLB Job, *one* environment configuration file is selected, and *all* configuration files from the `config` folder.
Using HOCON substitution the definitions of the selected environment configuration file can be used in the normal configuration files.  

Selecting additional configurations files on the SDLB command line is easy, as you can give a list of configuration file locations.
To start our SDLB job with for a specific environment, we can just add the corresponding environment file as follows:
```
./startJob.sh -c /mnt/config,/mnt/envConfig/dev.conf --feed-sel compute
```

## Creating DEV configuration file

The command above do anything new yet, as we first need to create the `envConfig/dev.conf` file.

As part of this tutorial, let's make the following configurations customizable per environment:
- database: The database name to be used in DeltaLakeTableDataObjects
- basePath: The root path where data files are stored
- basePathWithId: Helper variable to be used as path with DataObjectId appended
- tablePathWithId: Helper variable to be used as path with DataObjectId appended for table DataObjects.
  Separating this into a another variable allows us to switch to managed tables for corresponding environments by setting it to `null`.  

For this create an environment file `envConfig/dev.conf` with the following content, if it doesn't yet exist:
```
env {
  catalog = null # no catalog needed for local execution
  database = default
  basePath = "./"
  basePathWithId = ${env.basePath}"~{id}"
  tablePathWithId = ${env.basePathWithId}  
}
```

Then lets replace all `table.database` and `path` configuration entries with a HOCON substitution as follows, and add `table.catalog`:
```
  int-departures {
    type = DeltaLakeTableDataObject
    path = ${env.tablePathWithId}
    table = {
      catalog = ${env.catalog}
      db = ${env.database}
      name = int_departures
      primaryKey = [icao24, estdepartureairport, dt]
    }
    allowSchemaEvolution = true
  }
```

Note that HOCON substitution syntax needs to be placed outside of string double quotes.
Now you can test the configuration without running any feed. This can be done by using command line parameter `--test config`: 
```
./startJob.sh -c /mnt/config,/mnt/envConfig/dev.conf --feed-sel compute --test config
```

:::note Automated Testing
Testing the configuration is a very good starting point for automated integration tests.
It is the easiest CI pipeline and recommended for every project. See also [Testing](/docs/reference/testing.md).
:::

## Configuring other environments

To configure other environments like PROD (Production), a `envConfig/prd.conf` file is created and the relevant configurations adapted.
Then `dev.conf` in `startJob.sh` command is replaced with `prd.conf`.

A special case is managing secrets for different environments, e.g. passwords.
SDLB supports various [Secret Providers](/docs/reference/hoconSecrets.md), which can be configured differently per environment.

## Summary

You have now seen different parts of industrializing a data pipeline like robust data formats, caring about historical data and configuring different environments.
Further, you have explored data interactively with spark-shell. 

The final solution for departures/airports/btl.conf should look like the files ending with `part-2-solution` in [this directory](https://github.com/smart-data-lake/getting-started/tree/master/config).

In part 3 we will see how to incrementally load fresh flight data.
See you!

