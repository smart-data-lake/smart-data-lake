---
id: hoconOverview
title: Hocon Configurations
---

:::warning
This page is under review and currently not visible in the menu.
:::


## Overview
The configuration files are stored in the [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md) file format.
The main sections are global, connections, data objects and actions.
As a starting point, use the [application.conf](https://github.com/smart-data-lake/sdl-examples/blob/develop/src/main/resources/application.conf) from SDL-examples.
More details and options are described below.

### Global Options
In the global section of the configuration you can set global configurations such as spark options used by all executions.
You can find a list of all configurations under [API docs](https://smartdatalake.ch/docs/site/scaladocs/io/smartdatalake/app/GlobalConfig.html).  
A good list with examples can be found in the [application.conf](https://github.com/smart-data-lake/sdl-examples/blob/develop/src/main/resources/application.conf) of sdl-examples.

### Secrets User and Password Variables
TODO:
Mention Azure Key Vault here or at least point to the Azure specific doc to be created.

Usernames, passwords and other secrets should not be stored in your configuration files in clear text as these files are often stored directly in the version control system.
They should also not be visible in logfiles after execution.

Instead of having the username and password configured directly in your configuration files, you can use secret providers
for configuration values that end with `...Variable`, like BasicAuthMode's userVariable and passwordVariable.
To configure a secret use the convention `&ltSecretProviderId&gt#&ltSecretName&gt`.

Default secret providers are:

SecretProviderId|Pattern|Meaning
---|---|---
CLEAR|CLEAR#pd|The secret will be used literally (cleartext). This is only recommended for test environments.
ENV|ENV#pd|The value for this secret will be read from the environment variable called "pd".

You can configure custom secret providers by providing a list of secret providers with custom options and their id in GlobalConfig as follows:
```
global {
  secretProviders {
    &ltsecretProviderId&gt = {
     className = &ltfully qualified class name of SecretProvider&gt
     options = { &ltoptions as key/value&gt }
    }
  }
}
```

Example: configure DatabricksSecretProvider with id=DBSECRETS and scope=test (scope is a specific configuration needed for accessing secrets in Databricks).
```
global {
  secretProviders {
    DBSECRETS = {
     className = io.smartdatalake.util.secrets.DatabricksSecretProvider
     options = { scope = test }
    }
  }
}
```

You can create custom SecretProvider classes by implementing trait SecretProvider and a constructor with parameter `options: Map[String,String]`.

### Local substitution
Local substitution allows to reuse the id of a configuration object inside its attribute definitions by the special token "~{id}". See the following example:
```
dataObjects {
  dataXY {
    type = HiveTableDataObject
    path = "/data/~{id}"
    table {
      db = "default"
      name = "~{id}"
    }
  }
}
```
Note: local substitution only works in a fixed set of attributes defined in Environment.configPathsForLocalSubstitution.




