# FAQ

## There are so many DataObjects / Actions, how do I get started?
Common use cases are included in the [sdl-examples](https://github.com/smart-data-lake/sdl-examples) project, 
check out its application.conf.
This should give you a good overview of how to use data objects and actions.
Then check the [Reference](Reference.md) to see which types of data objects and actions are currently supported.

## How can I test Hadoop / HDFS locally ?
When using `local://` URIs, file permissions on Windows, or certain actions, local Hadoop binaries are required.

 1. Download your desired Apache Hadoop binary release from [https://hadoop.apache.org/releases.html](https://hadoop.apache.org/releases.html).
 1. Extract the contents of the Hadoop distribution archive to a location of your choice, e.g., `/path/to/hadoop` (Unix) or `C:\path\to\hadoop` (Windows).
 1. Set the environment variable `HADOOP_HOME=/path/to/hadoop` (Unix) or `HADOOP_HOME=C:\path\to\hadoop` (Windows). 
 1. **Windows only**: Download a Hadoop winutils distribution corresponding to your Hadoop version from https://github.com/steveloughran/winutils (for newer Hadoop releases at: https://github.com/cdarlint/winutils) and extract the contents to `%HADOOP_HOME%\bin`.
