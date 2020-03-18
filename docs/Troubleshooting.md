# Troubleshooting

## Windows: missing winutils
Error: `java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries`

Cause: The `winutils.exe` executable can not be found. Refer to [Runtime Dependencies](../README.md#runtime-dependencies) for installation instructions.

## Windows: `/tmp/hive` is not writable
Solution: Change to `%HADOOP_HOME%\bin` and execute `winutils chmod 777 /tmp/hive`.

## Resources not copied
Symptom: Tests fail due to missing or outdated resources. IntelliJ might not copy the resource files to the target directory. 

Solution: Execute the maven goal `resources:resources` (`mvn resources:resources`) manually after you changed any resource file.
