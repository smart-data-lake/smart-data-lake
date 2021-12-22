# Troubleshooting

## Windows: missing winutils
Error: `java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries`

Cause: The `winutils.exe` executable can not be found.   
- Download hadoop winutils binaries (e.g https://github.com/cdarlint/winutils/archive/refs/heads/master.zip)  
- Extract binaries for desired hadoop version into folder (e.g. hadoop-3.2.2\bin)  
- Set HADOOP_HOME evironment variable (e.g. HADOOP_HOME=...\hadoop-3.2.2).
  Note that the binary files need to be located at %HADOOP_HOME%\bin!
- Add %HADOOP_HOME%\bin to PATH variable.

## Windows: `/tmp/hive` is not writable / RuntimeException: Error while running command to get file permissions
Solution: Change to `%HADOOP_HOME%\bin` and execute `winutils chmod 777 /tmp/hive`.

## Windows: winutils.exe is not working correctly
Error:  
`winutils.exe - System Error The code execution cannot proceed because MSVCR100.dll was not found. Reinstalling the program may fix this problem.`  
Other errors are also possible:
- Similar error message when double clicking on winutils.exe (Popup)
- Errors when providing a path to the configuration instead of a single configuration file
- ExitCodeException exitCode=-1073741515 when executing SDL even though everything ran without errors

Solution: 
Install VC++ Redistributable Package from Microsoft:  
http://www.microsoft.com/en-us/download/details.aspx?id=5555 (x86)  
http://www.microsoft.com/en-us/download/details.aspx?id=14632 (x64)

## Resources not copied
Symptom: Tests fail due to missing or outdated resources or the execution starts but can not find the feeds specified. IntelliJ might not copy the resource files to the target directory. 

Solution: Execute the maven goal `resources:resources` (`mvn resources:resources`) manually after you changed any resource file.

## Maven compile error: tools.jar
Error Message: `Could not find artifact jdk.tools:jdk.tools:jar:1.7 at specified path ...`

Context: Hadoop/Spark has a dependency on the tools.jar file which is installed as part of the JDK installation.

Possible Reasons:
 1. Your system does not have a JDK installed (only a JRE).
    - Fix: Make sure a JDK is installed and your PATH and JAVA_HOME environment variables are pointing to the JDK installation.
 1. You are using a Java 9 JDK or higher. The tools.jar has been removed in JDK 9. See: https://openjdk.java.net/jeps/220
    - Fix: Downgrade your JDK to Java 8.
