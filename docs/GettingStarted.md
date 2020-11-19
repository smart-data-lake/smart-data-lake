# Getting Started

The following page will show you how to build the project and run your first feed. 
For a more comprehensive explanation of all options, see the [Reference](Reference.md). 

To run a first example, you need the jar file (see [Build](#Build)), an application.conf and some sample data. 

## Build
Smart Data Lake Builder is build using [Apache Maven](https://maven.apache.org/). 
To build the project, simply clone the GitHub repository and run `mvn install`.
This will resolve all dependencies and build a JAR file to run Smart Data Lake Builder. 

### Build Dependencies
Version 1.x
- *Spark 2.4*
- JDK 8 (Spark 2 doesn't support JDK 9 or higher)
- Scala 2.11 or 2.12
- Maven 3.0 (or higher)
Version 2.x
- *Spark 3.x*
- JDK >= 8
- Scala 2.12 (Spark 3 doesn't support scala 2.11 anymore)
- Maven 3.0 (or higher)

### Building JAR with Runtime Dependencies
To generate a JAR file which includes all dependencies, use the profile **fat-jar** which is defined in the pom.xml. Run Maven with `mvn -Pfat-jar package` in this case.
If you don't want to handle dependencies yourself, build Smart Data Lake Builder with this profile now and use it to run the following examples.
You may also choose to activate the profiles hadoop-3.2 (only for Spark 3.x, default ist Hadoop 2.7) and scala-2.11 or scala-2.12 (only for Spark 2.4).

## Configure
To run a first example, you need to create a file called **application.conf** in a folder we will call **resource folder** from now on.
Use the following content for your file:

```
dataObjects { 
  ab-csv-org {
    type = CsvFileDataObject
    path = "AB_NYC_2019.csv"
  }
  ab-excel {
    type = ExcelFileDataObject
    path = "~{id}/AB_NYC_2019.xlsx"
    excel-options {
      sheet-name = csvdata
    }
  }
}

actions {
  getting-started {
    type = CopyAction
    inputId = ab-csv-org
    outputId = ab-excel
    metadata {
      feed = getting-started
    }  
  }
}
```

Explanation:  
We defined two data objects in the application.conf.
The first one has an id of **ab-cvs-org** and represents the CSV sample file we will use. 
It has a path (with filename) where the file is stored and defines some CSV options like the delimiter used and whether the file has a header or not.  
The second data object has an id of **ab-excel** and represents an Excel file.
Again, with a path to the Excel file and some options, in this case the name of the Excel sheet.
Note that we used a placeholder **~{id}** in the pathname that will get substituted at runtime with the id of the data object.

Next, we define one action that reads from the CSV file (inputId) and writes an Excel file (outputId). 
inputId and outputId reference the ids of the data objects defined above. 
The type is set to CopyAction so the data will simply be copied without any transformations. 
Additionally, we defined some metadata on this action, namely a feed name. 
Feed names are used mainly for selecting the tasks to run and to organize your metadata.

As a result, this action will read the data from a CSV file and save them as an Excel file.  

## Sample data
You can use any CSV file for this first example. 
We used an open data set for these tests which you can download from [Kaggle](https://www.kaggle.com/dgomonov/new-york-city-airbnb-open-data).

## Run
1. For simplicity, put all files in one working directory: The smartdatalake-jar-with-dependencies, the application.conf and the sample CSV file.
1. Execute the feed: 
    
    `java -jar smartdatalake-jar-with-dependencies.jar --feed-sel getting-started`
   
1. Check to see if the target folder was created and contains the Excel file.   

Note, that you need to run the jar file with Java 8 (or higher). 
   
Use `--help` to see additional command line parameters and options.

The `-c` option for example can be used to define one ore more locations of your configurations files, if SDLB should not look for the application.conf resource in your classpath.
You can define configuration files directly or directories which contain configuration files.
If a directory is given, all configuration files found in this directory and it's subdirectories will be merged together.   

If you placed the CSV file in another directory, you need to define it's location in your application.conf instead of just providing the name of the CSV file.

That's it!   
You just successfully ran your first Smart Data Lake feed.

## Where to go from here
### Reference
Smart Data Lake Builder comes with many types of Data Objects and Actions.
Check the [Reference](Reference.md) to see what is implemented already and check back as more types will be implemented.

### Examples
There's a separate repository you should check out that contains many examples of Data Object and Action configurations:  
https://github.com/smart-data-lake/sdl-examples

### Get in touch
If you have issues, comments or feedback, please see [Contributing](Contribute.md) on how to get in touch.
