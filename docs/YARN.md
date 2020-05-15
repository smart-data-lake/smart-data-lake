# Run on YARN

Smart Data Lake can be easily executed on a YARN cluster by spark-submit.
The following steps will show you how to set everything up and start a first data load.
See [Running Spark on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html) for detailed Spark configuration options.

1.  Build the project (with activated profile fat-jar) if you haven't done that already: 
    ```bash
    mvn package -DskipTests -Pscala-2.11 -Pfat-jar)
    ```

1.  Copy test data file to hdfs home directory:
    ```bash
    hdfs dfs -put src/test/resources/AB_NYC_2019.csv
    ```

1.  Create an application.conf file:
    ```hocon
    dataObjects {
      ab-csv1 {
        type = CsvFileDataObject
        path = "AB_NYC_2019.csv"
      }
      ab-csv2 {
        type = CsvFileDataObject
        path = "AB_NYC_copy.csv"
      }
    }
    
    actions {
      loadCsv2Csv {
        type = CopyAction
        inputId = ab-csv1
        outputId = ab-csv2
        metadata {
          feed = ab-csv
        }
      }
    }
    ```

1.  Submit application to YARN cluster with spark-submit (make sure spark-submit version is 2.x and scala minor version is 2.11).
    Dont forget to replace the SmartDataLake version (2x).
    ```bash
    spark-submit --master yarn --deploy-mode client --jars target/smartdatalake_2.11-1.0.3-jar-with-dependencies.jar --class io.smartdatalake.app.DefaultSmartDataLakeBuilder target/smartdatalake_2.11-1.0.3-jar-with-dependencies.jar --feed-sel ab-csv -c file://`pwd`/application.conf
    ```