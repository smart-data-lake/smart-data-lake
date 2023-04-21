/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.smartdatalake.app

import io.smartdatalake.app.DatabricksSmartDataLakeBuilder.parser
import io.smartdatalake.config.ConfigurationException
import scopt.OParser

/*
 * AWS Glue Smart Data Lake Command Line Application.
 *
 * Glue passes all job parameters (command line arguments) down to the application. This application filters out the
 *   Glue specific ones.
 * SCOPT issue with Glue 4.0:
 * Since SmartDataLakeBuilder Version 2.5.0 we use scopt Version 4.0.1 in order to parse command line arguments.
 Unfortunately, Glue 4.0 overwrites the library scopt with an older version by changing the classpath of the boot classloader using a Vm Option that looks like -Xbootclasspath/a:"/path/to/scopt_2.12-3.7.1.jar"
 You are affected if you get an error message like this:
 Exception in User Class: java.lang.NoSuchMethodError : scopt.OptionDef.<init>(Lscopt/OptionDefKind;Ljava/lang/String;Lscopt/Read;)V
 
 * To fix the issue, you basically need to make sure scopt 4 is loaded, effectively undoing the -Xbootclasspath/a option:
 *  - place the newer scopt jar an s3 bucket. You can get it here https://mvnrepository.com/artifact/com.github.scopt/scopt_2.12/4.0.1,
 *  - specify the path to it in the job property `Dependent JARs path` and 
 *  - set Job parameter: `--user-jars-first` with value `true`
 */
class GlueSmartDataLakeBuilder extends SmartDataLakeBuilder {

  def parseAndRun(args: Array[String], ignoreOverrideJars: Boolean = false): Unit = {
    logger.info(s"Starting Program $appType $appVersion")

    // filter the CLI arguments
    def rmArg(sel: List[String], list: List[String]): List[String] = {
      list match {
        case Nil => sel
        case "--additional-python-modules" :: _ :: tail =>
          rmArg(sel, tail)
        case "--class" :: _ :: tail =>
          rmArg(sel, tail)
        case "--continuous-log-conversionPattern" :: _ :: tail =>
          rmArg(sel, tail)
        case "--continuous-log-logGroup" :: _ :: tail =>
          rmArg(sel, tail)
        case "--continuous-log-logStreamPrefix" :: _ :: tail =>
          rmArg(sel, tail)
        case "--datalake-formats" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-auto-scaling" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-continuous-cloudwatch-log" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-continuous-log-filter" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-glue-datacatalog" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-job-insights" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-metrics" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-rename-algorithm-v2" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-s3-parquet-optimized-committer" :: _ :: tail =>
          rmArg(sel, tail)
        case "--enable-spark-ui" :: _ :: tail =>
          rmArg(sel, tail)
        case "--extra-files" :: _ :: tail =>
          rmArg(sel, tail)
        case "--extra-jars" :: _ :: tail =>
          rmArg(sel, tail)
        case "--extra-py-files" :: _ :: tail =>
          rmArg(sel, tail)
        case "--job-bookmark-option" :: _ :: tail =>
          rmArg(sel, tail)
        case "--job-language" :: _ :: tail =>
          rmArg(sel, tail)
        case "--python-modules-installer-option" :: _ :: tail =>
          rmArg(sel, tail)
        case "--scriptLocation" :: _ :: tail =>
          rmArg(sel, tail)
        case "--spark-event-logs-path" :: _ :: tail =>
          rmArg(sel, tail)
        case "--TempDir" :: _ :: tail =>
          rmArg(sel, tail)
        case "--use-postgres-driver" :: _ :: tail =>
          rmArg(sel, tail)
        case "--user-jars-first" :: _ :: tail =>
          rmArg(sel, tail)
        case "--conf" :: _ :: tail =>
          rmArg(sel, tail)

        case "--JOB_ID" :: _ :: tail =>
          rmArg(sel, tail)
        case "--JOB_RUN_ID" :: _ :: tail =>
          rmArg(sel, tail)
        case "--JOB_NAME" :: _ :: tail =>
          rmArg(sel, tail)
        case "true" :: tail =>
          rmArg(sel, tail)

        case string :: tail =>
          logger.debug("while filtering AWS GLUE options, found option for SDLB:" + string)
          rmArg(sel :+ string, tail)
      }
    }

    val options: Seq[String] = rmArg(List[String](), args.toList)
    logger.info(s"Filtered command line args: +${options.mkString(";")}")

    OParser.parse(parser, options, SmartDataLakeBuilderConfig()) match {
      case Some(config) =>
        assert(config.overrideJars.isEmpty || ignoreOverrideJars, "Option override-jars is not supported by DefaultSmartDataLakeBuilder. Use DatabricksSmartDataLakeBuilder for this option.")
        val stats = run(config)
          .toSeq.sortBy(_._1).map(x => x._1 + "=" + x._2).mkString(" ") // convert stats to string
        logger.info(s"$appType finished successfully: $stats")
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}

object GlueSmartDataLakeBuilder {

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    val app = new GlueSmartDataLakeBuilder
    app.parseAndRun(args)
  }
}