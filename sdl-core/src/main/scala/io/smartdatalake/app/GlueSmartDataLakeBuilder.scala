/*
 * sdl-core - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

import scopt.{DefaultOParserSetup, OParser}

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
object GlueSmartDataLakeBuilder extends SmartDataLakeBuilder {

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logProgramStart()

    // Ignore the arguments we don't recognize as AWS Glue provides many unexpected arguments
    val setup = new DefaultOParserSetup {
      override def errorOnUnknownArgument: Boolean = false
    }

    OParser.parse(parser, args, SmartDataLakeBuilderConfig(), setup) match {
      case Some(config) =>
        val stats = run(config)
        logStats(stats)
      case None =>
        throwOParserError()
    }
  }
}