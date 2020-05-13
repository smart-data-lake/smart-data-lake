/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

import java.io.File

/**
 * Command Line Application to check for Kerberos specific parameters
 * Extend this class and call parseKerberosSpecificParameters
 * if you want to force Kerberos specific parameters to be enforced.
 */
abstract class KerberosSmartDataLakeBuilderImpl extends SmartDataLakeBuilder {

  // define additional options for kerberos to parse from command line arguments
  parser.opt[String]('d', "kerberos-domain")
    .action((arg, config) => config.copy(kerberosDomain = Some(arg)))
    .text("Kerberos-Domain for authentication (USERNAME@KERBEROS-DOMAIN).")
  parser.opt[String]('u', "username")
    .action((arg, config) => config.copy(username = Some(arg)))
    .text("Kerberos username for authentication (USERNAME@KERBEROS-DOMAIN).")
  parser.opt[File]('k', "keytab-path")
    .action((arg, config) => config.copy(keytabPath = Some(arg)))
    .text("Path to the Kerberos keytab file for authentication.")

}

object KerberosSmartDataLakeBuilder extends KerberosSmartDataLakeBuilderImpl {

  def main(args: Array[String]): Unit = {
    logger.info(s"Start programm $appType")

    //Get environment variables to check if we have a valid kerberos configuration
    val config = parseCommandLineArguments(args, initConfigFromEnvironment)

    config match {
      case Some(c) =>

        //If local authenticate the application - unnecessary on cluster
        val username = c.username.getOrElse(throw new IllegalArgumentException("Username needs to be set in local mode!"))
        val kerberosDomain = c.kerberosDomain.getOrElse(throw new IllegalArgumentException("Kerberos domain needs to be set in local mode!"))
        val kp = c.keytabPath.map(_.getPath).orElse(Some(ClassLoader.getSystemClassLoader.getResource(s"$username.keytab")).map(_.getPath))
          .getOrElse(throw new IllegalArgumentException("Couldn't find keytab file for authentication."))
        val principal = s"$username@$kerberosDomain"

        if (c.master.contains("local[*]")) AppUtil.authenticate(kp, principal)
        val stats = run(c)
        logger.info(s"$appType v$appVersion finished successfully: $stats")

      case None =>
        logger.error(s"$appType v$appVersion terminated due to an error.")
    }
  }
}