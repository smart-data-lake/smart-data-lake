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
package io.smartdatalake.util.misc

import io.smartdatalake.config.ConfigurationException

import scala.util.Try
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils


private[smartdatalake] object CredentialsUtil extends SmartDataLakeLogger {

  /**
   * Parses the credentials option from config to know what type of credentials to use and the name of the variables
   *
   * @param credentialsConfig contains the type and variables of the credentials like ENV#ENV_VARIABLE_NAME
   */
  def getCredentials(credentialsConfig: String): String = {

    val credentialsInfo = getCredentialsInfo(credentialsConfig)

    credentialsInfo._1 match {
      case "CLEAR" =>
        credentialsInfo._2
      case "ENV" =>
        getCredentialsFromEnv(credentialsInfo._2)
      case "DBSECRET" =>
        getCredentialsFromSecret(credentialsInfo._2)
      case "FILE" =>
        // Provide a property filename and a variable name to look for, separated by a semicolon
        getCredentialsFromFile(credentialsInfo._2)
      //default case when the parameter was not given
      case "" =>
        ""
      case _ =>
        throw new ConfigurationException(s"Credentials type ${credentialsInfo._1} is unknown.")
    }
  }

  def getCredentialsInfo(credentialsConfig: String): (String, String) = {
    logger.info(s"Parsing variable ${credentialsConfig}")
    credentialsConfig.split("#") match {
      case Array(x, y) => (x, y)
      case _ => throw new ConfigurationException(s"Credentials config variable $credentialsConfig is invalid, make sure it's of the form CREDENTIALSTYPE#VARIABLENAME")
    }
  }

  def getCredentialsFromEnv(variableName: String): String = {
    logger.info(s"Trying to get credentials from environment variable $variableName")
    sys.env.get(variableName) match {
      case Some(key) => key
      case _ => throw new ConfigurationException(s"Environment variable $variableName not found")
    }
  }

  def getCredentialsFromSecret(variableString: String): String = {
    logger.info(s"Trying to split variable string $variableString")
    variableString.split("\\.") match {
      case Array(x, y) =>
          try {
            val secretKey: String = dbutils.secrets.get(scope = x, key = y)
            if(secretKey.isEmpty){
              throw new ConfigurationException(s"Databricks secret key $y in scope $x is an empty string")
            } else secretKey
          } catch {
            case e: Exception => {
              throw new ConfigurationException(s"Databricks secret $y doesn't exist in scope $x. Exception: ${e.toString}")
            }
          }
      case _ => throw new ConfigurationException(s"Databricks secret variable string $variableString is invalid, make sure it's of the form SCOPE.VARIABLENAME")
    }
  }

  def getCredentialsFromFile(fileAndVariableName: String): String = {
    assert(fileAndVariableName.split(";").length == 2, s"Expected semicolon separated filename and variable, got $fileAndVariableName")
    val Array(file, variable) = fileAndVariableName.split(";")
    val props = TryWithRessource.execSource(scala.io.Source.fromFile(file))(_.getLines.toSeq)
    Try(props.find(_.startsWith(variable+"=")).get.split("=")(1)).getOrElse(throw new ConfigurationException(s"Variable $variable in file $file not found"))
  }




}
