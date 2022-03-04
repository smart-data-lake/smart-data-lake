package io.smartdatalake.meta.dagexporter

import io.smartdatalake.config.{ConfigToolbox, ConfigurationException, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.Action
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.writePretty
import scopt.OptionParser

case class DagExporterConfig(sdlConfigPaths: String = null)

object DagExporter extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  val instanceRegistry: InstanceRegistry = new InstanceRegistry()

  protected val parser: OptionParser[DagExporterConfig] = new OptionParser[DagExporterConfig]("DagExporter") {
    override def showUsageOnError: Option[Boolean] = Some(true)

    opt[String]('s', "sdlConfig")
      .required()
      .action((config, c) => c.copy(sdlConfigPaths = config))
      .text("One or multiple configuration files or directories containing configuration files for the sdlBuilder, separated by comma.")
    help("help").text("Display the help text.")
  }

  /**
   * Takes as input a SDL Config and prints it's containing Actions to STDOUT, in a simplified JSON-Format.
   */
  def main(args: Array[String]): Unit = {

    val config = DagExporterConfig()
    // Parse all command line arguments
    parser.parse(args, config) match {
      case Some(config) =>
        val dagAsJSON = exportConfigDagToJSON(config)
        println("BEGIN DAG")
        print(dagAsJSON)
        println("END DAG")
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  private def toSimplifiedAction(action: Action): SimplifiedAction = {
    SimplifiedAction(action.metadata, action.inputs.map(_.id.id), action.outputs.map(_.id.id))
  }

  def exportConfigDagToJSON(config: DagExporterConfig): String = {
    val (registry, _) = ConfigToolbox.loadAndParseConfig(config.sdlConfigPaths.split(','))
    val simplifiedActions: Map[String, SimplifiedAction] = registry.getActions.groupBy(action => action.id.id).mapValues(action => toSimplifiedAction(action.head))
    writePretty(simplifiedActions)(Serialization.formats(NoTypeHints))
  }
}
