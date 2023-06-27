package io.smartdatalake.util.misc

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore}

/**
 * Upload final state to given url. This is mainly used to upload state to the backend of the UI.
 * Config options:
 * - uploadUrl: url of API endpoint for upload using method=post.
 * - uploadStageDir: optional (but recommended) hadoop directory to save state if upload fails temporarily.
 *   Upload of these files is retried on initialization of next SDLB run.
 */
class FinalStateUploader(options: Map[String,StringOrSecret]) extends StateListener with SmartDataLakeLogger {

  private val uploadUrl = options.getOrElse("uploadUrl", throw new IllegalArgumentException("Option 'uploadUrl' not defined")).resolve()
  private val uploadStagePath = options.get("uploadStagePath").map(_.resolve())
  private val postWsClient = ScalaJWebserviceClient(uploadUrl, additionalHeaders = Map(), timeouts = None, authMode = None, proxy = None, followRedirects = true)
  private var stageStateStore: Option[HadoopFileActionDAGRunStateStore] = None

  logger.info(s"instantiated: uploadUrl=$uploadUrl uploadStagePath=$uploadStagePath")

  override def init(context: ActionPipelineContext): Unit = {
    uploadStagePath.foreach { path =>
      stageStateStore = Some(new HadoopFileActionDAGRunStateStore(path, context.application, context.hadoopConf))
      // check connection
      stageStateStore.get.getLatestRunId
    }
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[SdlConfigObject.ActionId]): Unit = {
    if (state.isFinal) {
      // post state to url, save to uploadStagePath on failure
      postWsClient.post(state.toJson.getBytes("UTF-8"), "application/json")
        .toOption.getOrElse {
          stageStateStore match {
            case Some(store) =>
              logger.info(s"Failed uploading final state to $uploadUrl. Saved it to uploadStagePath for retrying with next run.")
              val file = store.saveStateToFile(state)
            case None =>
              logger.warn(s"Failed uploading final state to $uploadUrl. To avoid loosing state configure FinalStateUploader.options.uploadStagePath.")
          }
        }
    }
  }
}
