package io.smartdatalake.util.misc

import io.smartdatalake.app.StateListener
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.UploadDefaults.uploadMimeType
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.ScalaJWebserviceClient
import io.smartdatalake.workflow.ActionDAGRunState.toJson
import io.smartdatalake.workflow.action.{ExecutionId, RuntimeEventState}
import io.smartdatalake.workflow.{ActionDAGRunState, ActionPipelineContext, HadoopFileActionDAGRunStateStore}
import org.apache.hadoop.fs.FileSystem

import scala.collection.mutable
import scala.util.Failure

/**
 * Upload final state to given baseUrl. This is mainly used to upload state to the backend of the UI.
 * Config options:
 * - baseUrl: API base URL for upload. Add tenant, repo (required), env and version as query parameters, e.g. https://<host>?tenant=<tenant>&repo=<repository>&env=<environment>"
 *   Default for tenant is "PrivateTenant". For this case the API should use the userId of authenticated user as tenant.
 *   Default for environment is 'std'.
 * - uploadStageDir: optional (but recommended) hadoop directory to save state if upload fails temporarily.
 *   Upload of these files is retried on initialization of next SDLB run.
 * - processUpdates: optional; if false, only final state is uploaded, otherwise all state updates are uploaded as partial updates.
 *   Default is processUpdates=true.
 */
class StateUploader(options: Map[String,StringOrSecret]) extends StateListener with SmartDataLakeLogger {

  private val baseUrl = options.getOrElse("baseUrl", throw new IllegalArgumentException("Option 'baseUrl' not defined")).resolve()
  private val stagePath = options.get("stagePath").map(_.resolve())
  private val processUpdates = options.get("processUpdates").map(_.resolve().toBoolean).getOrElse(true)
  private val defaultUploadCategoryParams = Map(
    "tenant" -> (if (!baseUrl.contains("tenant=")) Some(UploadDefaults.privateTenant) else None),
    "env" -> (if (!baseUrl.contains("env=")) Some(UploadDefaults.envDefault) else None)
  ).filter(_._2.nonEmpty).mapValues(_.get)
  private val uploadWsClient = ScalaJWebserviceClient(URIUtil.appendPath(baseUrl, "state"), additionalHeaders = Map(), timeouts = None, authMode = None, proxy = None, followRedirects = true)
  private[smartdatalake] var stageStateStore: Option[HadoopFileActionDAGRunStateStore] = None
  private val uploadedExecutionIds = mutable.Set[ExecutionId]()

  assert(baseUrl.contains("repo="), throw new IllegalArgumentException(s"repository not defined in upload baseUrl=$baseUrl, add query parameter 'repo' to baseUrl, e.g. https://<host>?repo=<repository>"))

  logger.info(s"instantiated: baseUrl=$baseUrl stagePath=$stagePath processUpdates=$processUpdates")

  override def prepare(context: ActionPipelineContext): Unit = {
    assert(context.appConfig.applicationName.nonEmpty, "Application name must be set on command line (--name) for using StateUploader.")
    // reset state
    uploadedExecutionIds.clear()
    // retry failed uploads from previous runs
    stagePath.foreach { path =>
      stageStateStore = Some(new HadoopFileActionDAGRunStateStore(path, context.application, context.hadoopConf))
      val stagedStates = stageStateStore.get.getFiles()
      implicit val filesystem: FileSystem = stageStateStore.get.filesystem
      try { // stop on first upload error
        stagedStates.foreach { file =>
          val body = HdfsUtil.readHadoopFile(file.path)
          logger.debug(s"POST: params=$defaultUploadCategoryParams body=$body")
          uploadWsClient.post(body.getBytes("UTF-8"), uploadMimeType, defaultUploadCategoryParams)
            .recoverWith{ case ex => Failure(new IllegalStateException(s"Failed on retrying to upload staged state file $file to $baseUrl: ${ex.getMessage}", ex))}
            .get
          filesystem.delete(file.path, false)
        }
      } catch {
        case ex: Throwable => logger.error(ex.getMessage)
      }
    }
  }

  override def notifyState(state: ActionDAGRunState, context: ActionPipelineContext, changedActionId: Option[SdlConfigObject.ActionId]): Unit = {
    if (state.isFinal || !uploadedExecutionIds.contains(context.executionId)) {
      // if final notification or first notification for executionId, upload full state to baseUrl or save to stagePath on failure
      uploadedExecutionIds.add(context.executionId)
      val body = toJson(state)
      logger.debug(s"POST: params=$defaultUploadCategoryParams body=$body")
      uploadWsClient.post(state.toJson.getBytes("UTF-8"), uploadMimeType, defaultUploadCategoryParams)
        .toOption.getOrElse {
          stageStateStore match {
            case Some(store) =>
              logger.info(s"Failed uploading final state to $baseUrl. Saved it to stagePath for retrying with next run.")
              store.saveStateToFile(state)
            case None =>
              logger.warn(s"Failed uploading final state to $baseUrl. To avoid loosing state configure FinalStateUploader.options.stagePath.")
          }
        }
    } else {
      // if intermediate notification, upload only changed action info to baseUrl
      if (processUpdates && changedActionId.isDefined) {
        // additional query parameters needed to identify run/attempt
        val runParams = Map(
          "application" -> context.appConfig.applicationName.get,
          "runId" -> context.executionId.runId.toString,
          "attemptId" -> context.executionId.attemptId.toString,
          "actionId" -> changedActionId.get.id
        )
        // upload changed action info
        val body = ActionDAGRunState.toJson(state.actionsState(changedActionId.get))
        logger.debug(s"PATCH: params=${defaultUploadCategoryParams ++ runParams} body=$body")
        uploadWsClient.patch(body.getBytes("UTF-8"), uploadMimeType, defaultUploadCategoryParams ++ runParams)
      }
    }
  }
}

private[smartdatalake] object UploadDefaults {
  val privateTenant = "PrivateTenant" // this tenant should be translated to the userId of the authenticated user through the API.
  val envDefault = "std"
  val versionDefault = "latest"
  val uploadMimeType = "application/json"
}