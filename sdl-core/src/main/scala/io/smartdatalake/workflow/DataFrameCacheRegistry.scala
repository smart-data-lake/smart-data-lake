/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger

import scala.collection.mutable

/**
 * Keeps track of DataFrames cached by Actions with cacheOutput=true, so that they can be released again
 * once no Action needs them anymore.
 *
 * Note that the registry holds the cached SubFeed itself and not just a counter. This is needed because
 * engines like Snowpark materialize eagerly and return a *new* DataFrame from `cache`, so the cached
 * DataFrame is not reachable through the SubFeeds flowing through the DAG.
 *
 * The registry is shared between the init and the exec phase of a run: consumers are registered during
 * init, the caches are created and released during exec.
 */
private[smartdatalake] class DataFrameCacheRegistry extends SmartDataLakeLogger {

  private val consumers = mutable.Map[DataObjectId, Seq[ActionId]]()
  private val caches = mutable.Map[DataObjectId, DataFrameSubFeed]()

  /**
   * Init phase: remember that `actionId` reads the DataFrame of `dataObjectId`.
   */
  def registerConsumer(dataObjectId: DataObjectId, actionId: ActionId): Unit = synchronized {
    consumers.update(dataObjectId, consumers.getOrElse(dataObjectId, Seq()) :+ actionId)
  }

  /**
   * Exec phase: true if any Action still needs to read the DataFrame of `dataObjectId`.
   */
  def isReused(dataObjectId: DataObjectId): Boolean = synchronized {
    consumers.get(dataObjectId).exists(_.nonEmpty)
  }

  /**
   * Number of Actions which still need to read the DataFrame of `dataObjectId`.
   */
  def consumerCount(dataObjectId: DataObjectId): Int = synchronized {
    consumers.get(dataObjectId).map(_.size).getOrElse(0)
  }

  /**
   * true if a cached DataFrame is currently held for `dataObjectId`.
   */
  def isCached(dataObjectId: DataObjectId): Boolean = synchronized {
    caches.contains(dataObjectId)
  }

  /**
   * Exec phase: remember a cached SubFeed so that it can be released later.
   */
  def register(subFeed: DataFrameSubFeed): Unit = synchronized {
    caches.update(subFeed.dataObjectId, subFeed)
  }

  /**
   * Exec phase: `actionId` is done reading `dataObjectId`. Releases the cache if it was the last consumer.
   */
  def releaseConsumer(dataObjectId: DataObjectId, actionId: ActionId): Unit = synchronized {
    val remaining = consumers.getOrElse(dataObjectId, Seq()).diff(Seq(actionId))
    consumers.update(dataObjectId, remaining)
    if (remaining.isEmpty) release(dataObjectId)
  }

  /**
   * Release all remaining caches. This must also be called if the DAG failed, otherwise caches of Actions
   * whose consumers never ran would be kept until the engine session is closed.
   */
  def releaseAll(): Unit = synchronized {
    caches.keys.toSeq.foreach(release)
  }

  /**
   * Reset the registry at the start of a run, e.g. for the next iteration in streaming mode.
   */
  def reset(): Unit = synchronized {
    releaseAll()
    consumers.clear()
  }

  private def release(dataObjectId: DataObjectId): Unit = {
    caches.remove(dataObjectId).foreach { subFeed =>
      logger.info(s"Releasing cached DataFrame for $dataObjectId")
      subFeed.uncache
    }
  }
}
