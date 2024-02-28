/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

import java.time.Duration

/**
 * Interface to be implemented by connection pools in SDLB.
 */
abstract class ConnectionPool[T] {
  def borrowObject: T
  def returnObject(connection: T): Unit
}

/**
 * Default implementation of a ConnectionPool wrapping Apache commons-pool2.
 */
class DefaultConnectionPool[T](maxTotal: Int, maxIdleTimeSec: Int, maxWaitTimeSec: Int, createConnection: Unit => T, closeConnection: T => Unit) extends ConnectionPool[T] {

  // use apache commons-pool2 as connection pool
  val pool = new GenericObjectPool[T](new BasePooledObjectFactory[T] {
    override def create(): T = createConnection()
    override def wrap(con: T): PooledObject[T] = new DefaultPooledObject(con)
    override def destroyObject(p: PooledObject[T]): Unit = closeConnection(p.getObject)
  })
  pool.setMaxTotal(maxTotal)
  pool.setMinEvictableIdle(Duration.ofSeconds(maxIdleTimeSec)) // timeout to close jdbc connection if not in use
  pool.setMaxWait(Duration.ofSeconds(maxWaitTimeSec))

  override def borrowObject: T = {
    pool.borrowObject()
  }

  override def returnObject(connection: T): Unit = {
    pool.returnObject(connection)
  }
}
