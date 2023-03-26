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

import java.io.Closeable

import org.apache.commons.pool2.impl.GenericObjectPool

import scala.io.Source

private[smartdatalake] object WithResource {
  /**
   * tries executing some function and closes the resource afterward also on exceptions
   */
  def exec[A <: Closeable, B](resource: A)(func: A => B): B = {
    try {
      val result = func(resource)
      result
    } finally {
      resource.close()
    }
  }

  /**
   * tries executing some function and closes the resource afterward also on exceptions
   */
  def execSource[A <: Source, B](resource: A)(func: A => B): B = {
    try {
      val result = func(resource)
      result
    } finally {
      resource.close()
    }
  }
}

private[smartdatalake] object WithResourcePool {
  /**
   * tries executing some function and returns the ressource afterwards to the pool
   */
  def exec[A, B](pool: GenericObjectPool[A])(func: A => B): B = {
    val resource = pool.borrowObject()
    try {
      val result = func(resource)
      result
    } finally {
      pool.returnObject(resource)
    }
  }
}

