/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.dag

import io.smartdatalake.util.misc.LogUtil
import org.scalameta.ascii.common.Dimension
import org.scalameta.ascii.layout.coordAssign.VertexRenderingStrategy

/**
 * Render a node by centering it horizontally and vertically within the given region.
 * Use a function to get text to render.
 *
 * Code copied from com.github.mdr.ascii.layout.coordAssign.ToStringVertexRenderingStrategy and adapted.
 */
class NodeRenderingStrategy(nodeToString: DAGNode => String) extends VertexRenderingStrategy[DAGNode] {

  def getPreferredSize(v: DAGNode): Dimension = {
    val lines = LogUtil.splitLines(nodeToString(v))
    Dimension(lines.size, if (lines.isEmpty) 0 else lines.map(_.size).max)
  }

  def getText(v: DAGNode, allocatedSize: Dimension): List[String] = {
    val unpaddedLines = LogUtil.splitLines(nodeToString(v)).take(allocatedSize.height).map { line => centerLine(allocatedSize, line) }
    val verticalDiscrepancy = Math.max(0, allocatedSize.height - unpaddedLines.size)
    val verticalPadding = List.fill(verticalDiscrepancy / 2)("")
    verticalPadding ++ unpaddedLines ++ verticalPadding
  }

  private def centerLine(allocatedSize: Dimension, line: String): String = {
    val discrepancy = allocatedSize.width - line.size
    val padding = " " * (discrepancy / 2)
    padding + line
  }

}
