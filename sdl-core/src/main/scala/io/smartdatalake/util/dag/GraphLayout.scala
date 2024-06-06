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

import org.scalameta.ascii.graph.Graph
import org.scalameta.ascii.layout.coordAssign.{Layouter, ToStringVertexRenderingStrategy, VertexRenderingStrategy}
import org.scalameta.ascii.layout.cycles.CycleRemover
import org.scalameta.ascii.layout.drawing.{EdgeElevator, KinkRemover, RedundantRowRemover, Renderer}
import org.scalameta.ascii.layout.layering.{LayerOrderingCalculator, LayeringCalculator}
import org.scalameta.ascii.layout.prefs.{LayoutPrefs, LayoutPrefsImpl}

/**
 * Code copied from com.github.mdr.ascii.layout.GraphLayout to fix but not using vertexRenderingStrategy.
 */
object GraphLayout {

  /**
   * Layout a graph as a String using toString() on the vertices
   */
  def renderGraph[V](graph: Graph[V]): String =
    renderGraph(graph, ToStringVertexRenderingStrategy, LayoutPrefsImpl())

  def renderGraph[V](
                      graph: Graph[V],
                      vertexRenderingStrategy: VertexRenderingStrategy[V] = ToStringVertexRenderingStrategy,
                      layoutPrefs: LayoutPrefs = LayoutPrefsImpl()
                    ): String = {
    val cycleRemovalResult = CycleRemover.removeCycles(graph)
    val (layering, _) = new LayeringCalculator[V].assignLayers(cycleRemovalResult)
    val reorderedLayering = LayerOrderingCalculator.reorder(layering)
    val layouter = new Layouter(vertexRenderingStrategy, layoutPrefs.vertical) // fixed: ToStringVertexRenderingStrategy was hardcoded here!
    var drawing = layouter.layout(reorderedLayering)

    if (layoutPrefs.removeKinks)
      drawing = KinkRemover.removeKinks(drawing)
    if (layoutPrefs.elevateEdges)
      drawing = EdgeElevator.elevateEdges(drawing)
    if (layoutPrefs.compactify)
      drawing = RedundantRowRemover.removeRedundantRows(drawing)

    if (!layoutPrefs.vertical)
      drawing = drawing.transpose
    Renderer.render(drawing, layoutPrefs)
  }

}
