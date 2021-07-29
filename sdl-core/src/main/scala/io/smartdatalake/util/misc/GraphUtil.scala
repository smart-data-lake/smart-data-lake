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

package io.smartdatalake.util.misc

/**
 * Helper functions for Graph traversals and algorithms.
 */
private[smartdatalake] object GraphUtil {

  /**
   * A simple directed graph defined by a list of edges (pairs of connected nodes)
   */
  case class Graph[A](edges: Set[(A,A)]) {

    /**
     * Map of inbound connected nodes per node from a list of edges.
     */
    private lazy val outboundEdgeMap: Map[A, Set[A]] = edges.groupBy(_._1).mapValues(_.map(_._2))

    /**
     * Map of outbound connected nodes per node from a list of edges.
     */
    private lazy val inboundEdgeMap: Map[A, Set[A]] = edges.groupBy(_._2).mapValues(_.map(_._1))

    /**
     * Search all connected nodes from a given node
     * @param startNode the node to start the search for connected nodes. This node is included in the result.
     * @param edgeMap Map of connected nodes per node
     * @param visitedNodes internal state to avoid visiting nodes multiple times
     */
    private def getConnectedNodes(startNode: A, edgeMap: Map[A,Set[A]], visitedNodes: Set[A] = Set()): Set[A] = {
      val newVisitedNodes = visitedNodes + startNode
      val nextNodes = edgeMap.getOrElse(startNode, Set()).diff(newVisitedNodes)
      newVisitedNodes ++ nextNodes.flatMap(a => getConnectedNodes(a, edgeMap, newVisitedNodes))
    }

    /**
     * Search all connected nodes from a given node in the direction the graph is defined
     */
    def getConnectedNodesForward(startNode: A): Set[A] = {
      getConnectedNodes(startNode, outboundEdgeMap)
    }

    /**
     * Search all connected nodes from a given node in reverse direction of the graphs definition
     */
    def getConnectedNodesReverse(startNode: A): Set[A] = {
      getConnectedNodes(startNode, inboundEdgeMap)
    }

  }

}
