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
package io.smartdatalake.workflow.action.generic.transformer

import org.scalameta.ascii.graph.Graph

class BinaryDecisionDiagramTransformer {

}

object BinaryDecisionDiagram extends Serializable {


  private type NodeId = String
  private type NodeLabel = Any
  private type NodeDescription = String


  /**
   * Vertices used when converting a BinaryDecisionDiagram to a graph.
   */
  abstract class LabelledVertex {
    def vertexLabel: String = this.toString
  }


  /**
   * A simple LabelledVertex with an id which also serves as its label.
   *
   * @param id The id of the vertex.
   */
  case class IdVertex(id: Any) extends LabelledVertex {
    override def vertexLabel: String = id.toString
  }


  /**
   * A simple LabelledVertex whose label differs form its id.
   *
   * The library for converting graphs to an ascii-representation does not support edge labels. As a workaround,
   * we use additional vertices of type IdVertexWithLabel instead of edge labels.
   * We distinguish the edges with an id and allow different edges to have the same label.
   *
   * @param id    The id of the vertex.
   * @param label The label of the vertex.
   */
  case class IdVertexWithLabel(id: Any, label: Any) extends LabelledVertex {
    override def vertexLabel: String = label.toString
  }


  /**
   * A LabelledVertex whose label is constructed from its id and additional information.
   *
   * @param id    The id of the vertex.
   * @param addOn Additional information used to construct the label of the vertex.
   */
  case class IdVertexWithCompositeLabel(id: Any, addOn: Any) extends LabelledVertex {
    override def vertexLabel: String = id.toString + ":\n" + addOn.toString
  }


  /**
   * Binäres Entscheidungsdiagramm.
   * (Kann nicht nur für Bäume, sondern allgemeiner auch für DAGs (directed acyclic graphs) verwendet werden.)
   */
  trait BinaryDecisionDiagram[-A, +B <: LabelledVertex] {
    // Code adapted from https://www.vincent-lunot.com/post/programming-a-decision-tree-predictor-in-scala-part-1/
    // [28.10.2021].

    /**
     * Jagt ein Sample durch den Entscheidungsbaum.
     *
     * @param sample   Sample, für das Entscheidung ermittelt werden soll.
     * @param parentId ID des aufrufenden Nodes. Wird benötigt, um nachzuvollziehen, von welchem Node die Entscheidung
     *                 getroffen wurde.
     * @return Getroffene Entscheidung zusammen mit der ID des aufrufenden Nodes.
     */
    def diagnose(sample: A, parentId: NodeId): (B, NodeId)

    /**
     * Erstellt eine Beschreibung aller Nodes im Entscheidungsdiagramm.
     *
     * @return Eine Map mit den IDs der Nodes als Keys und ihren Beschreibungen und Labels als Value.
     */
    def summary: Map[NodeId, (NodeDescription, NodeLabel)]

    /**
     * Repräsentation des aktuellen Nodes oder Leafs als Vertex eines Graphen.
     */
    def vertex: LabelledVertex

    /**
     * Repräsentation des BinaryDecisionDiagram als Graphen.
     */
    def graph(): Graph[LabelledVertex]

    /**
     * Repräsentation des BinaryDecisionDiagram als Graphen, mit zusätzlicher vorgeschalteter Kante mit Label zu einem
     * Vorgängervertex.
     *
     * @param parentVertex Vertex der dem Graphen vorgeschaltet werden soll.
     * @param edgeLabel    Label der vorgeschalteten Kante.
     */
    def graph(parentVertex: LabelledVertex, edgeLabel: LabelledVertex): Graph[LabelledVertex] = {
      val g = graph()
      val vertices = g.vertices + edgeLabel + parentVertex
      val edges = g.edges :+ (parentVertex, edgeLabel) :+ (edgeLabel, vertex)
      Graph(vertices, edges)
    }
  }


  /**
   * Leaves definieren Endpunkte im Diagramm und damit die eigentlichen Entscheidungen.
   *
   * @param decision Die Entscheidung, die von diesem Leaf retourniert wird.
   */
  case class Leaf[A, B <: LabelledVertex](decision: B) extends BinaryDecisionDiagram[A, B] {
    def diagnose(sample: A, parentId: NodeId): (B, NodeId) = (decision, parentId)

    def summary = Map.empty[NodeId, (NodeDescription, NodeLabel)]

    def vertex: LabelledVertex = decision

    def graph(): Graph[LabelledVertex] = {
      // Graph besteht nur aus der decision als LabelledVertex und hat keine Kanten:
      Graph(Set(vertex), List.empty[(LabelledVertex, LabelledVertex)])
    }
  }

  /**
   * Knoten definieren Verzweigungen im Entscheidungsdiagramm.
   *
   * @param id          ID des Nodes. Wird benötigt um nachzuvollziehen, von welchem Node die Entscheidung getroffen wurde.
   * @param label       Eine kurze Erläuterung zum Node, die in der Repräsentation als Graph angezeigt wird.
   * @param description Eine Beschreibung, was der Node, prüft.
   * @param test        Testfunktion, deren Ergebnis entscheidet, wie man sich im Diagram weiterbewegt.
   * @param left        Nächstes Leaf oder nächster Node, falls das Testergebnis true ist.
   * @param right       Nächstes Leaf oder nächster Node, falls das Testergebnis false ist.
   */
  case class Node[A, B <: LabelledVertex](id: NodeId, description: NodeDescription, label: NodeLabel,
                                          test: A => Boolean, left: BinaryDecisionDiagram[A, B],
                                          right: BinaryDecisionDiagram[A, B]
                                         ) extends BinaryDecisionDiagram[A, B] {

    def diagnose(sample: A, parentId: NodeId): (B, NodeId) = {
      if (test(sample)) left.diagnose(sample, id)
      else right.diagnose(sample, id)
    }

    def summary: Map[NodeId, (NodeDescription, NodeLabel)] = {
      left.summary ++ right.summary + (id -> (description, label))
    }

    def vertex: IdVertexWithCompositeLabel = IdVertexWithCompositeLabel(id, label)

    // Graph des linken Teils des Diagramms mit zusätzlicher vorgeschalteter Kante zu einem Vertex für diesen Node:
    def leftGraph: Graph[LabelledVertex] = left.graph(vertex, IdVertexWithLabel(id + "T", "true"))

    def rightGraph: Graph[LabelledVertex] = right.graph(vertex, IdVertexWithLabel(id + "F", "false"))

    def graph(): Graph[LabelledVertex] = {
      // Graph besteht aus der Vereinigung von leftGraph und rightGraph:
      val vertices = leftGraph.vertices ++ rightGraph.vertices
      val edges = leftGraph.edges ++ rightGraph.edges
      Graph(vertices, edges)
    }
  }
}
