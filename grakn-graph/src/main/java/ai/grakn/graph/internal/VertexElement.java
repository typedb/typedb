/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * <p>
 *     Represent a Vertex in a Grakn Graph
 * </p>
 *
 * <p>
 *    Wraps a tinkerpop {@link Vertex} constraining it to the Grakn Object Model.
 *    This is used to wrap common functionality between exposed {@link ai.grakn.concept.Concept} and unexposed
 *    internal vertices.
 * </p>
 *
 * @author fppt
 */
class VertexElement extends AbstractElement<Vertex>{
    VertexElement(AbstractGraknGraph graknGraph, Vertex element) {
        super(graknGraph, element);
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param label The type of the edges to retrieve
     * @return A collection of edges from this concept in a particular direction of a specific type
     */
    Stream<EdgeElement> getEdgesOfType(Direction direction, Schema.EdgeLabel label){
        Iterable<Edge> iterable = () -> getElement().edges(direction, label.getLabel());
        return StreamSupport.stream(iterable.spliterator(), false).
                map(edge -> getGraknGraph().getElementFactory().buildEdge(edge));
    }

    /**
     *
     * @param to the target {@link VertexElement}
     * @param type the type of the edge to create
     * @return The edge created
     */
    EdgeElement addEdge(VertexElement to, Schema.EdgeLabel type) {
        return getGraknGraph().getElementFactory().buildEdge(getElement().addEdge(type.getLabel(), to.getElement()));
    }

    /**
     *  @param to the target {@link VertexElement}
     * @param type the type of the edge to create
     */
    EdgeElement putEdge(VertexElement to, Schema.EdgeLabel type){
        ConceptImpl toConcept = (ConceptImpl) to;
        GraphTraversal<Vertex, Edge> traversal = getGraknGraph().getTinkerPopGraph().traversal().V(getElementId().getValue()).outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getId().getRawValue()).select("edge");
        if(!traversal.hasNext()) {
            return addEdge(toConcept, type);
        } else {
            return getGraknGraph().getElementFactory().buildEdge(traversal.next());
        }
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param type The type of the edges to retrieve
     */
    void deleteEdges(Direction direction, Schema.EdgeLabel type){
        getElement().edges(direction, type.getLabel()).forEachRemaining(Edge::remove);
    }

    /**
     * Deletes an edge of a specific type going to a specific {@link VertexElement}
     * @param type The type of the edge
     * @param to The target {@link VertexElement}
     */
    void deleteEdgeTo(Schema.EdgeLabel type, VertexElement to){
        GraphTraversal<Vertex, Edge> traversal = getGraknGraph().getTinkerPopGraph().traversal().V(getElementId().getValue()).
                outE(type.getLabel()).as("edge").otherV().hasId(to.getElementId().getValue()).select("edge");
        if(traversal.hasNext()) {
            traversal.next().remove();
        }
    }
}
