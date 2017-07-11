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

import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
class VertexElement extends AbstractElement<Vertex, Schema.VertexProperty>{
    static String PREFIX = "V";

    VertexElement(AbstractGraknGraph graknGraph, Vertex element) {
        super(graknGraph, element, PREFIX);
    }

    /**
     *
     * @param direction The direction of the edges to retrieve
     * @param label The type of the edges to retrieve
     * @return A collection of edges from this concept in a particular direction of a specific type
     */
    Stream<EdgeElement> getEdgesOfType(Direction direction, Schema.EdgeLabel label){
        Iterable<Edge> iterable = () -> element().edges(direction, label.getLabel());
        return StreamSupport.stream(iterable.spliterator(), false).
                map(edge -> graph().factory().buildEdge(edge));
    }

    /**
     *
     * @param to the target {@link VertexElement}
     * @param type the type of the edge to create
     * @return The edge created
     */
    EdgeElement addEdge(VertexElement to, Schema.EdgeLabel type) {
        return graph().factory().buildEdge(element().addEdge(type.getLabel(), to.element()));
    }

    /**
     * @param to the target {@link VertexElement}
     * @param type the type of the edge to create
     */
    EdgeElement putEdge(VertexElement to, Schema.EdgeLabel type){
        GraphTraversal<Vertex, Edge> traversal = graph().getTinkerTraversal().
                has(Schema.VertexProperty.ID.name(), id().getValue()).
                outE(type.getLabel()).as("edge").otherV().
                has(Schema.VertexProperty.ID.name(), to.id().getValue()).select("edge");

        if(!traversal.hasNext()) {
            return addEdge(to, type);
        } else {
            return graph().factory().buildEdge(traversal.next());
        }
    }

    /**
     * Deletes all the edges of a specific {@link Schema.EdgeLabel} to or from a specific set of targets.
     * If no targets are provided then all the edges of the specified type are deleted
     *
     * @param direction The direction of the edges to delete
     * @param label The edge label to delete
     * @param targets An optional set of targets to delete edges from
     */
    void deleteEdge(Direction direction, Schema.EdgeLabel label, VertexElement... targets){
        Iterator<Edge> edges = element().edges(direction, label.getLabel());
        if(targets.length == 0){
            edges.forEachRemaining(Edge::remove);
        } else {
            Set<Vertex> verticesToDelete = Arrays.stream(targets).map(AbstractElement::element).collect(Collectors.toSet());
            edges.forEachRemaining(edge -> {
                boolean delete = false;
                switch (direction){
                    case BOTH:
                        delete = verticesToDelete.contains(edge.inVertex()) || verticesToDelete.contains(edge.outVertex());
                        break;
                    case IN:
                        delete = verticesToDelete.contains(edge.outVertex());
                        break;
                    case OUT:
                        delete = verticesToDelete.contains(edge.inVertex());
                        break;
                }

                if(delete) edge.remove();
            });
        }
    }

    /**
     * Sets the value of a property with the added restriction that no other vertex can have that property.
     *
     * @param key The key of the unique property to mutate
     * @param value The new value of the unique property
     */
    void propertyUnique(Schema.VertexProperty key, String value){
        if(!graph().isBatchGraph()) {
            GraphTraversal<Vertex, Vertex> traversal = graph().getTinkerTraversal().has(key.name(), value);
            if(traversal.hasNext()) throw PropertyNotUniqueException.cannotChangeProperty(element(), traversal.next(), key, value);
        }

        property(key, value);
    }


    /**
     * Sets a property which cannot be mutated
     *
     * @param vertexProperty The key of the immutable property to mutate
     * @param newValue The new value to put on the property (if the property is not set)
     * @param foundValue The current valud of the property
     * @param converter Helper method to ensure data is persisted in the correct format
     */
    <X> void propertyImmutable(Schema.VertexProperty vertexProperty, X newValue, X foundValue, Function<X, Object> converter){
        if(newValue == null){
            throw GraphOperationException.settingNullProperty(vertexProperty);
        }

        if(foundValue != null){
            if(!foundValue.equals(newValue)){
                throw GraphOperationException.immutableProperty(foundValue, newValue, vertexProperty);
            }
        } else {
            property(vertexProperty, converter.apply(newValue));
        }
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Vertex [").append(id()).append("] /n");
        element().properties().forEachRemaining(
                p -> stringBuilder.append("Property [").append(p.key()).append("] value [").append(p.value()).append("] /n"));
        return stringBuilder.toString();
    }

}
