/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


package grakn.core.graph.core;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.InvalidElementException;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;

/**
 * JanusGraphRelation is the most abstract form of a relation between a vertex and some other entity, where
 * relation is understood in its mathematical sense. It generalizes the notion of an edge and a property.
 * <br>
 * A JanusGraphRelation extends {@link JanusGraphElement} which means it is an entity in its own right. This means, a JanusGraphRelation
 * can have properties and unidirectional edges connecting it to other vertices.
 * <br>
 * A JanusGraphRelation is an abstract concept. A JanusGraphRelation is either a {@link JanusGraphVertexProperty} or a {@link JanusGraphEdge}.
 * A JanusGraphRelation has a type which is either a label or key depending on the implementation.
 * <br>
 * A JanusGraphRelation is either directed, or unidirected. Properties are always directed (connecting a vertex
 * with a value). A unidirected edge is a special type of directed edge where the connection is only established from the
 * perspective of the outgoing vertex. In that sense, a unidirected edge is akin to a link.
 *
 * @see JanusGraphEdge
 * @see JanusGraphVertexProperty
 */
public interface JanusGraphRelation extends JanusGraphElement {

    /**
     * Retrieves the value associated with the given key on this vertex and casts it to the specified type.
     * If the key has cardinality SINGLE, then there can be at most one value and this value is returned (or null).
     * Otherwise a list of all associated values is returned, or an empty list if non exist.
     * <p>
     *
     * @param key string identifying a key
     * @return value or list of values associated with key
     */
    <V> V value(String key);

    /**
     * Returns the type of this relation.
     * <p>
     * The type is either a label ({@link EdgeLabel} if this relation is an edge or a key ({@link PropertyKey}) if this
     * relation is a property.
     *
     * @return Type of this relation
     */
    RelationType getType();

    /**
     * Returns the direction of this relation from the perspective of the specified vertex.
     *
     * @param vertex vertex on which the relation is incident
     * @return The direction of this relation from the perspective of the specified vertex.
     * @throws InvalidElementException if this relation is not incident on the vertex
     */
    Direction direction(Vertex vertex);

    /**
     * Checks whether this relation is incident on the specified vertex.
     *
     * @param vertex vertex to check incidence for
     * @return true, if this relation is incident on the vertex, else false
     */
    boolean isIncidentOn(Vertex vertex);

    /**
     * Checks whether this relation is a loop.
     * An relation is a loop if it connects a vertex with itself.
     *
     * @return true, if this relation is a loop, else false.
     */
    boolean isLoop();

    /**
     * Checks whether this relation is a property.
     *
     * @return true, if this relation is a property, else false.
     * @see JanusGraphVertexProperty
     */
    boolean isProperty();

    /**
     * Checks whether this relation is an edge.
     *
     * @return true, if this relation is an edge, else false.
     * @see JanusGraphEdge
     */
    boolean isEdge();


}
