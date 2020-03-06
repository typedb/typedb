/*
 * Copyright (C) 2020 Grakn Labs
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


import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * JanusGraphVertex is the basic unit of a JanusGraph.
 * It extends the functionality provided by Blueprint's Vertex by helper and convenience methods.
 * <p>
 * Vertices have incident edges and properties. Edge connect the vertex to other vertices. Properties attach key-value
 * pairs to this vertex to define it.
 * <p>
 * Like JanusGraphRelation a vertex has a vertex label.
 */
public interface JanusGraphVertex extends JanusGraphElement, Vertex {

    /* ---------------------------------------------------------------
     * Creation and modification methods
     * ---------------------------------------------------------------
     */

    /**
     * Creates a new edge incident on this vertex.
     * <p>
     * Creates and returns a new JanusGraphEdge of the specified label with this vertex being the outgoing vertex
     * and the given vertex being the incoming vertex.
     * <br>
     * Automatically creates the edge label if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an IllegalArgumentException.
     *
     * @param label  label of the edge to be created
     * @param vertex incoming vertex of the edge to be created
     * @return new edge
     */
    @Override
    JanusGraphEdge addEdge(String label, Vertex vertex, Object... keyValues);

    /**
     * Creates a new property for this vertex and given key with the specified value.
     * <p>
     * Creates and returns a new JanusGraphVertexProperty for the given key on this vertex with the specified
     * object being the value.
     * <br>
     * Automatically creates the property key if it does not exist and automatic creation of types is enabled. Otherwise,
     * this method with throw an IllegalArgumentException.
     *
     * @param key   key of the property to be created
     * @param value value of the property to be created
     * @return New property
     * @throws IllegalArgumentException if the value does not match the data type of the property key.
     */
    @Override
    default <V> JanusGraphVertexProperty<V> property(String key, V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    @Override
    <V> JanusGraphVertexProperty<V> property(String key, V value, Object... keyValues);


    @Override
    <V> JanusGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues);

    /* ---------------------------------------------------------------
     * Vertex Label
     * ---------------------------------------------------------------
     */

    /**
     * Returns the name of the vertex label for this vertex.
     */
    @Override
    default String label() {
        return vertexLabel().name();
    }

    /**
     * Returns the vertex label of this vertex.
     */
    VertexLabel vertexLabel();

    // Incident JanusGraphRelation Access methods

    /**
     * Starts a new JanusGraphVertexQuery for this vertex.
     * <p>
     * Initializes and returns a new JanusGraphVertexQuery based on this vertex.
     *
     * @return New JanusGraphQuery for this vertex
     * see JanusGraphVertexQuery
     */
    JanusGraphVertexQuery<? extends JanusGraphVertexQuery> query();

    /**
     * Checks whether this entity has been loaded into the current transaction and modified.
     *
     * @return True, has been loaded and modified, else false.
     */
    boolean isModified();

}
