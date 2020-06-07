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

package grakn.core.graph.core.schema;

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.VertexLabel;

/**
 * When a graph is configured to automatically create vertex/edge labels and property keys when they are first used,
 * a DefaultTypeMaker implementation is used to define them by invoking the #makeVertexLabel(VertexLabelMaker),
 * #makeEdgeLabel(EdgeLabelMaker), or #makePropertyKey(PropertyKeyMaker) methods respectively.
 * <br>
 * By providing a custom DefaultTypeMaker implementation, one can specify how these types should be defined by default.
 * A DefaultTypeMaker implementation is specified in the graph configuration using the full path which means the
 * implementation must be on the classpath.
 *
 * see RelationTypeMaker
 */
public interface DefaultSchemaMaker {

    /**
     * Creates a new edge label with default settings against the provided EdgeLabelMaker.
     *
     * @param factory EdgeLabelMaker through which the edge label is created
     * @return A new edge label
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default EdgeLabel makeEdgeLabel(EdgeLabelMaker factory) {
        return factory.directed().make();
    }

    /**
     * @return the default cardinality of a property if created for the given key
     */
    Cardinality defaultPropertyCardinality(String key);

    /**
     * Creates a new property key with default settings against the provided PropertyKeyMaker.
     *
     * @param factory PropertyKeyMaker through which the property key is created
     * @return A new property key
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        return factory.cardinality(defaultPropertyCardinality(factory.getName())).dataType(Object.class).make();
    }

    /**
     * Creates a new property key with default settings against the provided PropertyKeyMaker and value.
     *
     * @param factory PropertyKeyMaker through which the property key is created
     * @param value   the value of the property. The default implementation does not use this parameter.
     * @return A new property key
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default PropertyKey makePropertyKey(PropertyKeyMaker factory, Object value) {
        return makePropertyKey(factory);
    }

    /**
     * Creates a new vertex label with the default settings against the provided VertexLabelMaker.
     *
     * @param factory VertexLabelMaker through which the vertex label is created
     * @return A new vertex label
     * @throws IllegalArgumentException if the name is already in use or if other configured values are invalid.
     */
    default VertexLabel makeVertexLabel(VertexLabelMaker factory) {
        return factory.make();
    }

    /**
     * Whether to ignore undefined types occurring in a query.
     * <p>
     * If this method returns true, then undefined types referred to in a JanusGraphVertexQuery will be silently
     * ignored and an empty result set will be returned. If this method returns false, then usage of undefined types
     * in queries results in an IllegalArgumentException.
     */
    boolean ignoreUndefinedQueryTypes();

    /**
     * Add property constraints for a given vertex label using the schema manager.
     *
     * @param vertexLabel to which the constraint applies.
     * @param key         defines the property which should be added to the vertex label as a constraint.
     * @param manager     is used to update the schema.
     * see SchemaManager
     */
    default void makePropertyConstraintForVertex(VertexLabel vertexLabel, PropertyKey key, SchemaManager manager) {
        manager.addProperties(vertexLabel, key);
    }

    /**
     * Add property constraints for a given edge label using the schema manager.
     *
     * @param edgeLabel to which the constraint applies.
     * @param key       defines the property which should be added to the edge label as a constraint.
     * @param manager   is used to update the schema.
     * see SchemaManager
     */
    default void makePropertyConstraintForEdge(EdgeLabel edgeLabel, PropertyKey key, SchemaManager manager) {
        manager.addProperties(edgeLabel, key);
    }

    /**
     * Add a constraint on which vertices the given edge label can connect using the schema manager.
     *
     * @param edgeLabel to which the constraint applies.
     * @param outVLabel specifies the outgoing vertex for this connection.
     * @param inVLabel  specifies the incoming vertex for this connection.
     * @param manager   is used to update the
     * see SchemaManager
     */
    default void makeConnectionConstraint(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel, SchemaManager manager) {
        manager.addConnection(edgeLabel, outVLabel, inVLabel);
    }

}
