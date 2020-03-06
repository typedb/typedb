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

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.VertexLabel;

public interface SchemaInspector {

    /* ---------------------------------------------------------------
     * Schema
     * ---------------------------------------------------------------
     */

    /**
     * Checks whether a type with the specified name exists.
     *
     * @param name name of the type
     * @return true, if a type with the given name exists, else false
     */
    boolean containsRelationType(String name);

    /**
     * Returns the type with the given name.
     * Note, that type names must be unique.
     *
     * @param name name of the type to return
     * @return The type with the given name, or null if such does not exist
     * see RelationType
     */
    RelationType getRelationType(String name);

    /**
     * Checks whether a property key of the given name has been defined in the JanusGraph schema.
     *
     * @param name name of the property key
     * @return true, if the property key exists, else false
     */
    boolean containsPropertyKey(String name);

    /**
     * Returns the property key with the given name. If automatic type making is enabled, it will make the property key
     * using the configured default type maker if a key with the given name does not exist.
     *
     * @param name name of the property key to return
     * @return the property key with the given name
     * @throws IllegalArgumentException if a property key with the given name does not exist or if the
     *                                  type with the given name is not a property key
     * see PropertyKey
     */
    PropertyKey getOrCreatePropertyKey(String name);


    /**
     * Returns the property key with the given name. If automatic type making is enabled, it will make the property key
     * using the configured default type maker if a key with the given name does not exist.
     * <p>
     * The default implementation simply calls the #getOrCreatePropertyKey(String name) getOrCreatePropertyKey method
     *
     * @param name  name of the property key to return
     * @param value the value of the property key. This param is not used by the default
     *              implementation
     * @return the property key with the given name
     * @throws IllegalArgumentException if a property key with the given name does not exist or if the
     *                                  type with the given name is not a property key
     * see PropertyKey
     */
    default PropertyKey getOrCreatePropertyKey(String name, Object value) {
        return getOrCreatePropertyKey(name);
    }

    /**
     * Returns the property key with the given name. If it does not exist, NULL is returned
     */
    PropertyKey getPropertyKey(String name);

    /**
     * Checks whether an edge label of the given name has been defined in the JanusGraph schema.
     *
     * @param name name of the edge label
     * @return true, if the edge label exists, else false
     */
    boolean containsEdgeLabel(String name);

    /**
     * Returns the edge label with the given name. If automatic type making is enabled, it will make the edge label
     * using the configured default type maker if a label with the given name does not exist.
     *
     * @param name name of the edge label to return
     * @return the edge label with the given name
     * @throws IllegalArgumentException if an edge label with the given name does not exist or if the
     *                                  type with the given name is not an edge label
     * see EdgeLabel
     */
    EdgeLabel getOrCreateEdgeLabel(String name);

    /**
     * Returns the edge label with the given name. If it does not exist, NULL is returned
     */
    EdgeLabel getEdgeLabel(String name);

    /**
     * Whether a vertex label with the given name exists in the graph.
     */
    boolean containsVertexLabel(String name);

    /**
     * Returns the vertex label with the given name. If such does not exist, NULL is returned.
     */
    VertexLabel getVertexLabel(String name);

    /**
     * Returns the vertex label with the given name. If a vertex label with this name does not exist, the label is
     * automatically created through the registered DefaultSchemaMaker.
     * <p>
     * Attempting to automatically create a vertex label might cause an exception depending on the configuration.
     */
    VertexLabel getOrCreateVertexLabel(String name);


}
