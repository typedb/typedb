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
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.time.Duration;

/**
 * The JanusGraphManagement interface provides methods to define, update, and inspect the schema of a JanusGraph graph.
 * It wraps a JanusGraphTransaction and therefore copies many of its methods as they relate to schema inspection
 * and definition.
 * <p>
 * JanusGraphManagement behaves like a transaction in that it opens a transactional scope for reading the schema and making
 * changes to it. As such, it needs to be explicitly closed via its #commit() or #rollback() methods.
 * A JanusGraphManagement transaction is opened on a graph via JanusGraph#openManagement().
 * <p>
 * JanusGraphManagement provides methods to:
 * <ul>
 * <li>Schema Types: View, update, and create vertex labels, edge labels, and property keys</li>
 * <li>Relation Type Index: View and create vertex-centric indexes on edge labels and property keys</li>
 * <li>Graph Index: View and create graph-wide indexes for efficient element retrieval</li>
 * <li>Consistency Management: Set the consistency level of individual schema elements</li>
 * </ul>
 */
public interface JanusGraphManagement extends SchemaManager {

    /*
    ##################### RELATION TYPE INDEX ##########################
     */

    /**
     * Identical to #buildEdgeIndex(EdgeLabel, String, org.apache.tinkerpop.gremlin.structure.Direction, org.apache.tinkerpop.gremlin.process.traversal.Order, PropertyKey...)
     * with default sort order org.apache.tinkerpop.gremlin.process.traversal.Order#asc.
     *
     * @return the created RelationTypeIndex
     */
    RelationTypeIndex buildEdgeIndex(EdgeLabel label, String name, Direction direction, PropertyKey... sortKeys);

    /**
     * Creates a RelationTypeIndex for the provided edge label. That means, that all edges of that label will be
     * indexed according to this index definition which will speed up certain vertex-centric queries.
     * <p>
     * An indexed is defined by its name, the direction in which the index should be created (can be restricted to one
     * direction or both), the sort order and - most importantly - the sort keys which define the index key.
     *
     * @return the created RelationTypeIndex
     */
    RelationTypeIndex buildEdgeIndex(EdgeLabel label, String name, Direction direction, Order sortOrder, PropertyKey... sortKeys);

    /**
     * Identical to #buildPropertyIndex(PropertyKey, String, org.apache.tinkerpop.gremlin.process.traversal.Order, PropertyKey...)
     * with default sort order org.apache.tinkerpop.gremlin.process.traversal.Order#asc.
     *
     * @return the created RelationTypeIndex
     */
    RelationTypeIndex buildPropertyIndex(PropertyKey key, String name, PropertyKey... sortKeys);

    /**
     * Creates a RelationTypeIndex for the provided property key. That means, that all properties of that key will be
     * indexed according to this index definition which will speed up certain vertex-centric queries.
     * <p>
     * An indexed is defined by its name, the sort order and - most importantly - the sort keys which define the index key.
     *
     * @return the created RelationTypeIndex
     */
    RelationTypeIndex buildPropertyIndex(PropertyKey key, String name, Order sortOrder, PropertyKey... sortKeys);

    /**
     * Whether a RelationTypeIndex with the given name has been defined for the provided RelationType
     */
    boolean containsRelationIndex(RelationType type, String name);

    /**
     * Returns the RelationTypeIndex with the given name for the provided RelationType or null
     * if it does not exist
     */
    RelationTypeIndex getRelationIndex(RelationType type, String name);

    /**
     * Returns an Iterable over all RelationTypeIndexes defined for the provided RelationType
     */
    Iterable<RelationTypeIndex> getRelationIndexes(RelationType type);

    /*
    ##################### GRAPH INDEX ##########################
     */


    /**
     * Whether the graph has a graph index defined with the given name.
     */
    boolean containsGraphIndex(String name);

    /**
     * Returns the graph index with the given name or null if it does not exist
     */
    JanusGraphIndex getGraphIndex(String name);

    /**
     * Returns all graph indexes that index the given element type.
     */
    Iterable<JanusGraphIndex> getGraphIndexes(Class<? extends Element> elementType);

    /**
     * Returns an IndexBuilder to add a graph index to this JanusGraph graph. The index to-be-created
     * has the provided name and indexes elements of the given type.
     */
    IndexBuilder buildIndex(String indexName, Class<? extends Element> elementType);


    void addIndexKey(JanusGraphIndex index, PropertyKey key, Parameter... parameters);

    /**
     * Builder for JanusGraphIndex. Allows for the configuration of a graph index prior to its construction.
     */
    interface IndexBuilder {

        /**
         * Adds the given key to the composite key of this index
         *
         * @return this IndexBuilder
         */
        IndexBuilder addKey(PropertyKey key);

        /**
         * Adds the given key and associated parameters to the composite key of this index
         *
         * @return this IndexBuilder
         */
        IndexBuilder addKey(PropertyKey key, Parameter... parameters);

        /**
         * Restricts this index to only those elements that have the provided schemaType. If this graph index indexes
         * vertices, then the argument is expected to be a vertex label and only vertices with that label will be indexed.
         * Likewise, for edges and properties only those with the matching relation type will be indexed.
         *
         * @return this IndexBuilder
         */
        IndexBuilder indexOnly(JanusGraphSchemaType schemaType);

        /**
         * Makes this a unique index for the configured element type,
         * i.e. an index key can be associated with at most one element in the graph.
         *
         * @return this IndexBuilder
         */
        IndexBuilder unique();

        /**
         * Builds a composite index according to the specification
         *
         * @return the created composite JanusGraphIndex
         */
        JanusGraphIndex buildCompositeIndex();

        /**
         * Builds a mixed index according to the specification against the backend index with the given name (i.e.
         * the name under which that index is configured in the graph configuration)
         *
         * @param backingIndex the name of the mixed index
         * @return the created mixed JanusGraphIndex
         */
        JanusGraphIndex buildMixedIndex(String backingIndex);

    }

    /*
    ##################### CONSISTENCY SETTING ##########################
     */

    /**
     * Retrieves the consistency modifier for the given JanusGraphSchemaElement. If none has been explicitly
     * defined, ConsistencyModifier#DEFAULT is returned.
     */
    ConsistencyModifier getConsistency(JanusGraphSchemaElement element);

    /**
     * Retrieves the time-to-live for the given JanusGraphSchemaType as a Duration.
     * If no TTL has been defined, the returned Duration will be zero-length ("lives forever").
     */
    Duration getTTL(JanusGraphSchemaType type);

    /*
    ##################### CLUSTER MANAGEMENT ##########################
     */

    /**
     * Returns an iterable over all defined types that have the given clazz (either EdgeLabel which returns all labels,
     * PropertyKey which returns all keys, or RelationType which returns all types).
     *
     * @param clazz RelationType or sub-interface
     * @return Iterable over all types for the given category (label, key, or both)
     */
    <T extends RelationType> Iterable<T> getRelationTypes(Class<T> clazz);

    /**
     * Returns an Iterable over all defined VertexLabels.
     */
    Iterable<VertexLabel> getVertexLabels();

    /**
     * Whether this management transaction is open or has been closed (i.e. committed or rolled-back)
     */
    boolean isOpen();

    /**
     * Commits this management transaction and persists all schema changes. Closes this transaction.
     *
     * see JanusGraphTransaction#commit()
     */
    void commit();

    /**
     * Closes this management transaction and discards all changes.
     *
     * see JanusGraphTransaction#rollback()
     */
    void rollback();

}
