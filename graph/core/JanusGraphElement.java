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


import grakn.core.graph.util.datastructures.Removable;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * JanusGraphElement represents the abstract concept of an entity in the graph and specifies basic methods for interacting
 * with entities.
 * The two basic entities in a graph database are JanusGraphRelation and JanusGraphVertex.
 * Entities have a life cycle state which reflects the current state of the entity with respect
 * to the JanusGraphTransaction in which it occurs. An entity may be in any one of these states
 * and any given time:
 * <ul>
 * <li><strong>New:</strong> The entity has been created in the current transaction</li>
 * <li><strong>Loaded:</strong> The entity has been loaded from disk into the current transaction and has not been modified</li>
 * <li><strong>Modified:</strong> The entity has been loaded from disk into the current transaction and has been modified</li>
 * <li><strong>Deleted:</strong> The entity has been deleted in the current transaction</li>
 * </ul>
 * Depending on the concrete type of the entity, an entity may be identifiable,
 * i.e. it has a unique ID which can be retrieved via #id
 * (use #hasId to determine if a given entity has a unique ID).
 *
 * see JanusGraphVertex
 * see JanusGraphRelation
 */
public interface JanusGraphElement extends Element, Idfiable, Removable {

    @Override
    JanusGraphTransaction graph();

    /**
     * Returns a unique identifier for this entity.
     * <p>
     * The unique identifier may only be set when the transaction in which entity is created commits.
     * Some entities are never assigned a unique identifier if they depend on a parent entity.
     * <p>
     * JanusGraph allocates blocks of identifiers and automatically assigns identifiers to elements
     * automatically be default.
     *
     * @return The unique identifier for this entity
     * @throws IllegalStateException if the entity does not (yet) have a unique identifier
     * see #hasId
     */
    @Override
    default Object id() {
        return longId();
    }

    /**
     * Unique identifier for this entity. This id can be temporarily assigned and might change.
     * Use #id() for the permanent id.
     *
     * @return Unique long id
     */
    @Override
    long longId();

    /**
     * Checks whether this entity has a unique identifier.
     * <p>
     * Note that some entities may never be assigned an identifier and others will only be
     * assigned an identifier at the end of a transaction.
     *
     * @return true if this entity has been assigned a unique id, else false
     * see #longId()
     */
    boolean hasId();

    /**
     * Deletes this entity and any incident edges or properties from the graph.
     * <p>
     *
     * @throws IllegalStateException if the entity cannot be deleted or if the user does not
     *                               have permission to remove the entity
     */
    @Override
    void remove();

    /**
     * Sets the value for the given key on this element.
     * The key must be defined to have Cardinality#SINGLE, otherwise this method throws an exception.
     *
     * @param key   the string identifying the key
     * @param value the object value
     */
    @Override
    <V> Property<V> property(String key, V value);

    /**
     * Retrieves the value associated with the given key on this element and casts it to the specified type.
     * If the key has cardinality SINGLE, then there can be at most one value and this value is returned (or null).
     * Otherwise a list of all associated values is returned, or an empty list if non exist.
     * <p>
     *
     * @param key key
     * @return value or list of values associated with key
     */
    <V> V valueOrNull(PropertyKey key);

    //########### LifeCycle Status ##########

    /**
     * Checks whether this entity has been newly created in the current transaction.
     *
     * @return True, if entity has been newly created, else false.
     */
    boolean isNew();

    /**
     * Checks whether this entity has been loaded into the current transaction and not yet modified.
     *
     * @return True, has been loaded and not modified, else false.
     */
    boolean isLoaded();

    /**
     * Checks whether this entity has been deleted into the current transaction.
     *
     * @return True, has been deleted, else false.
     */
    boolean isRemoved();

}
