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

/**
 * Used to control JanusGraph's consistency behavior on eventually consistent or other non-transactional backend systems.
 * The consistency behavior can be defined for individual JanusGraphSchemaElements which then applies to all instances.
 * <p>
 * Consistency modifiers are installed on schema elements via JanusGraphManagement#setConsistency(JanusGraphSchemaElement, ConsistencyModifier)
 * and can be read using JanusGraphManagement#getConsistency(JanusGraphSchemaElement).
 */
public enum ConsistencyModifier {

    /**
     * Uses the default consistency model guaranteed by the enclosing transaction against the configured
     * storage backend.
     * <p>
     * What this means exactly, depends on the configuration of the storage backend as well as the (optional) configuration
     * of the enclosing transaction.
     */
    DEFAULT,

    /**
     * Locks will be explicitly acquired to guarantee consistency if the storage backend supports locks.
     * <p>
     * The exact consistency guarantees depend on the configured lock implementation.
     * <p>
     * Note, that locking may be ignored under certain transaction configurations.
     */
    LOCK,


    /**
     * Causes JanusGraph to delete and add a new edge/property instead of overwriting an existing one, hence avoiding potential
     * concurrent write conflicts. This only applies to multi-edges and list-properties.
     * <p>
     * Note, that this potentially impacts how the data should be read.
     */
    FORK

}
