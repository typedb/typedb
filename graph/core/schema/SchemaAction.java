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

package grakn.core.graph.core.schema;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Update actions to be executed through {@link JanusGraphManagement} in {@link JanusGraphManagement#updateIndex(Index, SchemaAction)}.
 */
public enum SchemaAction {

    /**
     * Registers the index with all instances in the graph cluster. After an index is installed, it must be registered
     * with all graph instances.
     */
    REGISTER_INDEX,

    /**
     * Re-builds the index from the graph
     */
    REINDEX,

    /**
     * Enables the index so that it can be used by the query processing engine. An index must be registered before it
     * can be enabled.
     */
    ENABLE_INDEX,

    /**
     * Disables the index in the graph so that it is no longer used.
     */
    DISABLE_INDEX,

    /**
     * Removes the index from the graph (optional operation)
     */
    REMOVE_INDEX;

    public Set<SchemaStatus> getApplicableStatus() {
        switch (this) {
            case REGISTER_INDEX:
                return ImmutableSet.of(SchemaStatus.INSTALLED);
            case REINDEX:
                return ImmutableSet.of(SchemaStatus.REGISTERED, SchemaStatus.ENABLED);
            case ENABLE_INDEX:
                return ImmutableSet.of(SchemaStatus.REGISTERED);
            case DISABLE_INDEX:
                return ImmutableSet.of(SchemaStatus.REGISTERED, SchemaStatus.INSTALLED, SchemaStatus.ENABLED);
            case REMOVE_INDEX:
                return ImmutableSet.of(SchemaStatus.DISABLED);
            default:
                throw new IllegalArgumentException("Action is invalid: " + this);
        }
    }

    public Set<SchemaStatus> getFailureStatus() {
        switch (this) {
            case REGISTER_INDEX:
                return ImmutableSet.of(SchemaStatus.DISABLED);
            case REINDEX:
                return ImmutableSet.of(SchemaStatus.INSTALLED, SchemaStatus.DISABLED);
            case ENABLE_INDEX:
                return ImmutableSet.of(SchemaStatus.INSTALLED, SchemaStatus.DISABLED);
            case DISABLE_INDEX:
                return ImmutableSet.of();
            case REMOVE_INDEX:
                return ImmutableSet.of(SchemaStatus.REGISTERED, SchemaStatus.INSTALLED, SchemaStatus.ENABLED);
            default:
                throw new IllegalArgumentException("Action is invalid: " + this);
        }
    }

    public boolean isApplicableStatus(SchemaStatus status) {
        if (getFailureStatus().contains(status)) {
            throw new IllegalArgumentException(String.format("Update action [%s] cannot be invoked for index with status [%s]", this, status));
        }
        return getApplicableStatus().contains(status);
    }

}
