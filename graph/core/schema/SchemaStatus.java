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

import grakn.core.graph.core.schema.Index;

/**
 * Designates the status of a {@link Index} in a graph.
 *
 */
public enum SchemaStatus {

    /**
     * The index is installed in the system but not yet registered with all instances in the cluster
     */
    INSTALLED,

    /**
     * The index is registered with all instances in the cluster but not (yet) enabled
     */
    REGISTERED,

    /**
     * The index is enabled and in use
     */
    ENABLED,

    /**
     * The index is disabled and no longer in use
     */
    DISABLED;


    public boolean isStable() {
        switch(this) {
            case INSTALLED: return false;
            default: return true;
        }
    }

}
