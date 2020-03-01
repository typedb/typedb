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

package grakn.core.graph.diskstorage;

import grakn.core.graph.graphdb.relations.RelationCache;

/**
 * An entry is the primitive persistence unit used in the graph database storage backend.
 * <p>
 * An entry consists of a column and value both of which are general java.nio.ByteBuffers.
 * The value may be null but the column may not.
 *
 */
public interface Entry extends StaticBuffer, MetaAnnotated {

    int getValuePosition();

    boolean hasValue();

    StaticBuffer getColumn();

    <T> T getColumnAs(Factory<T> factory);

    StaticBuffer getValue();

    <T> T getValueAs(Factory<T> factory);

    /**
     * Returns the cached parsed representation of this Entry if it exists, else NULL
     */
    RelationCache getCache();

    /**
     * Sets the cached parsed representation of this Entry. This method does not synchronize,
     * so a previously set representation would simply be overwritten.
     */
    void setCache(RelationCache cache);
}
