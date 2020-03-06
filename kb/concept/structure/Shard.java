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
 *
 */

package grakn.core.kb.concept.structure;

import com.google.common.annotations.VisibleForTesting;

import java.util.stream.Stream;

public interface Shard {

    VertexElement vertex();
    /**
     * @return The id of this shard. Strings are used because shards are looked up via the string index.
     */
    Object id();

    /**
     * Links a new concept's vertex to this shard.
     *
     * @param conceptVertex The concept to link to this shard
     */
    void link(VertexElement conceptVertex);

    /**
     * @return All the concept linked to this shard
     */
    @VisibleForTesting
    Stream<VertexElement> links();
}
