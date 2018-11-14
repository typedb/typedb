/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server.deduplicator;

import grakn.core.server.keyspace.Keyspace;
import com.google.auto.value.AutoValue;

/**
 * A class to hold a keyspace and an index together.
 *
 */
@AutoValue
public abstract class KeyspaceIndexPair {
    public abstract Keyspace keyspace();
    public abstract String index();

    public static KeyspaceIndexPair create(Keyspace keyspace, String index) {
        return new AutoValue_KeyspaceIndexPair(keyspace, index);
    }
}
