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

package grakn.core.graph.graphdb.configuration;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.graphdb.database.serialize.Serializer;

/**
 * Helper class for registering data types with JanusGraph
 *
 * @param <T>
 */
public class RegisteredAttributeClass<T> {

    private final int position;
    private final Class<T> type;
    private final AttributeSerializer<T> serializer;

    public RegisteredAttributeClass(int position, Class<T> type, AttributeSerializer<T> serializer) {
        Preconditions.checkArgument(position >= 0, "Position must be a positive integer, given: %s", position);
        this.position = position;
        this.type = Preconditions.checkNotNull(type);
        this.serializer = Preconditions.checkNotNull(serializer);
    }

    void registerWith(Serializer s) {
        s.registerClass(position, type, serializer);
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) {
            return true;
        } else if (!getClass().isInstance(oth)) {
            return false;
        }
        return type.equals(((RegisteredAttributeClass<?>) oth).type);
    }

    @Override
    public int hashCode() {
        return type.hashCode() + 110432;
    }

    @Override
    public String toString() {
        return type.toString();
    }
}
