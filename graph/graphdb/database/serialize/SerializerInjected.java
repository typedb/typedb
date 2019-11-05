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

package grakn.core.graph.graphdb.database.serialize;

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.WriteBuffer;

/**
 * Marks a {@link AttributeSerializer} that requires a {@link Serializer}
 * to serialize the internal state. It is expected that the serializer is passed into this object upon initialization and before usage.
 * Furthermore, such serializers will convert the {@link WriteBuffer} passed into the
 * {@link AttributeSerializer}'s write methods to be cast to {@link DataOutput}.
 */
public interface SerializerInjected {

    void setSerializer(Serializer serializer);

}
