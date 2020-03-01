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

package grakn.core.graph.graphdb.database.serialize.attribute;

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.database.serialize.SerializerInjected;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

/**
 * Serializes Serializable objects.
 * @param <T> Serializable type
 */
public class SerializableSerializer<T extends Serializable> implements AttributeSerializer<T>, SerializerInjected {

    private Serializer serializer;

    @Override
    public T read(ScanBuffer buffer) {
        byte[] data = serializer.readObjectNotNull(buffer,byte[].class);
        return (T) SerializationUtils.deserialize(data);
    }

    @Override
    public void write(WriteBuffer buffer, T attribute) {
        DataOutput out = (DataOutput) buffer;
        out.writeObjectNotNull(SerializationUtils.serialize(attribute));
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

}
