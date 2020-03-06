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
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.database.serialize.SerializerInjected;

import java.lang.reflect.Array;


public class ParameterArraySerializer extends ArraySerializer implements AttributeSerializer<Parameter[]>, SerializerInjected {

    private Serializer serializer;

    @Override
    public Parameter[] convert(Object value) {
        return convertInternal(value, null, Parameter.class);
    }

    @Override
    protected Object getArray(int length) {
        return new Parameter[length];
    }

    @Override
    protected void setArray(Object array, int pos, Object value) {
        Array.set(array, pos, value);
    }

    //############### Serialization ###################

    @Override
    public Parameter[] read(ScanBuffer buffer) {
        int length = getLength(buffer);
        if (length < 0) return null;
        Parameter[] result = new Parameter[length];
        for (int i = 0; i < length; i++) {
            result[i] = serializer.readObjectNotNull(buffer, Parameter.class);
        }
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, Parameter[] attribute) {
        writeLength(buffer, attribute);
        if (attribute != null) {
            for (Parameter anAttribute : attribute) {
                ((DataOutput) buffer).writeObjectNotNull(anAttribute);
            }
        }
    }


    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }
}
