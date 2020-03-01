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

import com.google.common.base.Preconditions;
import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.database.serialize.SerializerInjected;


public class ParameterSerializer implements AttributeSerializer<Parameter>, SerializerInjected {

    private Serializer serializer;

    @Override
    public Parameter read(ScanBuffer buffer) {
        String key = serializer.readObjectNotNull(buffer,String.class);
        Object value = serializer.readClassAndObject(buffer);
        return new Parameter(key,value);
    }

    @Override
    public void write(WriteBuffer buffer, Parameter attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.key());
        out.writeClassAndObject(attribute.value());
    }


    @Override
    public void setSerializer(Serializer serializer) {
        Preconditions.checkNotNull(serializer);
        this.serializer=serializer;
    }
}
