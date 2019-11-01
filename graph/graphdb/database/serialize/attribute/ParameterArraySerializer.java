// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.database.serialize.attribute;

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.Serializer;
import grakn.core.graph.graphdb.database.serialize.SerializerInjected;
import grakn.core.graph.graphdb.database.serialize.attribute.ArraySerializer;

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
        if (length<0) return null;
        Parameter[] result = new Parameter[length];
        for (int i = 0; i < length; i++) {
            result[i]=serializer.readObjectNotNull(buffer, Parameter.class);
        }
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, Parameter[] attribute) {
        writeLength(buffer,attribute);
        if (attribute!=null)
            for (Parameter anAttribute : attribute) ((DataOutput) buffer).writeObjectNotNull(anAttribute);
    }


    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer=serializer;
    }
}
