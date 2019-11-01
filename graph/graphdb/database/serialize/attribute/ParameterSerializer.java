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
