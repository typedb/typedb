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

package grakn.core.graph.graphdb.database.serialize;

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.diskstorage.WriteBuffer;


public class NoSerializer<V> implements AttributeSerializer<V> {

    private final Class<V> datatype;

    public NoSerializer(Class<V> datatype) {
        this.datatype = datatype;
    }

    private IllegalArgumentException error() {
        return new IllegalArgumentException("Serializing objects of type ["+datatype+"] is not supported");
    }

    @Override
    public V read(ScanBuffer buffer) {
        throw error();
    }

    @Override
    public void write(WriteBuffer buffer, V attribute) {
        throw error();
    }

    @Override
    public void verifyAttribute(V value) {
        throw error();
    }

    @Override
    public V convert(Object value) {
        throw error();
    }

}
