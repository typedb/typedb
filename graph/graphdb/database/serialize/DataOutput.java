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

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

public interface DataOutput extends WriteBuffer {

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putLong(long val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putInt(int val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putShort(short val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putByte(byte val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putBytes(byte[] val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putBytes(StaticBuffer val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putChar(char val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putFloat(float val);

    @Override
    org.janusgraph.graphdb.database.serialize.DataOutput putDouble(double val);

    org.janusgraph.graphdb.database.serialize.DataOutput writeObject(Object object, Class<?> type);

    org.janusgraph.graphdb.database.serialize.DataOutput writeObjectByteOrder(Object object, Class<?> type);

    org.janusgraph.graphdb.database.serialize.DataOutput writeObjectNotNull(Object object);

    org.janusgraph.graphdb.database.serialize.DataOutput writeClassAndObject(Object object);

}
