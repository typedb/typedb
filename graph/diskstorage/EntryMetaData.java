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

package grakn.core.graph.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import grakn.core.graph.util.encoding.StringEncoding;

import java.util.EnumMap;
import java.util.function.Function;

/**
 * @author Alexander Patrikalakis (amcp@mit.edu)
 */
public enum EntryMetaData {

    TTL(Integer.class, false, data -> data instanceof Integer && ((Integer) data) >= 0L),
    VISIBILITY(String.class, true, data -> data instanceof String && StringEncoding.isAsciiString((String) data)),
    TIMESTAMP(Long.class, false, data -> data instanceof Long);

    EntryMetaData(Class<?> dataType, boolean identifying, Function<Object, Boolean> validator) {
        this.dataType = dataType;
        this.identifying = identifying;
        this.validator = validator;
    }

    public static final java.util.Map<EntryMetaData, Object> EMPTY_METADATA = ImmutableMap.of();

    private final Class<?> dataType;
    private final boolean identifying;
    private final Function<Object, Boolean> validator;

    public Class<?> getDataType() {
        return dataType;
    }

    public boolean isIdentifying() {
        return identifying;
    }

    /**
     * Validates a datum according to the metadata type.
     *
     * @param datum object to validate
     * @return true if datum is a valid instance of this type and false otherwise.
     */
    public boolean isValidData(Object datum) {
        Preconditions.checkNotNull(datum);
        return validator.apply(datum);
    }

    /**
     * EntryMetaData.Map extends EnumMap to add validation prior to invoking the superclass EnumMap::put(k,v) method.
     */
    public static class Map extends EnumMap<org.janusgraph.diskstorage.EntryMetaData, Object> {

        public Map() {
            super(org.janusgraph.diskstorage.EntryMetaData.class);
        }

        @Override
        public Object put(org.janusgraph.diskstorage.EntryMetaData key, Object value) {
            Preconditions.checkArgument(key.isValidData(value), "Invalid meta data [%s] for [%s]", value, key);
            return super.put(key, value);
        }

    }

}
