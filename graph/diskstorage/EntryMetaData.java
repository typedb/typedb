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

package grakn.core.graph.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import grakn.core.graph.util.encoding.StringEncoding;

import java.util.EnumMap;
import java.util.function.Function;

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
    public static class Map extends EnumMap<EntryMetaData, Object> {

        public Map() {
            super(EntryMetaData.class);
        }

        @Override
        public Object put(EntryMetaData key, Object value) {
            Preconditions.checkArgument(key.isValidData(value), "Invalid meta data [%s] for [%s]", value, key);
            return super.put(key, value);
        }

    }

}
