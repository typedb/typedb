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

package grakn.core.graph.core.schema;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.indexing.KeyInformation;
import grakn.core.graph.graphdb.types.ParameterType;

/**
 * Used to change the default mapping of an indexed key by providing the mapping explicitly as a parameter to
 * JanusGraphManagement#addIndexKey(JanusGraphIndex, PropertyKey, Parameter[]).
 * <p>
 * This applies mostly to string data types of keys, where the mapping specifies whether the string value is tokenized
 * (#TEXT) or indexed as a whole (#STRING), or both (#TEXTSTRING).
 */
public enum Mapping {

    DEFAULT,
    TEXT,
    STRING,
    TEXTSTRING,
    PREFIX_TREE;

    /**
     * Returns the mapping as a parameter so that it can be passed to JanusGraphManagement#addIndexKey(JanusGraphIndex, PropertyKey, Parameter[])
     */
    public Parameter asParameter() {
        return ParameterType.MAPPING.getParameter(this);
    }

    //------------ USED INTERNALLY -----------

    public static Mapping getMapping(KeyInformation information) {
        Object value = ParameterType.MAPPING.findParameter(information.getParameters(), null);
        if (value == null) return DEFAULT;
        else {
            Preconditions.checkArgument((value instanceof Mapping || value instanceof String), "Invalid mapping specified: %s", value);
            if (value instanceof String) {
                value = Mapping.valueOf(value.toString().toUpperCase());
            }
            return (Mapping) value;
        }
    }

    public static Mapping getMapping(String store, String key, KeyInformation.IndexRetriever information) {
        KeyInformation ki = information.get(store, key);
        Preconditions.checkNotNull(ki, "Could not find key information for: %s", key);
        return getMapping(ki);
    }


}
