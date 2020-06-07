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

package grakn.core.graph.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.internal.Token;
import grakn.core.graph.graphdb.types.TypeUtil;

import java.util.Map;
import java.util.Set;

public abstract class SystemTypeManager {

    private final static Map<Long, SystemRelationType> SYSTEM_TYPES_BY_ID;
    private final static Map<String, SystemRelationType> SYSTEM_TYPES_BY_NAME;
    private static final Set<String> ADDITIONAL_RESERVED_NAMES;
    private static final char[] RESERVED_CHARS = {'{', '}', '"', Token.SEPARATOR_CHAR};

    static {
        synchronized (SystemTypeManager.class) {
            ImmutableMap.Builder<Long, SystemRelationType> idBuilder = ImmutableMap.builder();
            ImmutableMap.Builder<String, SystemRelationType> nameBuilder = ImmutableMap.builder();
            for (SystemRelationType et : new SystemRelationType[]{BaseKey.SchemaCategory, BaseKey.SchemaDefinitionDesc,
                    BaseKey.SchemaDefinitionProperty, BaseKey.SchemaName, BaseKey.SchemaUpdateTime,
                    BaseKey.VertexExists,
                    BaseLabel.VertexLabelEdge, BaseLabel.SchemaDefinitionEdge,
                    ImplicitKey.ID, ImplicitKey.JANUSGRAPHID, ImplicitKey.LABEL,
                    ImplicitKey.KEY, ImplicitKey.VALUE, ImplicitKey.ADJACENT_ID,
                    ImplicitKey.TIMESTAMP, ImplicitKey.TTL, ImplicitKey.VISIBILITY
            }) {
                idBuilder.put(et.longId(), et);
                nameBuilder.put(et.name(), et);
            }

            SYSTEM_TYPES_BY_ID = idBuilder.build();
            SYSTEM_TYPES_BY_NAME = nameBuilder.build();


            ADDITIONAL_RESERVED_NAMES = ImmutableSet.of(
                    "key", "vertex", "edge", "element", "property", "label");
        }
    }

    public static SystemRelationType getSystemType(long id) {
        return SYSTEM_TYPES_BY_ID.get(id);
    }

    public static SystemRelationType getSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.get(name);
    }

    public static void throwIfSystemName(JanusGraphSchemaCategory category, String name) {
        TypeUtil.checkTypeName(category, name);
        if (SystemTypeManager.isSystemType(name.toLowerCase()) || Token.isSystemName(name)) {
            throw new IllegalArgumentException("Name cannot be in protected namespace: " + name);
        }
        for (char c : RESERVED_CHARS) {
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name contains reserved character %s: %s", c, name);
        }
    }

    public static boolean isSystemType(String name) {
        return SYSTEM_TYPES_BY_NAME.containsKey(name) || ADDITIONAL_RESERVED_NAMES.contains(name);
    }


}
