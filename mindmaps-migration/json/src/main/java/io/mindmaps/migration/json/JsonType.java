/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.migration.json;

import io.mindmaps.concept.ResourceType.DataType;
import mjson.Json;

/**
 * Enum reflecting the various possible JSON types
 */
enum JsonType {
    OBJECT,
    ARRAY,
    STRING(DataType.STRING),
    NUMBER(DataType.DOUBLE),
    INTEGER(DataType.LONG),
    BOOLEAN(DataType.BOOLEAN),
    NULL;

    /**
     * Get a JsonType from the given string representation of it as described in JSON schema (e.g. 'object', 'number')
     */
    static JsonType getType(String type) {
        try {
            return JsonType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("no such type '" + type + "'");
        }
    }

    /**
     * Get the JsonType of the given JSON
     */
    static JsonType getType(Json json) {
        if (json.isObject()) {
            return JsonType.OBJECT;
        } else if (json.isArray()) {
            return JsonType.ARRAY;
        } else if (json.isString()) {
            return JsonType.STRING;
        } else if (json.isNumber()) {
            return JsonType.NUMBER;
        } else if (json.isBoolean()) {
            return JsonType.BOOLEAN;
        } else if (json.isNull()) {
            return JsonType.NULL;
        } else {
            throw new RuntimeException("Unknown JSON type: " + json);
        }
    }

    private final DataType datatype;

    /**
     * Create a JsonType which is not a resource type in mindmaps (contains no primitive data)
     */
    JsonType() {
        datatype = null;
    }

    /**
     * Create a JsonType that represents a resource type with a particular datatype in mindmaps
     */
    JsonType(DataType datatype) {
        this.datatype = datatype;
    }

    /**
     * Get the string representation of a JsonType in JSON schema
     */
    String getName() {
        return toString().toLowerCase();
    }

    /**
     * Get the mindmaps datatype of the given JsonType, if it has one. Otherwise null.
     */
    DataType getDatatype() {
        return datatype;
    }

    /**
     * Return whether the given JsonType can be described as a resource type in mindmaps
     */
    boolean isResourceType() {
        return datatype != null;
    }
}
