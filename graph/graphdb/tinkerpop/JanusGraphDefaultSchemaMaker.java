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

package grakn.core.graph.graphdb.tinkerpop;

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.DefaultSchemaMaker;
import grakn.core.graph.core.schema.PropertyKeyMaker;

import java.util.Date;
import java.util.UUID;

/**
 * DefaultSchemaMaker implementation for Janus graphs
 */
public class JanusGraphDefaultSchemaMaker implements DefaultSchemaMaker {

    @Override
    public Cardinality defaultPropertyCardinality(String key) {
        return Cardinality.SINGLE;
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory, Object value) {
        String name = factory.getName();
        Class actualClass = determineClass(value);
        return factory.cardinality(defaultPropertyCardinality(name)).dataType(actualClass).make();
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
    }

    protected Class determineClass(Object value) {
        if (value instanceof String) {
            return String.class;
        } else if (value instanceof Character) {
            return Character.class;
        } else if (value instanceof Boolean) {
            return Boolean.class;
        } else if (value instanceof Byte) {
            return Byte.class;
        } else if (value instanceof Short) {
            return Short.class;
        } else if (value instanceof Integer) {
            return Integer.class;
        } else if (value instanceof Long) {
            return Long.class;
        } else if (value instanceof Float) {
            return Float.class;
        } else if (value instanceof Double) {
            return Double.class;
        } else if (value instanceof Date) {
            return Date.class;
        } else if (value instanceof UUID) {
            return UUID.class;
        } else {
            return Object.class;
        }
    }
}
