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

package grakn.core.graph.core;

// TODO is this vestigial now that TP3's VertexProperty.Cardinality exists?

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * The cardinality of the values associated with given key for a particular element.
 *
*/
public enum Cardinality {

    /**
     * Only a single value may be associated with the given key.
     */
    SINGLE,

    /**
     * Multiple values and duplicate values may be associated with the given key.
     */
    LIST,


    /**
     * Multiple but distinct values may be associated with the given key.
     */
    SET;

    public org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality convert() {
        switch (this) {
            case SINGLE: return VertexProperty.Cardinality.single;
            case LIST: return VertexProperty.Cardinality.list;
            case SET: return VertexProperty.Cardinality.set;
            default: throw new AssertionError("Unrecognized cardinality: " + this);
        }
    }

    public static Cardinality convert(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality cardinality) {
        switch (cardinality) {
            case single: return SINGLE;
            case list: return LIST;
            case set: return SET;
            default: throw new AssertionError("Unrecognized cardinality: " + cardinality);
        }
    }


}
