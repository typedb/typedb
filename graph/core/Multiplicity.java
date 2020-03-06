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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * The multiplicity of edges between vertices for a given label. Multiplicity here is understood in the same sense as
 * for UML class diagrams <a href="https://en.wikipedia.org/wiki/Class_diagram#Multiplicity">https://en.wikipedia.org/wiki/Class_diagram#Multiplicity</a>
 */
public enum Multiplicity {

    /**
     * The given edge label specifies a multi-graph, meaning that the multiplicity is not constrained and that
     * there may be multiple edges of this label between any given pair of vertices.
     *
     * <a href="https://en.wikipedia.org/wiki/Multigraph">https://en.wikipedia.org/wiki/Multigraph</a>
     */
    MULTI,

    /**
     * The given edge label specifies a simple graph, meaning that the multiplicity is not constrained but that there
     * can only be at most a single edge of this label between a given pair of vertices.
     */
    SIMPLE,

    /**
     * There can only be a single in-edge of this label for a given vertex but multiple out-edges (i.e. in-unique)
     */
    ONE2MANY,

    /**
     * There can only be a single out-edge of this label for a given vertex but multiple in-edges (i.e. out-unique)
     */
    MANY2ONE,

    /**
     * There can be only a single in and out-edge of this label for a given vertex (i.e. unique in both directions).
     */
    ONE2ONE;


    /**
     * Whether this multiplicity imposes any constraint on the number of edges that may exist between a pair of vertices.
     */
    public boolean isConstrained() {
        return this != MULTI;
    }

    public boolean isConstrained(Direction direction) {
        if (direction == Direction.BOTH) return isConstrained();
        return this != MULTI && (this == SIMPLE || isUnique(direction));
    }


    /**
     * If this multiplicity implies edge uniqueness in the given direction for any given vertex.
     */
    public boolean isUnique(Direction direction) {
        switch (direction) {
            case IN:
                return this == ONE2MANY || this == ONE2ONE;
            case OUT:
                return this == MANY2ONE || this == ONE2ONE;
            case BOTH:
                return this == ONE2ONE;
            default:
                throw new AssertionError("Unknown direction: " + direction);
        }
    }

    //######### CONVERTING MULTIPLICITY <-> CARDINALITY ########

    public static Multiplicity convert(Cardinality cardinality) {
        Preconditions.checkNotNull(cardinality);
        switch (cardinality) {
            case LIST:
                return MULTI;
            case SET:
                return SIMPLE;
            case SINGLE:
                return MANY2ONE;
            default:
                throw new AssertionError("Unknown cardinality: " + cardinality);
        }
    }

    public Cardinality getCardinality() {
        switch (this) {
            case MULTI:
                return Cardinality.LIST;
            case SIMPLE:
                return Cardinality.SET;
            case MANY2ONE:
                return Cardinality.SINGLE;
            default:
                throw new AssertionError("Invalid multiplicity: " + this);
        }
    }

}
