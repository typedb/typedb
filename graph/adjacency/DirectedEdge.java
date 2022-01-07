/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.graph.adjacency;

import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.TypeEdge;
import com.vaticle.typedb.core.graph.iid.EdgeIID;

public abstract class DirectedEdge {

    public abstract Edge<?, ?, ?> get();

    public abstract EdgeIID<?, ?, ?, ?> directedIID();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DirectedEdge that = (DirectedEdge) o;
        return directedIID().equals(that.directedIID());
    }

    @Override
    public int hashCode() {
        return directedIID().hashCode();
    }

    abstract static class Type extends DirectedEdge implements Comparable<Type> {
        final TypeEdge edge;

        Type(TypeEdge edge) {
            this.edge = edge;
        }

        public TypeEdge get() {
            return edge;
        }

        @Override
        public EdgeIID.Type directedIID() {
            return null;
        }

        public static Type in(TypeEdge edge) {
            return create(edge, edge.inIID());
        }

        public static Type out(TypeEdge edge) {
            return create(edge, edge.outIID());
        }

        private static Type create(TypeEdge edge, EdgeIID.Type iid) {
            return new Type(edge) {
                @Override
                public EdgeIID.Type directedIID() {
                    return iid;
                }
            };
        }

        @Override
        public int compareTo(Type other) {
            return directedIID().compareTo(other.directedIID());
        }
    }

    public abstract static class Thing extends DirectedEdge implements Comparable<Thing> {
        final ThingEdge edge;

        Thing(ThingEdge edge) {
            this.edge = edge;
        }

        public ThingEdge get() {
            return edge;
        }

        @Override
        public EdgeIID.Thing directedIID() {
            return null;
        }

        public static Thing in(ThingEdge edge) {
            return create(edge, edge.inIID());
        }

        public static Thing out(ThingEdge edge) {
            return create(edge, edge.outIID());
        }

        private static Thing create(ThingEdge edge, EdgeIID.Thing iid) {
            return new Thing(edge) {
                @Override
                public EdgeIID.Thing directedIID() {
                    return iid;
                }
            };
        }

        @Override
        public int compareTo(Thing other) {
            return directedIID().compareTo(other.directedIID());
        }
    }
}
