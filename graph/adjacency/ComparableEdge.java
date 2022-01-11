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

public abstract class ComparableEdge {

    public abstract Edge<?, ?, ?> edge();

    public abstract EdgeIID<?, ?, ?, ?> iid();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ComparableEdge that = (ComparableEdge) o;
        return iid().equals(that.iid());
    }

    @Override
    public int hashCode() {
        return iid().hashCode();
    }

    abstract static class Type extends ComparableEdge implements Comparable<Type> {

        final TypeEdge edge;

        Type(TypeEdge edge) {
            this.edge = edge;
        }

        public TypeEdge edge() {
            return edge;
        }

        @Override
        public abstract EdgeIID.Type iid();

        public static Type byInIID(TypeEdge edge) {
            return create(edge, edge.inIID());
        }

        public static Type byOutIID(TypeEdge edge) {
            return create(edge, edge.outIID());
        }

        private static Type create(TypeEdge edge, EdgeIID.Type iid) {
            return new Type(edge) {
                @Override
                public EdgeIID.Type iid() {
                    return iid;
                }
            };
        }

        @Override
        public int compareTo(Type other) {
            return iid().compareTo(other.iid());
        }
    }

    public abstract static class Thing extends ComparableEdge implements Comparable<Thing> {
        final ThingEdge edge;

        Thing(ThingEdge edge) {
            this.edge = edge;
        }

        public ThingEdge edge() {
            return edge;
        }

        @Override
        public abstract EdgeIID.Thing iid();

        public static Thing byInIID(ThingEdge edge) {
            return create(edge, edge.inIID());
        }

        public static Thing byOutIID(ThingEdge edge) {
            return create(edge, edge.outIID());
        }

        private static Thing create(ThingEdge edge, EdgeIID.Thing iid) {
            return new Thing(edge) {
                @Override
                public EdgeIID.Thing iid() {
                    return iid;
                }
            };
        }

        @Override
        public int compareTo(Thing other) {
            return iid().compareTo(other.iid());
        }
    }
}
