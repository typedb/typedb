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
 *
 */

package grakn.core.traversal.graph;

import java.util.Objects;

public class TraversalEdge<VERTEX extends TraversalVertex<?, ?>> {

    private final EdgeProperty property;
    private final VERTEX from;
    private final VERTEX to;
    private final int hash;

    public TraversalEdge(EdgeProperty property, VERTEX from, VERTEX to) {
        this.property = property;
        this.from = from;
        this.to = to;
        this.hash = Objects.hash(property, from, to);
    }

    public EdgeProperty property() {
        return property;
    }

    public VERTEX from() {
        return from;
    }

    public VERTEX to() {
        return to;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        TraversalEdge<?> that = (TraversalEdge<?>) object;
        return (this.property.equals(that.property) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
