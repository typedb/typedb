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

import grakn.core.traversal.Identifier;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class TraversalVertex<EDGE extends TraversalEdge<?>, PROPERTY extends VertexProperty> {

    private final Identifier identifier;
    private final Set<EDGE> outgoing;
    private final Set<EDGE> incoming;
    private final int hash;

    protected final Set<PROPERTY> properties;

    public TraversalVertex(Identifier identifier) {
        this.identifier = identifier;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
        this.properties = new HashSet<>();
        this.hash = Objects.hash(identifier);
    }

    public boolean isThing() { return false; }

    public boolean isType() { return false; }

    public Identifier identifier() {
        return identifier;
    }

    public Set<EDGE> outs() {
        return outgoing;
    }

    public Set<EDGE> ins() {
        return incoming;
    }

    public void out(EDGE edge) {
        outgoing.add(edge);
    }

    public void in(EDGE edge) {
        incoming.add(edge);
    }

    public Set<PROPERTY> properties() {
        return properties;
    }

    public void property(PROPERTY property) {
        properties.add(property);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TraversalVertex<?, ?> that = (TraversalVertex<?, ?>) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
