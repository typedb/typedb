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

package grakn.core.traversal;

import java.util.HashSet;
import java.util.Set;

abstract class TraversalVertex<E extends TraversalEdge<?>> {

    private final Identifier identifier;
    private final Set<TraversalProperty> properties;
    final Set<E> outgoing;
    final Set<E> incoming;

    TraversalVertex(Identifier identifier) {
        this.identifier = identifier;
        this.properties = new HashSet<>();
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
    }

    abstract void out(E edge);

    abstract void in(E edge);

    public Set<E> outs() {
        return outgoing;
    }

    public Set<E> ins() {
        return incoming;
    }

    Identifier identifier() {
        return identifier;
    }

    void property(TraversalProperty property) {
        properties.add(property);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TraversalVertex<?> that = (TraversalVertex<?>) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    static class Structure extends TraversalVertex<TraversalEdge.Structure> {

        private final Traversal.Structure structure;

        Structure(Identifier identifier, Traversal.Structure structure) {
            super(identifier);
            this.structure = structure;
        }

        @Override
        void out(TraversalEdge.Structure edge) {
            outgoing.add(edge);
        }

        @Override
        void in(TraversalEdge.Structure edge) {
            incoming.add(edge);
        }
    }

    static class Planner extends TraversalVertex<TraversalEdge.Planner> {

        private final Traversal.Planner planner;

        Planner(Identifier identifier, Traversal.Planner planner) {
            super(identifier);
            this.planner = planner;
        }

        @Override
        void out(TraversalEdge.Planner edge) {
            // TODO
        }

        @Override
        void in(TraversalEdge.Planner edge) {
            // TODO
        }
    }

    static class Procedure extends TraversalVertex<TraversalEdge.Procedure> {

        private final Traversal.Procedure procedure;

        Procedure(Identifier identifier, Traversal.Procedure procedure) {
            super(identifier);
            this.procedure = procedure;
        }

        @Override
        void out(TraversalEdge.Procedure edge) {
            // TODO
        }

        @Override
        void in(TraversalEdge.Procedure edge) {
            // TODO
        }
    }
}
