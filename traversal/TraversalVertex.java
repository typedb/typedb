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

    Identifier identifier() {
        return identifier;
    }

    void property(TraversalProperty property) {
        properties.add(property);
    }

    static class Pattern extends TraversalVertex<TraversalEdge.Pattern> {

        private final Traversal.Pattern pattern;

        Pattern(Identifier identifier, Traversal.Pattern pattern) {
            super(identifier);
            this.pattern = pattern;
        }

        @Override
        void out(TraversalEdge.Pattern edge) {
            outgoing.add(edge);
        }

        @Override
        void in(TraversalEdge.Pattern edge) {
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

    static class Plan extends TraversalVertex<TraversalEdge.Plan> {

        private final Traversal.Plan plan;

        Plan(Identifier identifier, Traversal.Plan plan) {
            super(identifier);
            this.plan = plan;
        }

        @Override
        void out(TraversalEdge.Plan edge) {
            // TODO
        }

        @Override
        void in(TraversalEdge.Plan edge) {
            // TODO
        }
    }
}
