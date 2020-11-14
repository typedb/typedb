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

package grakn.core.traversal.planner;

import com.google.ortools.linearsolver.MPVariable;
import grakn.core.common.exception.GraknException;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.property.VertexProperty;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

abstract class PlannerVertex {

    private final Planner planner;
    final Identifier identifier;
    private final Set<PlannerEdge> outgoing;
    private final Set<PlannerEdge> incoming;
    private MPVariable varIsStartingPoint;
    private MPVariable varHasIncomingEdge;
    private MPVariable varHasOutgoingEdge;

    PlannerVertex(Planner planner, Identifier identifier) {
        this.planner = planner;
        this.identifier = identifier;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
    }

    abstract Set<? extends VertexProperty> properties();

    abstract void initalise();

    void out(PlannerEdge edge) {
        outgoing.add(edge);
    }

    void in(PlannerEdge edge) {
        incoming.add(edge);
    }

    Identifier identifier() {
        return identifier;
    }

    Set<PlannerEdge> outs() {
        return outgoing;
    }

    Set<PlannerEdge> ins() {
        return incoming;
    }

    boolean isThing() {
        return false;
    }

    boolean isType() {
        return false;
    }

    PlannerVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(PlannerVertex.Thing.class)));
    }

    PlannerVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(PlannerVertex.Type.class)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlannerVertex that = (PlannerVertex) o;
        return (this.identifier.equals(that.identifier) && Objects.equals(this.properties(), that.properties()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, properties());
    }

    static class Thing extends PlannerVertex {

        private Set<VertexProperty.Thing> properties;

        Thing(Planner planner, Identifier identifier) {
            super(planner, identifier);
        }

        @Override
        boolean isThing() {
            return true;
        }

        @Override
        PlannerVertex.Thing asThing() {
            return this;
        }

        @Override
        Set<VertexProperty.Thing> properties() {
            return properties;
        }

        void properties(Set<VertexProperty.Thing> properties) {
            this.properties = properties;
        }

        @Override
        void initalise() {
            // TODO
        }
    }

    static class Type extends PlannerVertex {

        private Set<VertexProperty.Type> properties;

        Type(Planner planner, Identifier identifier) {
            super(planner, identifier);
        }

        @Override
        public boolean isType() {
            return true;
        }

        @Override
        public PlannerVertex.Type asType() {
            return this;
        }

        @Override
        Set<VertexProperty.Type> properties() {
            return properties;
        }

        void properties(Set<VertexProperty.Type> properties) {
            this.properties = properties;
        }

        @Override
        void initalise() {
            // TODO
        }
    }
}
