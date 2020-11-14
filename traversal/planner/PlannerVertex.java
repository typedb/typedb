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

    final Planner planner;
    final Identifier identifier;
    final Set<PlannerEdge> outgoing;
    final Set<PlannerEdge> incoming;
    boolean isIndexed;
    MPVariable varIsStartingPoint;
    MPVariable varHasIncomingEdge;
    MPVariable varHasOutgoingEdge;

    PlannerVertex(Planner planner, Identifier identifier) {
        this.planner = planner;
        this.identifier = identifier;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
        this.isIndexed = false;
    }

    abstract Set<? extends VertexProperty> properties();

    abstract void initialise();

    void out(PlannerEdge edge) {
        outgoing.add(edge);
    }

    void in(PlannerEdge edge) {
        incoming.add(edge);
    }

    boolean isIndexed() {
        return isIndexed;
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

        private final Set<VertexProperty.Thing> properties;
        private VertexProperty.Thing.IID iid;
        private VertexProperty.Thing.Isa isa;
        private VertexProperty.Thing.Value value;

        Thing(Planner planner, Identifier identifier) {
            super(planner, identifier);
            this.properties = new HashSet<>();
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

        void property(VertexProperty.Thing property) {
            if (property.isIndexed()) isIndexed = true;
            if (property.isIndexed()) iid = property.asIID();
            else if (property.isIsa()) isa = property.asIsa();
            else if (property.isValue()) value = property.asValue();
            properties.add(property);
        }

        @Override
        void initialise() {

        }
    }

    static class Type extends PlannerVertex {

        private final Set<VertexProperty.Type> properties;
        private VertexProperty.Type.Label label;
        private VertexProperty.Type.Abstract abstractProp;
        private VertexProperty.Type.ValueType valueType;
        private VertexProperty.Type regex;

        Type(Planner planner, Identifier identifier) {
            super(planner, identifier);
            this.properties = new HashSet<>();
            this.isIndexed = true; // VertexProperty.Type is always indexed
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

        void property(VertexProperty.Type property) {
            assert property.isIndexed();
            if (property.isLabel()) label = property.asLabel();
            else if (property.isAbstract()) abstractProp = property.asAbstract();
            else if (property.isValueType()) valueType = property.asValueType();
            else if (property.isRegex()) regex = property = property.asRegex();
            properties.add(property);
        }

        @Override
        void initialise() {
            // TODO
        }
    }
}
