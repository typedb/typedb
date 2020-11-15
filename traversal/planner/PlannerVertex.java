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
import grakn.core.traversal.graph.TraversalVertex;

import java.util.HashSet;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

abstract class PlannerVertex<PROPERTY extends TraversalVertex.Property> extends TraversalVertex<PlannerEdge, PROPERTY> {

    private final Planner planner;
    private MPVariable varIsStartingPoint;
    private MPVariable varHasIncomingEdges;
    private MPVariable varHasOutgoingEdges;
    private boolean isInitialised;
    boolean hasIndex;

    PlannerVertex(Planner planner, Identifier identifier) {
        super(identifier);
        this.planner = planner;
        this.hasIndex = false;
    }

    Planner planner() {
        return planner;
    }

    boolean hasIndex() {
        return hasIndex;
    }

    boolean isInitialised() {
        return isInitialised;
    }

    void initialiseVariables() {

    }

    void initialiseConstraints() {

    }

    PlannerVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(PlannerVertex.Thing.class)));
    }

    PlannerVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(PlannerVertex.Type.class)));
    }

    static class Thing extends PlannerVertex<TraversalVertex.Property.Thing> {

        private TraversalVertex.Property.Thing.IID iid;
        private TraversalVertex.Property.Thing.Isa isa;
        private final Set<TraversalVertex.Property.Thing.Value> value;

        Thing(Planner planner, Identifier identifier) {
            super(planner, identifier);
            this.value = new HashSet<>();
        }

        @Override
        public boolean isThing() { return true; }

        @Override
        PlannerVertex.Thing asThing() { return this; }

        @Override
        public void property(TraversalVertex.Property.Thing property) {
            if (property.isIndexed()) hasIndex = true;
            if (property.isIndexed()) iid = property.asIID();
            else if (property.isIsa()) isa = property.asIsa();
            else if (property.isValue()) value.add(property.asValue());
            properties.add(property);
        }
    }

    static class Type extends PlannerVertex<TraversalVertex.Property.Type> {

        private TraversalVertex.Property.Type.Label label;
        private TraversalVertex.Property.Type.Abstract abstractProp;
        private TraversalVertex.Property.Type.ValueType valueType;
        private TraversalVertex.Property.Type regex;

        Type(Planner planner, Identifier identifier) {
            super(planner, identifier);
            this.hasIndex = true; // VertexProperty.Type is always indexed
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public PlannerVertex.Type asType() { return this; }

        @Override
        public void property(TraversalVertex.Property.Type property) {
            assert property.isIndexed();
            if (property.isLabel()) label = property.asLabel();
            else if (property.isAbstract()) abstractProp = property.asAbstract();
            else if (property.isValueType()) valueType = property.asValueType();
            else if (property.isRegex()) regex = property = property.asRegex();
            properties.add(property);
        }
    }
}
