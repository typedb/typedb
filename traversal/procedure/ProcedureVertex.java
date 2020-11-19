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

package grakn.core.traversal.procedure;

import grakn.core.common.exception.GraknException;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.graph.TraversalVertex;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.UNRECOGNISED_VALUE;

abstract class ProcedureVertex<PROPERTY extends ProcedureVertex.Filter<?>> extends TraversalVertex<ProcedureEdge, PROPERTY> {

    private final Procedure procedure;
    private final boolean isStartingVertex;

    ProcedureVertex(Identifier identifier, Procedure procedure, boolean isStartingVertex) {
        super(identifier);
        this.procedure = procedure;
        this.isStartingVertex = isStartingVertex;
    }

    public ProcedureVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(ProcedureVertex.Thing.class)));
    }

    public ProcedureVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(ProcedureVertex.Type.class)));
    }

    static class Thing extends ProcedureVertex<Filter.Thing<?>> {

        Thing(Identifier identifier, Procedure procedure, boolean isStartingVertex) {
            super(identifier, procedure, isStartingVertex);
        }

        public void property(TraversalVertex.Property.Thing property) {
            super.property(Filter.Thing.of(property));
        }

        @Override
        public boolean isThing() { return true; }

        @Override
        public ProcedureVertex.Thing asThing() { return this; }
    }

    static class Type extends ProcedureVertex<Filter.Type<?>> {

        Type(Identifier identifier, Procedure procedure, boolean isStartingVertex) {
            super(identifier, procedure, isStartingVertex);
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public ProcedureVertex.Type asType() { return this; }

        public void property(TraversalVertex.Property.Type property) {
            super.property(Filter.Type.of(property));
        }
    }

    static abstract class Filter<P extends TraversalVertex.Property> extends TraversalVertex.Property {

        private final P property;

        Filter(P property) {
            this.property = property;
        }

        P property() {
            return property;
        }

        @Override
        public String toString() {
            return "Filter: " + property.toString();
        }

        static abstract class Thing<P extends TraversalVertex.Property.Thing> extends Filter<P> {

            Thing(P property) {
                super(property);
            }

            public static Filter.Thing<?> of(TraversalVertex.Property.Thing property) {
                if (property.isIID()) return new IID(property.asIID());
                else if (property.isIsa()) return new Isa(property.asIsa());
                else if (property.isValue()) return new Value(property.asValue());
                else throw new GraknException(UNRECOGNISED_VALUE);
            }

            static class IID extends Filter.Thing<TraversalVertex.Property.Thing.IID> {

                IID(TraversalVertex.Property.Thing.IID property) {
                    super(property);
                }
            }

            static class Isa extends Filter.Thing<TraversalVertex.Property.Thing.Isa> {

                Isa(TraversalVertex.Property.Thing.Isa property) {
                    super(property);
                }
            }

            static class Value extends Filter.Thing<TraversalVertex.Property.Thing.Value> {

                Value(TraversalVertex.Property.Thing.Value property) {
                    super(property);
                }
            }
        }

        static abstract class Type<P extends TraversalVertex.Property.Type> extends Filter<P> {

            Type(P property) {
                super(property);
            }

            public static Filter.Type<?> of(TraversalVertex.Property.Type property) {
                if (property.isLabel()) return new Label(property.asLabel());
                else if (property.isAbstract()) return new Abstract(property.asAbstract());
                else if (property.isValueType()) return new ValueType(property.asValueType());
                else if (property.isRegex()) return new Regex(property.asRegex());
                else throw GraknException.of(UNRECOGNISED_VALUE);
            }

            static class Label extends Filter.Type<TraversalVertex.Property.Type.Label> {

                Label(TraversalVertex.Property.Type.Label property) {
                    super(property);
                }
            }

            static class Abstract extends Filter.Type<TraversalVertex.Property.Type.Abstract> {

                Abstract(TraversalVertex.Property.Type.Abstract property) {
                    super(property);
                }
            }

            static class ValueType extends Filter.Type<TraversalVertex.Property.Type.ValueType> {

                ValueType(TraversalVertex.Property.Type.ValueType property) {
                    super(property);
                }
            }

            static class Regex extends Filter.Type<TraversalVertex.Property.Type.Regex> {

                Regex(TraversalVertex.Property.Type.Regex property) {
                    super(property);
                }
            }
        }
    }
}
