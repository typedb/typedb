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
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Predicate;
import grakn.core.traversal.graph.TraversalVertex;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.single;
import static grakn.core.traversal.common.Predicate.Operator.Equality.EQ;
import static java.util.Collections.emptyIterator;
import static java.util.stream.Collectors.toSet;

public abstract class ProcedureVertex<VERTEX extends Vertex<?, ?>, PROPERTIES extends TraversalVertex.Properties> extends TraversalVertex<ProcedureEdge<?, ?>, PROPERTIES> {

    private final boolean isStartingVertex;
    private final AtomicReference<Set<Integer>> dependedEdgeOrders;
    private ProcedureEdge<?, ?> iteratorEdge;

    ProcedureVertex(Identifier identifier, boolean isStartingVertex) {
        super(identifier);
        this.isStartingVertex = isStartingVertex;
        this.dependedEdgeOrders = new AtomicReference<>(null);
    }

    public abstract ResourceIterator<? extends VERTEX> iterator(GraphManager graphMgr, Traversal.Parameters parameters);

    @Override
    public void in(ProcedureEdge<?, ?> edge) {
        super.in(edge);
        if (iteratorEdge == null || edge.order() < iteratorEdge.order()) iteratorEdge = edge;
    }

    public boolean isStartingVertex() {
        return isStartingVertex;
    }

    public Set<Integer> dependedEdgeOrders() {
        dependedEdgeOrders.compareAndSet(null, computeDependedEdgeOrders());
        return dependedEdgeOrders.get();
    }

    private Set<Integer> computeDependedEdgeOrders() {
        if (ins().isEmpty()) return set();
        else return set(branchEdge().from().dependedEdgeOrders(), branchEdge().order());
    }

    public ProcedureEdge<?, ?> branchEdge() {
        if (ins().isEmpty()) return null;
        else return iteratorEdge;
    }

    public ProcedureVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(ProcedureVertex.Thing.class)));
    }

    public ProcedureVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(ProcedureVertex.Type.class)));
    }

    static class Thing extends ProcedureVertex<ThingVertex, Properties.Thing> {

        private Set<Filter.Thing> filters;

        Thing(Identifier identifier, boolean isStartingVertex) {
            super(identifier, isStartingVertex);
        }

        @Override
        protected Properties.Thing newProperties() {
            return new Properties.Thing();
        }

        @Override
        public void props(Properties.Thing properties) {
            filters = Filter.Thing.of(properties, identifier());
            super.props(properties);
        }

        @Override
        public boolean isThing() { return true; }

        @Override
        public ProcedureVertex.Thing asThing() { return this; }

        @Override
        public ResourceIterator<? extends ThingVertex> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex();
            if (props().hasIID()) {
                return iteratorFromIID(graphMgr, parameters);
            } else if (!props().types().isEmpty()) {
                return iteratorFromTypes(graphMgr, parameters);
            } else {
                throw GraknException.of(ILLEGAL_STATE);
            }
        }

        private ResourceIterator<? extends ThingVertex> iteratorFromIID(GraphManager graphMgr,
                                                                        Traversal.Parameters parameters) {
            assert props().hasIID() && identifier().isVariable();
            Identifier.Variable id = identifier().asVariable();
            ResourceIterator<? extends ThingVertex> iter = single(graphMgr.data().get(parameters.getIID(id))).noNulls();
            if (!props().types().isEmpty()) iter = filterTypes(iter);
            if (!props().predicates().isEmpty()) iter = filterPredicates(filterAttributes(iter), parameters, id);
            return iter;
        }

        private ResourceIterator<? extends ThingVertex> filterTypes(ResourceIterator<? extends ThingVertex> iterator) {
            return iterator.filter(v -> props().types().contains(v.type().properLabel()));
        }

        private ResourceIterator<AttributeVertex<?>> filterAttributes(ResourceIterator<? extends ThingVertex> iterator) {
            // TODO: should we throw an exception if the user asserts a value predicate on a non-attribute?
            return iterator.filter(ThingVertex::isAttribute).map(ThingVertex::asAttribute);
        }

        private ResourceIterator<? extends ThingVertex> filterPredicates(ResourceIterator<AttributeVertex<?>> iterator,
                                                                         Traversal.Parameters parameters,
                                                                         Identifier.Variable id) {
            // TODO: should we throw an exception if the user assert a value non-comparable value types?
            for (Predicate<?> predicate : props().predicates()) {
                for (Traversal.Parameters.Value value : parameters.getValues(id, predicate)) {
                    iterator = iterator.filter(a -> predicate.apply(a, value));
                }
            }
            return iterator;
        }

        private ResourceIterator<? extends ThingVertex> iteratorFromTypes(GraphManager graphMgr,
                                                                          Traversal.Parameters parameters) {
            assert !props().types().isEmpty();
            ResourceIterator<? extends ThingVertex> iterator;
            Optional<Predicate<?>> eq;
            if ((eq = props().predicates().stream().filter(p -> p.operator().equals(EQ)).findFirst()).isPresent()) {
                iterator = iteratorOfAttributes(graphMgr, parameters, eq.get());
            } else {
                iterator = iterate(props().types().iterator())
                        .map(l -> graphMgr.schema().getType(l)).noNulls()
                        .flatMap(t -> graphMgr.data().get(t)).noNulls();
            }

            if (!props().predicates().isEmpty()) {
                iterator = filterPredicates(filterAttributes(iterator), parameters, identifier().asVariable());
            }

            return iterator;
        }

        private ResourceIterator<? extends AttributeVertex<?>> iteratorOfAttributes(GraphManager graphMgr,
                                                                                    Traversal.Parameters parameters,
                                                                                    Predicate<?> eqPredicate) {
            // TODO: should we throw an exception if the user asserts 2 values for a given vertex?
            assert identifier().isVariable();
            Set<Traversal.Parameters.Value> values = parameters.getValues(identifier().asVariable(), eqPredicate);
            if (values.size() > 1) return iterate(emptyIterator());
            return iterate(props().types().iterator())
                    .map(l -> graphMgr.schema().getType(l)).noNulls()
                    .map(t -> attributeVertex(graphMgr, t, values.iterator().next())).noNulls();
        }

        private AttributeVertex<?> attributeVertex(GraphManager graphMgr, TypeVertex type,
                                                   Traversal.Parameters.Value value) {
            if (value.isBoolean()) return graphMgr.data().get(type, value.getBoolean());
            else if (value.isLong()) return graphMgr.data().get(type, value.getLong());
            else if (value.isDouble()) return graphMgr.data().get(type, value.getDouble());
            else if (value.isString()) return graphMgr.data().get(type, value.getString());
            else if (value.isDateTime()) return graphMgr.data().get(type, value.getDateTime());
            else throw GraknException.of(ILLEGAL_STATE);
        }
    }

    static class Type extends ProcedureVertex<TypeVertex, Properties.Type> {

        private Set<Filter.Type> filters;

        Type(Identifier identifier, boolean isStartingVertex) {
            super(identifier, isStartingVertex);
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public void props(Properties.Type properties) {
            filters = Filter.Type.of(properties);
            super.props(properties);
        }

        @Override
        public ResourceIterator<TypeVertex> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            return null; // TODO
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public ProcedureVertex.Type asType() { return this; }
    }

    abstract static class Filter {

        @Override
        public abstract String toString();

        abstract static class Thing extends Filter {

            static Set<Filter.Thing> of(Properties.Thing property, Identifier identifier) {
                Set<Filter.Thing> filters = new HashSet<>();
                if (property.hasIID()) filters.add(new IID(identifier));
                else if (!property.types().isEmpty()) filters.add(new Types(property.types()));
                else if (!property.predicates().isEmpty()) filters.addAll(
                        property.predicates().stream().map(c -> new Predicate(c, identifier)).collect(toSet())
                );
                return filters;
            }

            static class IID extends Filter.Thing {

                private final Identifier param;

                IID(Identifier param) {
                    this.param = param;
                }

                @Override
                public String toString() {
                    return String.format("Filter: IID { iid: param(%s) }", param);
                }
            }

            static class Types extends Filter.Thing {

                private final Set<Label> labels;

                Types(Set<Label> labels) {
                    this.labels = labels;
                }

                @Override
                public String toString() {
                    return String.format("Filter: Types { labels: %s }", labels);
                }
            }

            static class Predicate extends Filter.Thing {

                private final grakn.core.traversal.common.Predicate<?> predicate;
                private final Identifier param;

                Predicate(grakn.core.traversal.common.Predicate<?> predicate, Identifier param) {
                    this.predicate = predicate;
                    this.param = param;
                }

                @Override
                public String toString() {
                    return String.format("Filter: Value { predicate: %s, value: param(%s) }", predicate, param);
                }
            }
        }

        static abstract class Type extends Filter {

            static Set<Filter.Type> of(Properties.Type properties) {
                Set<Filter.Type> filters = new HashSet<>();
                if (!properties.labels().isEmpty()) filters.add(new Labels(properties.labels()));
                else if (properties.isAbstract()) filters.add(new Abstract());
                else if (properties.valueType().isPresent()) filters.add(new ValueType(properties.valueType().get()));
                else if (properties.regex().isPresent()) filters.add(new Regex(properties.regex().get()));
                return filters;
            }

            static class Labels extends Filter.Type {

                private final Set<Label> labels;

                Labels(Set<Label> labels) {
                    this.labels = labels;
                }

                @Override
                public String toString() {
                    return String.format("Filter: Label { label: %s }", labels);
                }
            }

            static class Abstract extends Filter.Type {

                Abstract() {}

                @Override
                public String toString() {
                    return "Filter: Abstract { abstract: true }";
                }
            }

            static class ValueType extends Filter.Type {

                private final Encoding.ValueType valueType;

                ValueType(Encoding.ValueType valueType) {
                    this.valueType = valueType;
                }

                @Override
                public String toString() {
                    return String.format("Filter: Value Type { value: %s }", valueType);
                }
            }

            static class Regex extends Filter.Type {

                private final String regex;

                Regex(String regex) {
                    this.regex = regex;
                }

                @Override
                public String toString() {
                    return String.format("Filter: Regex { regex: %s }", regex);
                }
            }
        }
    }
}
