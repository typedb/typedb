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
import grakn.core.graph.GraphManager;
import grakn.core.graph.edge.ThingEdge;
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Predicate;
import grakn.core.traversal.graph.TraversalVertex;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.common.iterator.Iterators.link;
import static grakn.core.common.iterator.Iterators.single;
import static grakn.core.graph.util.Encoding.ValueType.STRING;
import static grakn.core.traversal.common.Predicate.Operator.Equality.EQ;
import static java.util.Collections.emptyIterator;

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
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(ProcedureVertex.Thing.class));
    }

    public ProcedureVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(ProcedureVertex.Type.class));
    }

    static class Thing extends ProcedureVertex<ThingVertex, Properties.Thing> {

        Thing(Identifier identifier, boolean isStartingVertex) {
            super(identifier, isStartingVertex);
        }

        @Override
        protected Properties.Thing newProperties() {
            return new Properties.Thing();
        }

        @Override
        public boolean isThing() { return true; }

        @Override
        public ProcedureVertex.Thing asThing() { return this; }

        @Override
        public ResourceIterator<? extends ThingVertex> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex();
            if (props().hasIID()) return iterateAndFilterFromIID(graphMgr, parameters);
            else if (!props().types().isEmpty()) return iterateAndFilterFromTypes(graphMgr, parameters);
            else throw GraknException.of(ILLEGAL_STATE);
        }

        ResourceIterator<? extends ThingVertex> iterateAndFilterFromIID(GraphManager graphMgr,
                                                                        Traversal.Parameters parameters) {
            assert props().hasIID() && id().isVariable();
            Identifier.Variable id = id().asVariable();
            ResourceIterator<? extends ThingVertex> iter = single(graphMgr.data().get(parameters.getIID(id))).noNulls();
            if (!props().types().isEmpty()) iter = filterTypes(iter);
            if (!props().predicates().isEmpty()) iter = filterPredicates(iter, parameters);
            return iter;
        }

        ResourceIterator<? extends ThingVertex> iterateAndFilterFromTypes(GraphManager graphMgr,
                                                                          Traversal.Parameters parameters) {
            assert !props().types().isEmpty();
            ResourceIterator<? extends ThingVertex> iter;
            Optional<Predicate.Value<?>> eq;

            if ((eq = props().predicates().stream().filter(p -> p.operator().equals(EQ)).findFirst()).isPresent()) {
                iter = iteratorOfAttributes(graphMgr, parameters, eq.get());
            } else {
                iter = iterate(props().types().iterator())
                        .map(l -> graphMgr.schema().getType(l)).noNulls()
                        .flatMap(t -> graphMgr.data().get(t)).noNulls();
            }

            if (props().predicates().isEmpty()) return iter;
            else return filterPredicates(iter, parameters, eq.orElse(null));
        }

        ResourceIterator<? extends ThingVertex> filterIID(ResourceIterator<? extends ThingVertex> iterator, Traversal.Parameters parameters) {
            return iterator.filter(v -> v.iid().equals(parameters.getIID(id().asVariable())));
        }

        ResourceIterator<ThingEdge> filterIIDOnEdge(ResourceIterator<ThingEdge> iterator,
                                                    Traversal.Parameters parameters, boolean isForward) {
            Function<ThingEdge, ThingVertex> fn = e -> isForward ? e.to() : e.from();
            return iterator.filter(e -> fn.apply(e).iid().equals(parameters.getIID(id().asVariable())));
        }

        ResourceIterator<? extends ThingVertex> filterTypes(ResourceIterator<? extends ThingVertex> iterator) {
            return iterator.filter(v -> props().types().contains(v.type().properLabel()));
        }

        ResourceIterator<ThingEdge> filterTypesOnEdge(ResourceIterator<ThingEdge> iterator, boolean isForward) {
            Function<ThingEdge, ThingVertex> fn = e -> isForward ? e.to() : e.from();
            return iterator.filter(e -> props().types().contains(fn.apply(e).type().properLabel()));
        }

        ResourceIterator<? extends AttributeVertex<?>> filterPredicates(ResourceIterator<? extends ThingVertex> iterator,
                                                                        Traversal.Parameters parameters) {
            return filterPredicates(iterator, parameters, null);
        }

        ResourceIterator<? extends AttributeVertex<?>> filterPredicates(ResourceIterator<? extends ThingVertex> iterator,
                                                                        Traversal.Parameters parameters,
                                                                        @Nullable Predicate.Value<?> exclude) {
            // TODO: should we throw an exception if the user asserts a value predicate on a non-attribute?
            // TODO: should we throw an exception if the user assert a value non-comparable value types?
            assert id().isVariable();
            ResourceIterator<? extends AttributeVertex<?>> attributes =
                    iterator.filter(ThingVertex::isAttribute).<AttributeVertex<?>>map(ThingVertex::asAttribute);
            for (Predicate.Value<?> predicate : props().predicates()) {
                if (Objects.equals(predicate, exclude)) break;
                for (Traversal.Parameters.Value value : parameters.getValues(id().asVariable(), predicate)) {
                    attributes = attributes.filter(a -> predicate.apply(a, value));
                }
            }
            return attributes;
        }

        ResourceIterator<ThingEdge> filterPredicatesOnEdge(ResourceIterator<ThingEdge> iterator,
                                                           Traversal.Parameters parameters, boolean isForward) {
            assert id().isVariable();
            Function<ThingEdge, ThingVertex> fn = e -> isForward ? e.to() : e.from();
            iterator = iterator.filter(e -> fn.apply(e).isAttribute());
            for (Predicate.Value<?> predicate : props().predicates()) {
                for (Traversal.Parameters.Value value : parameters.getValues(id().asVariable(), predicate)) {
                    iterator = iterator.filter(e -> predicate.apply(fn.apply(e).asAttribute(), value));
                }
            }
            return iterator;
        }

        ResourceIterator<? extends AttributeVertex<?>> iteratorOfAttributes(GraphManager graphMgr,
                                                                            Traversal.Parameters parameters,
                                                                            Predicate.Value<?> eqPredicate) {
            // TODO: should we throw an exception if the user asserts 2 values for a given vertex?
            assert id().isVariable();
            Set<Traversal.Parameters.Value> values = parameters.getValues(id().asVariable(), eqPredicate);
            if (values.size() > 1) return iterate(emptyIterator());
            return iterate(props().types().iterator())
                    .map(l -> graphMgr.schema().getType(l)).noNulls().filter(TypeVertex::isAttributeType)
                    .map(t -> attributeVertex(graphMgr, t, values.iterator().next())).noNulls();
        }

        private AttributeVertex<?> attributeVertex(GraphManager graphMgr, TypeVertex type,
                                                   Traversal.Parameters.Value value) {
            assert type.isAttributeType();
            if (value.isBoolean()) return graphMgr.data().get(type, value.getBoolean());
            else if (value.isLong()) return graphMgr.data().get(type, value.getLong());
            else if (value.isDouble()) return graphMgr.data().get(type, value.getDouble());
            else if (value.isString()) return graphMgr.data().get(type, value.getString());
            else if (value.isDateTime()) return graphMgr.data().get(type, value.getDateTime());
            else throw GraknException.of(ILLEGAL_STATE);
        }
    }

    static class Type extends ProcedureVertex<TypeVertex, Properties.Type> {

        Type(Identifier identifier, boolean isStartingVertex) {
            super(identifier, isStartingVertex);
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public ResourceIterator<TypeVertex> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex() && id().isVariable();
            ResourceIterator<TypeVertex> iterator = null;

            if (!props().labels().isEmpty()) iterator = iterateLabels(graphMgr);
            if (props().valueType().isPresent()) iterator = iterateOrFilterValueTypes(graphMgr, iterator);
            if (props().isAbstract()) iterator = iterateOrFilterAbstract(graphMgr, iterator);
            if (props().regex().isPresent()) iterator = iterateAndFilterRegex(graphMgr, iterator);
            if (iterator == null)
                iterator = link(list(graphMgr.schema().entityTypes(), graphMgr.schema().relationTypes(),
                                     graphMgr.schema().attributeTypes()));// graphMgr.schema().roleTypes())); // TODO discuss ramifications
            return iterator;
        }

        ResourceIterator<TypeVertex> filter(ResourceIterator<TypeVertex> iterator) {
            if (!props().labels().isEmpty()) iterator = filterLabels(iterator);
            if (props().valueType().isPresent()) iterator = filterValueTypes(iterator);
            if (props().isAbstract()) iterator = filterAbstract(iterator);
            if (props().regex().isPresent()) iterator = filterRegex(iterator);
            return iterator;
        }

        private ResourceIterator<TypeVertex> iterateLabels(GraphManager graphMgr) {
            return iterate(props().labels().iterator()).map(l -> graphMgr.schema().getType(l)).noNulls();
        }

        private ResourceIterator<TypeVertex> filterLabels(ResourceIterator<TypeVertex> iterator) {
            return iterator.filter(t -> props().labels().contains(t.properLabel()));
        }

        private ResourceIterator<TypeVertex> iterateOrFilterValueTypes(GraphManager graphMgr,
                                                                       ResourceIterator<TypeVertex> iterator) {
            assert props().valueType().isPresent();
            if (iterator == null) return graphMgr.schema().attributeTypes(props().valueType().get());
            else return filterValueTypes(iterator);
        }

        private ResourceIterator<TypeVertex> filterValueTypes(ResourceIterator<TypeVertex> iterator) {
            assert props().valueType().isPresent();
            return iterator.filter(t -> Objects.equals(t.valueType(), props().valueType().get()));
        }

        private ResourceIterator<TypeVertex> iterateOrFilterAbstract(GraphManager graphMgr,
                                                                     ResourceIterator<TypeVertex> iterator) {
            if (iterator == null) return graphMgr.schema().thingTypes().filter(TypeVertex::isAbstract);
            else return filterAbstract(iterator);
        }

        private ResourceIterator<TypeVertex> filterAbstract(ResourceIterator<TypeVertex> iterator) {
            return iterator.filter(TypeVertex::isAbstract);
        }

        private ResourceIterator<TypeVertex> iterateAndFilterRegex(GraphManager graphMgr,
                                                                   ResourceIterator<TypeVertex> iterator) {
            if (iterator == null) iterator = graphMgr.schema().attributeTypes(STRING);
            return filterRegex(iterator);
        }

        private ResourceIterator<TypeVertex> filterRegex(ResourceIterator<TypeVertex> iterator) {
            return iterator.filter(at -> at.regex() != null && at.regex().pattern().equals(props().regex().get()));
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public ProcedureVertex.Type asType() { return this; }
    }
}
