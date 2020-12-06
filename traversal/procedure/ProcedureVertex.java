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
import grakn.core.graph.vertex.AttributeVertex;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Predicate;
import grakn.core.traversal.graph.TraversalVertex;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;
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
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(ProcedureVertex.Thing.class)));
    }

    public ProcedureVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(ProcedureVertex.Type.class)));
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
            for (Predicate.Value<?> predicate : props().predicates()) {
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
            Optional<Predicate.Value<?>> eq;
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
                                                                                    Predicate.Value<?> eqPredicate) {
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

        Type(Identifier identifier, boolean isStartingVertex) {
            super(identifier, isStartingVertex);
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public ResourceIterator<TypeVertex> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex() && identifier().isVariable();
            ResourceIterator<TypeVertex> iterator = null;

            if (!props().labels().isEmpty()) iterator = iterateLabels(graphMgr);
            if (props().valueType().isPresent()) iterator = iterateOrFilterValueTypes(graphMgr, iterator);
            if (props().isAbstract()) iterator = iterateOrFilterAbstract(graphMgr, iterator);
            if (props().regex().isPresent()) iterator = iterateAndFilterRegex(graphMgr, iterator);
            return iterator;
        }

        private ResourceIterator<TypeVertex> iterateAndFilterRegex(GraphManager graphMgr,
                                                                   ResourceIterator<TypeVertex> iterator) {
            if (iterator == null) iterator = graphMgr.schema().attributeTypes(STRING);
            return iterator.filter(at -> at.regex() != null && at.regex().pattern().equals(props().regex().get()));
        }

        private ResourceIterator<TypeVertex> iterateLabels(GraphManager graphMgr) {
            return iterate(props().labels().iterator()).map(l -> graphMgr.schema().getType(l)).noNulls();
        }

        private ResourceIterator<TypeVertex> iterateOrFilterValueTypes(GraphManager graphMgr,
                                                                       ResourceIterator<TypeVertex> iterator) {
            assert props().valueType().isPresent();
            if (iterator == null) return graphMgr.schema().attributeTypes(props().valueType().get());
            else return iterator.filter(t -> Objects.equals(t.valueType(), props().valueType().get()));
        }

        private ResourceIterator<TypeVertex> iterateOrFilterAbstract(GraphManager graphMgr,
                                                                     ResourceIterator<TypeVertex> iterator) {
            if (iterator == null) return graphMgr.schema().thingTypes().filter(TypeVertex::isAbstract);
            else return iterator.filter(TypeVertex::isAbstract);
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public ProcedureVertex.Type asType() { return this; }
    }
}
