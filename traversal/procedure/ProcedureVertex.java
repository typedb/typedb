/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.predicate.Predicate;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_ATTRIBUTE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.Seekable.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.tree;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ROLE_TYPE;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;

public abstract class ProcedureVertex<
        VERTEX extends Vertex<?, ?>,
        PROPERTIES extends TraversalVertex.Properties
        > extends TraversalVertex<ProcedureEdge<?, ?>, PROPERTIES> {

    private final boolean isStartingVertex;
    private final AtomicReference<Set<Integer>> dependedEdgeOrders;
    private ProcedureEdge<?, ?> branchEdge;

    ProcedureVertex(Identifier identifier, boolean isStartingVertex) {
        super(identifier);
        this.isStartingVertex = isStartingVertex;
        this.dependedEdgeOrders = new AtomicReference<>(null);
    }

    public abstract Seekable<? extends VERTEX, Order.Asc> iterator(GraphManager graphMgr, Traversal.Parameters parameters);

    @Override
    public void in(ProcedureEdge<?, ?> edge) {
        super.in(edge);
        if (branchEdge == null || edge.order() < branchEdge.order()) branchEdge = edge;
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
        if (ins().isEmpty() || isStartingVertex()) return null;
        else return branchEdge;
    }

    public Thing asThing() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.class));
    }

    public ProcedureVertex.Type asType() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(ProcedureVertex.Type.class));
    }

    static TypeVertex assertTypeNotNull(TypeVertex type, Label label) {
        // TODO: replace this with assertions once query validation is implemented
        // TODO: what happens to the state of transaction if we throw in a traversal/match?
        if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, label);
        else return type;
    }

    @Override
    public String toString() {
        String str = super.toString();
        if (isStartingVertex) str += " (start)";
        if (outs().isEmpty()) str += " (end)";
        return str;
    }

    public static class Thing extends ProcedureVertex<ThingVertex, Properties.Thing> {

        Thing(Identifier identifier, boolean isStartingVertex) {
            super(identifier, isStartingVertex);
        }

        @Override
        protected Properties.Thing newProperties() {
            return new Properties.Thing();
        }

        @Override
        public boolean isThing() {
            return true;
        }

        @Override
        public Thing asThing() {
            return this;
        }

        @Override
        public Seekable<? extends ThingVertex, Order.Asc> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex();
            if (props().hasIID()) return iterateAndFilterFromIID(graphMgr, parameters);
            else return iterateAndFilterFromTypes(graphMgr, parameters);
        }

        Seekable<? extends ThingVertex, Order.Asc> filter(Seekable<? extends ThingVertex, Order.Asc> iterator,
                                                          Traversal.Parameters params) {
            iterator = filterTypes(iterator);
            if (props().hasIID()) iterator = filterIID(iterator, params);
            if (!props().predicates().isEmpty()) iterator = filterPredicates(filterAttributes(iterator), params);
            return iterator;
        }

        Seekable<? extends ThingVertex, Order.Asc> iterateAndFilterFromIID(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert props().hasIID() && id().isVariable();
            Identifier.Variable id = id().asVariable();
            ThingVertex vertex = graphMgr.data().getReadable(parameters.getIID(id));
            if (vertex == null) return emptySorted();
            Seekable<? extends ThingVertex, Order.Asc> iter = filterTypes(iterateSorted(ASC, vertex));
            if (!props().predicates().isEmpty()) iter = filterPredicates(filterAttributes(iter), parameters);
            return iter;
        }

        Seekable<? extends ThingVertex, Order.Asc> iterateAndFilterFromTypes(GraphManager graphMgr,
                                                                             Traversal.Parameters parameters) {
            Seekable<? extends ThingVertex, Order.Asc> iter;
            Optional<Predicate.Value<?>> eq = iterate(props().predicates()).filter(p -> p.operator().equals(EQ)).first();
            if (eq.isPresent()) iter = iteratorOfAttributesWithTypes(graphMgr, parameters, eq.get());
            else {
                FunctionalIterator<TypeVertex> typeIter = iterate(props().types().iterator())
                        .map(l -> assertTypeNotNull(graphMgr.schema().getType(l), l));
                if (id().isVariable()) typeIter = typeIter.filter(t -> !t.encoding().equals(ROLE_TYPE));
                iter = typeIter.mergeMap(ASC, t -> graphMgr.data().getReadable(t));
            }

            if (props().predicates().isEmpty()) return iter;
            else {
                // TODO we shouldn't need to filter attributes since the type iterator should already filter in attribute types only to start with.
                return filterPredicates(filterAttributes(iter), parameters, eq.orElse(null));
            }
        }

        Seekable<? extends ThingVertex, Order.Asc> filterIID(Seekable<? extends ThingVertex, Order.Asc> iterator,
                                                             Traversal.Parameters parameters) {
            // TODO optimise with seek
            return iterator.filter(v -> v.iid().equals(parameters.getIID(id().asVariable())));
        }

        Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> filterIIDOnPlayerAndRole(Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iterator,
                                                                                         Traversal.Parameters parameters) {
            // TODO optimise with seek if we can
            return iterator.filter(kv -> kv.key().iid().equals(parameters.getIID(id().asVariable())));
        }

        Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> filterTypesOnEdge(Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iterator) {
            return iterator.filter(kv -> props().types().contains(kv.key().type().properLabel()));
        }

        Seekable<? extends ThingVertex, Order.Asc> filterTypes(Seekable<? extends ThingVertex, Order.Asc> iterator) {
            return iterator.filter(v -> props().types().contains(v.type().properLabel()));
        }

        Seekable<? extends AttributeVertex<?>, Order.Asc> filterPredicates(Seekable<? extends AttributeVertex<?>, Order.Asc> iterator,
                                                                           Traversal.Parameters parameters) {
            return filterPredicates(iterator, parameters, null);
        }

        Seekable<? extends AttributeVertex<?>, Order.Asc> filterPredicates(Seekable<? extends AttributeVertex<?>, Order.Asc> iterator,
                                                                           Traversal.Parameters parameters,
                                                                           @Nullable Predicate.Value<?> exclude) {
            // TODO: should we throw an exception if the user asserts a value predicate on a non-attribute?
            // TODO: should we throw an exception if the user assert a value non-comparable value types?
            assert id().isVariable();
            for (Predicate.Value<?> predicate : props().predicates()) {
                if (Objects.equals(predicate, exclude)) continue;
                for (Traversal.Parameters.Value value : parameters.getValues(id().asVariable(), predicate)) {
                    iterator = iterator.filter(a -> predicate.apply(a.asAttribute(), value));
                }
            }
            return iterator;
        }

        Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> filterPredicatesOnEdge(Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iterator,
                                                                                       Traversal.Parameters parameters) {
            assert id().isVariable();
            iterator = iterator.filter(kv -> kv.key().isAttribute());
            for (Predicate.Value<?> predicate : props().predicates()) {
                for (Traversal.Parameters.Value value : parameters.getValues(id().asVariable(), predicate)) {
                    iterator = iterator.filter(kv -> predicate.apply(kv.key().asAttribute(), value));
                }
            }
            return iterator;
        }

        Seekable<? extends AttributeVertex<?>, Order.Asc> iteratorOfAttributesWithTypes(GraphManager graphMgr,
                                                                                        Traversal.Parameters params,
                                                                                        Predicate.Value<?> eq) {
            FunctionalIterator<TypeVertex> attributeTypes = iterate(props().types().iterator())
                    .map(l -> graphMgr.schema().getType(l)).noNulls()
                    .map(t -> {
                        if (t.isAttributeType()) return t;
                        else throw TypeDBException.of(TYPE_NOT_ATTRIBUTE_TYPE, t.properLabel());
                    }).filter(t -> eq.valueType().assignables().contains(t.valueType()));
            return iteratorOfAttributes(graphMgr, attributeTypes, params, eq);
        }

        Seekable<? extends AttributeVertex<?>, Order.Asc> iteratorOfAttributes(
                GraphManager graphMgr, FunctionalIterator<TypeVertex> attributeTypes,
                Traversal.Parameters parameters, Predicate.Value<?> eqPredicate
        ) {
            assert id().isVariable();
            Set<Traversal.Parameters.Value> values = parameters.getValues(id().asVariable(), eqPredicate);
            if (values.size() > 1) return emptySorted();
            Traversal.Parameters.Value value = values.iterator().next();
            TreeSet<AttributeVertex<?>> attributes = new TreeSet<>();
            attributeTypes.map(t -> attributeVertex(graphMgr, t, value))
                    .filter(Objects::nonNull).forEachRemaining(attributes::add);
            return iterateSorted(ASC, attributes);
        }

        private AttributeVertex<?> attributeVertex(GraphManager graphMgr, TypeVertex type,
                                                   Traversal.Parameters.Value value) {
            assert type.isAttributeType();
            switch (type.valueType()) {
                case BOOLEAN:
                    return graphMgr.data().getReadable(type, value.getBoolean());
                case LONG:
                    return graphMgr.data().getReadable(type, value.getLong());
                case DOUBLE:
                    return graphMgr.data().getReadable(type, value.getDouble());
                case STRING:
                    return graphMgr.data().getReadable(type, value.getString());
                case DATETIME:
                    return graphMgr.data().getReadable(type, value.getDateTime());
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        static Seekable<AttributeVertex<?>, Order.Asc> filterAttributes(Seekable<? extends ThingVertex, Order.Asc> iterator) {
            // TODO: trying to achieve this without casting seems impossible due to the reverse mapping required by mapSorted?
            return ((Seekable<ThingVertex, Order.Asc>) iterator).filter(ThingVertex::isAttribute)
                    .mapSorted(ASC, ThingVertex::asAttribute, v -> v);
        }
    }

    public static class Type extends ProcedureVertex<TypeVertex, Properties.Type> {

        Type(Identifier identifier, boolean isStartingVertex) {
            super(identifier, isStartingVertex);
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public Seekable<? extends TypeVertex, Order.Asc> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex() && id().isVariable();
            Seekable<TypeVertex, Order.Asc> iterator = null;

            if (!props().labels().isEmpty()) iterator = iterateLabels(graphMgr);
            if (!props().valueTypes().isEmpty()) iterator = iterateOrFilterValueTypes(graphMgr, iterator);
            if (props().isAbstract()) iterator = iterateOrFilterAbstract(graphMgr, iterator);
            if (props().regex().isPresent()) iterator = iterateAndFilterRegex(graphMgr, iterator);
            if (iterator == null) {
                if (mustBeAttributeType()) return graphMgr.schema().attributeTypes();
                else if (mustBeRelationType()) return graphMgr.schema().relationTypes();
                else if (mustBeRoleType()) return graphMgr.schema().roleTypes();
                else if (mustBeThingType()) return graphMgr.schema().thingTypes();
                else iterator = graphMgr.schema().thingTypes().merge(graphMgr.schema().roleTypes());
            }
            return iterator;
        }

        Seekable<TypeVertex, Order.Asc> filter(Seekable<TypeVertex, Order.Asc> iterator) {
            if (!props().labels().isEmpty()) iterator = filterLabels(iterator);
            if (!props().valueTypes().isEmpty()) iterator = filterValueTypes(iterator);
            if (props().isAbstract()) iterator = filterAbstract(iterator);
            if (props().regex().isPresent()) iterator = filterRegex(iterator);
            return iterator;
        }

        private boolean mustBeAttributeType() {
            return iterate(outs()).anyMatch(ProcedureEdge::onlyStartsFromAttributeType);
        }

        private boolean mustBeRelationType() {
            return iterate(outs()).anyMatch(ProcedureEdge::onlyStartsFromRelationType);
        }

        private boolean mustBeRoleType() {
            return iterate(outs()).anyMatch(ProcedureEdge::onlyStartsFromRoleType);
        }

        private boolean mustBeThingType() {
            return iterate(outs()).anyMatch(ProcedureEdge::onlyStartsFromThingType);
        }

        private Seekable<TypeVertex, Order.Asc> iterateLabels(GraphManager graphMgr) {
            return iterate(props().labels()).mergeMap(ASC, l -> iterateSorted(ASC, assertTypeNotNull(graphMgr.schema().getType(l), l)));
        }

        private Seekable<TypeVertex, Order.Asc> filterLabels(Seekable<TypeVertex, Order.Asc> iterator) {
            assert !props().labels().isEmpty();
            return iterator.filter(t -> props().labels().contains(t.properLabel()));
        }

        private Seekable<TypeVertex, Order.Asc> iterateOrFilterValueTypes(GraphManager graphMgr,
                                                                          Seekable<TypeVertex, Order.Asc> iterator) {
            assert !props().valueTypes().isEmpty();
            if (iterator == null) {
                List<Seekable<TypeVertex, Order.Asc>> iterators = new ArrayList<>();
                for (Encoding.ValueType valueType : props().valueTypes()) {
                    iterators.add(graphMgr.schema().attributeTypes(valueType));
                }
                return Iterators.Sorted.Seekable.merge(ASC, iterate(iterators));
            } else return filterValueTypes(iterator);
        }

        private Seekable<TypeVertex, Order.Asc> filterValueTypes(Seekable<TypeVertex, Order.Asc> iterator) {
            assert !props().valueTypes().isEmpty();
            return iterator.filter(t -> props().valueTypes().contains(t.valueType()));
        }

        private Seekable<TypeVertex, Order.Asc> iterateOrFilterAbstract(GraphManager graphMgr,
                                                                        Seekable<TypeVertex, Order.Asc> iterator) {
            if (iterator == null) return graphMgr.schema().thingTypes().filter(TypeVertex::isAbstract);
            else return filterAbstract(iterator);
        }

        private Seekable<TypeVertex, Order.Asc> filterAbstract(Seekable<TypeVertex, Order.Asc> iterator) {
            return iterator.filter(TypeVertex::isAbstract);
        }

        private Seekable<TypeVertex, Order.Asc> iterateAndFilterRegex(GraphManager graphMgr,
                                                                      Seekable<TypeVertex, Order.Asc> iterator) {
            if (iterator == null) iterator = graphMgr.schema().attributeTypes(STRING);
            return filterRegex(iterator);
        }

        private Seekable<TypeVertex, Order.Asc> filterRegex(Seekable<TypeVertex, Order.Asc> iterator) {
            return iterator.filter(at -> at.regex() != null && at.regex().pattern().equals(props().regex().get()));
        }

        @Override
        public boolean isType() {
            return true;
        }

        @Override
        public ProcedureVertex.Type asType() {
            return this;
        }
    }
}
