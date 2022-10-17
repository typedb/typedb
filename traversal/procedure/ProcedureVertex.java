/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.AttributeVertex;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.graph.vertex.impl.ThingVertexImpl;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.predicate.PredicateOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.UNSUPPORTED_OPERATION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.merge;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.convertToBoolean;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.convertToDateTime;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.convertToDouble;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.convertToLong;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.convertToString;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.ROLE_TYPE;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.GTE;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.LTE;

public abstract class ProcedureVertex<
        VERTEX extends Vertex<?, ?>,
        PROPERTIES extends TraversalVertex.Properties
        > extends TraversalVertex<ProcedureEdge<?, ?>, PROPERTIES> {

    private int order;
    private Set<ProcedureVertex<?, ?>> dependees;

    ProcedureVertex(Identifier identifier) {
        super(identifier);
    }

    public <ORDER extends Order> Forwardable<? extends VERTEX, ORDER> iterator(
            GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
    ) {
        return iterator(graphMgr, parameters, order, false);
    }

    public abstract <ORDER extends Order> Forwardable<? extends VERTEX, ORDER> iterator(
            GraphManager graphMgr, Traversal.Parameters parameters, ORDER order, boolean forceValueSort
    );

    public boolean isStartVertex() {
        return ins().isEmpty();
    }

    public Thing asThing() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Thing.class));
    }

    public ProcedureVertex.Type asType() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(ProcedureVertex.Type.class));
    }

    public int order() {
        return order;
    }

    void setOrder(int order) {
        this.order = order;
    }

    public Set<ProcedureVertex<?, ?>> dependees() {
        if (dependees == null) {
            Set<ProcedureVertex<?, ?>> vertices = new HashSet<>();
            ins().forEach(e -> vertices.add(e.from()));
            dependees = vertices;
        }
        return dependees;
    }

    @Override
    public String toString() {
        String str = order() + ": " + super.toString();
        if (isStartVertex()) str += " (start)";
        if (outs().isEmpty()) str += " (end)";
        return str;
    }

    public static class Thing extends ProcedureVertex<ThingVertex, Properties.Thing> {

        // TODO: add BDD test to that fails if we try to exclude LT and GT
        private static final Set<PredicateOperator> optimisablePredicates = set(LTE, GTE);

        private Boolean isScope;

        Thing(Identifier identifier) {
            super(identifier);
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

        public boolean isScope() {
            if (isScope == null) {
                isScope = iterate(outs()).anyMatch(e -> e.direction().isForward() && (e.isRolePlayer() || e.isRelating())) ||
                        iterate(ins()).anyMatch(e -> e.direction().isBackward() && (e.isRolePlayer() || e.isRelating()));
            }
            return isScope;
        }

        public boolean overlaps(Thing other, Traversal.Parameters params) {
            if (props().hasIID() && other.props().hasIID()) {
                return params.getIID(id().asVariable()).equals(params.getIID(other.id().asVariable()));
            } else {
                return !intersection(props().types(), other.props().types()).isEmpty();
            }
        }

        @Override
        public <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterator(
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order, boolean forceValueSort
        ) {
            if (props().hasIID()) return iterateAndFilterFromIID(graphMgr, parameters, order);
            else return iterateAndFilterFromTypes(graphMgr, parameters, order, forceValueSort);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromIID(
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
        ) {
            assert props().hasIID() && id().isVariable() && !props().types().isEmpty();
            Identifier.Variable id = id().asVariable();
            ThingVertex vertex = graphMgr.data().getReadable(parameters.getIID(id));
            if (vertex == null) return emptySorted(order);
            return iterateAndFilter(vertex, parameters, order);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromTypes(
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
        ) {
            return iterateAndFilterFromTypes(graphMgr, parameters, order, false);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromTypes(
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order, boolean forceValueSort
        ) {
            return iterateAndFilterFromTypes(graphMgr, parameters, iterate(props().types()).map(graphMgr.schema()::getType), order, forceValueSort);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromTypes(
                GraphManager graphMgr, Traversal.Parameters parameters, FunctionalIterator<TypeVertex> types,
                ORDER order
        ) {
            return iterateAndFilterFromTypes(graphMgr, parameters, types, order, false);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromTypes(
                GraphManager graphMgr, Traversal.Parameters parameters, FunctionalIterator<TypeVertex> types,
                ORDER order, boolean forceValueSort
        ) {
            assert types.hasNext();
            Optional<Predicate.Value<?, ?>> eq = iterate(props().predicates()).filter(p -> p.operator().equals(EQ)).first();
            if (eq.isPresent()) {
                List<AttributeVertex<?>> attributes = attributesEqual(graphMgr, parameters, eq.get());
                return iterateAndFilterPredicates(attributes, parameters, order, forceValueSort);
            } else {
                if (id().isVariable()) types = types.filter(t -> !t.encoding().equals(ROLE_TYPE));
                List<Pair<TypeVertex, Forwardable<ThingVertex, ORDER>>> itersByType = types.map(t ->
                        new Pair<>(t, graphMgr.data().getReadable(t, order))
                ).toList();
                return mergeAndFilterPredicatesOnVertices(
                        graphMgr, itersByType, parameters, order, forceValueSort
                );
            }
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilter(
                ThingVertex vertex, Traversal.Parameters params, ORDER order
        ) {
            if (!checkTypes(vertex) || props().hasIID() && !checkIID(vertex, params) ||
                    !props().predicates().isEmpty() && !checkPredicates(vertex, params, set())) {
                return emptySorted(order);
            } else {
                return iterateSorted(order, vertex);
            }
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterPredicates(
                ThingVertex vertex, Traversal.Parameters params, ORDER order
        ) {
            if (!checkPredicates(vertex, params, set())) return emptySorted(order);
            else return iterateSorted(order, vertex);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterPredicates(
                List<? extends ThingVertex> vertices, Traversal.Parameters params, ORDER order
        ) {
            return iterateAndFilterPredicates(vertices, params, order, false);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterPredicates(
                List<? extends ThingVertex> vertices, Traversal.Parameters params, ORDER order, boolean forceValueSort
        ) {
            if (props().predicates().isEmpty() && !forceValueSort) return iterateSorted(vertices, order);
            else {
                if (iterate(vertices).anyMatch(v -> v.isAttribute() && v.asAttribute().valueType().equals(STRING))) {
                    return iterateSorted(filterPredicatesAndMapVertices(vertices, params, ThingVertex::asAttribute), order);
                } else {
                    return iterateSorted(filterPredicatesAndMapVertices(vertices, params, v -> v.asAttribute().toValue()), order);
                }
            }
        }

        private TreeSet<ThingVertex> filterPredicatesAndMapVertices(
                List<? extends ThingVertex> vertices, Traversal.Parameters params,
                Function<ThingVertex, ? extends AttributeVertex<?>> mapper
        ) {
            TreeSet<ThingVertex> filtered = new TreeSet<>();
            vertices.forEach(v -> {
                if (checkPredicates(v, params, set())) filtered.add(mapper.apply(v));
            });
            return filtered;
        }

        <ORDER extends Order> Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER> iterateAndFilterPredicatesOnEdges(
                List<KeyValue<ThingVertex, ThingVertex>> edges, Traversal.Parameters params, ORDER order
        ) {
            if (props().predicates().isEmpty()) return iterateSorted(edges, order);
            else {
                if (iterate(edges).anyMatch(kv -> kv.key().isAttribute() && kv.key().asAttribute().type().valueType().equals(STRING))) {
                    return iterateSorted(filterPredicatesAndMapEdges(edges, params, ThingVertex::asAttribute), order);
                } else {
                    return iterateSorted(filterPredicatesAndMapEdges(edges, params, v -> v.asAttribute().toValue()), order);
                }
            }
        }

        private TreeSet<KeyValue<ThingVertex, ThingVertex>> filterPredicatesAndMapEdges(
                List<KeyValue<ThingVertex, ThingVertex>> edges,
                Traversal.Parameters params, Function<ThingVertex, AttributeVertex<?>> mapper
        ) {
            TreeSet<KeyValue<ThingVertex, ThingVertex>> filtered = new TreeSet<>();
            edges.forEach(kv -> {
                if (checkPredicates(kv.key(), params, set())) {
                    filtered.add(new KeyValue<>(mapper.apply(kv.key()), kv.value()));
                }
            });
            return filtered;
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> mergeAndFilterPredicatesOnVertices(
                GraphManager graphMgr, List<Pair<TypeVertex, Forwardable<ThingVertex, ORDER>>> vertexIters,
                Traversal.Parameters params, ORDER order
        ) {
            return mergeAndFilterPredicatesOnVertices(graphMgr, vertexIters, params, order, false);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> mergeAndFilterPredicatesOnVertices(
                GraphManager graphMgr, List<Pair<TypeVertex, Forwardable<ThingVertex, ORDER>>> vertexIters,
                Traversal.Parameters params, ORDER order, boolean forceValueSort
        ) {
            if (props().predicates().isEmpty() && !forceValueSort) {
                return merge(iterate(vertexIters).map(Pair::second), order);
            } else {
                if (iterate(vertexIters).anyMatch(pair -> pair.first().isAttributeType() && pair.first().asType().valueType().equals(STRING))) {
                    // TODO: once strings are sortable by value, we can optimise the predicates
                    return merge(
                            iterate(vertexIters).map(pair -> pair.second().filter(a -> checkPredicates(a, params, set()))),
                            order
                    );
                } else {
                    return merge(iterate(vertexIters)
                            .map(pair -> applyPredicatesOnVertices(graphMgr, params, pair.first(), pair.second())
                                    .mapSorted(
                                            a -> a.asAttribute().toValue(),
                                            v -> {
                                                assert v.isValue() && pair.first().valueType().comparables().contains(v.valueType());
                                                return attributeVertexTarget(graphMgr, pair.first(), v, order.isAscending());
                                            },
                                            order
                                    )
                            ), order);
                }
            }
        }

        <ORDER extends Order> Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER> mergeAndFilterPredicatesOnEdges(
                GraphManager graphMgr, List<Pair<TypeVertex, Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER>>> edgeIters,
                Traversal.Parameters params, ORDER order
        ) {
            if (props().predicates().isEmpty()) return merge(iterate(edgeIters).map(Pair::second), order);
            else {
                if (iterate(edgeIters).anyMatch(pair -> pair.first().isAttributeType() && pair.first().asType().valueType().equals(STRING))) {
                    // TODO: once strings are sortable by value, we can optimise the predicates
                    return merge(
                            iterate(edgeIters).map(pair -> pair.second().filter(a -> checkPredicates(a.key(), params, set()))),
                            order
                    );
                } else {
                    return merge(iterate(edgeIters)
                                    .map(pair -> applyPredicatesOnEdges(graphMgr, params, pair.first(), pair.second())
                                            .mapSorted(
                                                    a -> new KeyValue<>(a.key().asAttribute().toValue(), a.value()),
                                                    v -> {
                                                        AttributeVertex.Value<?> value = v.key().asAttribute().toValue();
                                                        assert pair.first().valueType().comparables().contains(value.valueType());
                                                        ThingVertex target = attributeVertexTarget(graphMgr, pair.first(), value, order.isAscending());
                                                        return new KeyValue<>(target, v.value());
                                                    },
                                                    order)
                                    ),
                            order);
                }
            }
        }

        private <ORDER extends Order> Forwardable<ThingVertex, ORDER> applyPredicatesOnVertices(
                GraphManager graphMgr, Traversal.Parameters params, TypeVertex type,
                Forwardable<ThingVertex, ORDER> vertexIterator
        ) {
            assert type.isAttributeType() && type.asType().valueType().isSorted() && id().isVariable();

            if (vertexIterator.order().isAscending()) {
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> largest = params.largestGTValue(id().asVariable());
                if (largest.isPresent()) {
                    ThingVertex target = attributeVertexTarget(graphMgr, type, largest.get().second(), true);
                    vertexIterator.forward(target);
                }
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> smallest = params.smallestLTValue(id().asVariable());
                if (smallest.isPresent()) {
                    vertexIterator = vertexIterator.stopWhen(v -> !smallest.get().first().apply(v.asAttribute(), smallest.get().second()));
                }
            } else {
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> smallest = params.smallestLTValue(id().asVariable());
                if (smallest.isPresent()) {
                    ThingVertex target = attributeVertexTarget(graphMgr, type, smallest.get().second(), false);
                    vertexIterator.forward(target);
                }
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> largest = params.largestGTValue(id().asVariable());
                if (largest.isPresent()) {
                    vertexIterator = vertexIterator.stopWhen(v -> !largest.get().first().apply(v.asAttribute(), largest.get().second()));
                }
            }
            return vertexIterator.filter(a -> checkPredicates(a, params, optimisablePredicates));
        }

        private <ORDER extends Order> Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER> applyPredicatesOnEdges(
                GraphManager graphMgr, Traversal.Parameters params, TypeVertex toType,
                Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER> edgeIterator
        ) {
            assert toType.isAttributeType() && toType.asType().valueType().isSorted() && id().isVariable();

            if (edgeIterator.order().isAscending()) {
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> largest = params.largestGTValue(id().asVariable());
                if (largest.isPresent()) {
                    ThingVertex target = attributeVertexTarget(graphMgr, toType, largest.get().second(), true);
                    edgeIterator.forward(KeyValue.of(target, null));
                }
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> smallest = params.smallestLTValue(id().asVariable());
                if (smallest.isPresent()) {
                    edgeIterator = edgeIterator.stopWhen(kv -> smallest.get().first().apply(kv.key().asAttribute(), smallest.get().second()));
                }
            } else {
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> smallest = params.smallestLTValue(id().asVariable());
                if (smallest.isPresent()) {
                    ThingVertex target = attributeVertexTarget(graphMgr, toType, smallest.get().second(), false);
                    edgeIterator.forward(KeyValue.of(target, null));
                }
                Optional<Pair<Predicate.Value<?, ?>, Traversal.Parameters.Value<?>>> largest = params.largestGTValue(id().asVariable());
                if (largest.isPresent()) {
                    edgeIterator = edgeIterator.stopWhen(v -> largest.get().first().apply(v.key().asAttribute(), largest.get().second()));
                }
            }
            return edgeIterator.filter(kv -> checkPredicates(kv.key(), params, optimisablePredicates));
        }

        private boolean checkPredicates(
                ThingVertex vertex, Traversal.Parameters params, Set<PredicateOperator> excludeOperators
        ) {
            assert id().isVariable();
            for (Predicate.Value<?, ?> predicate : props().predicates()) {
                if (!excludeOperators.contains(predicate.operator())) {
                    for (Traversal.Parameters.Value<?> value : params.getValues(id().asVariable(), predicate)) {
                        if (!predicate.apply(vertex.asAttribute(), value)) return false;
                    }
                }
            }
            return true;
        }

        private boolean checkIID(ThingVertex vertex, Traversal.Parameters parameters) {
            assert parameters.getIID(id().asVariable()) != null;
            return vertex.iid().equals(parameters.getIID(id().asVariable()));
        }

        private boolean checkTypes(ThingVertex vertex) {
            return props().types().contains(vertex.type().properLabel());
        }

        List<AttributeVertex<?>> attributesEqual(
                GraphManager graphMgr, Traversal.Parameters params, Predicate.Value<?, ?> eq
        ) {
            FunctionalIterator<TypeVertex> attributeTypes = iterate(props().types().iterator())
                    .map(l -> graphMgr.schema().getType(l))
                    .filter(t -> eq.valueType().assignables().contains(t.valueType()));
            return attributesEqual(graphMgr, attributeTypes, params, eq);
        }

        List<AttributeVertex<?>> attributesEqual(
                GraphManager graphMgr, FunctionalIterator<TypeVertex> attributeTypes,
                Traversal.Parameters parameters, Predicate.Value<?, ?> eqPredicate
        ) {
            assert id().isVariable();
            Set<Traversal.Parameters.Value<?>> values = parameters.getValues(id().asVariable(), eqPredicate);
            if (values.size() > 1) return list();
            Traversal.Parameters.Value<?> value = values.iterator().next();
            List<AttributeVertex<?>> attributes = new ArrayList<>();
            attributeTypes.map(t -> attributeVertex(graphMgr, t, value))
                    .filter(Objects::nonNull).forEachRemaining(attributes::add);
            return attributes;
        }

        private <T> AttributeVertex<?> attributeVertex(
                GraphManager graphMgr, TypeVertex type, Traversal.Parameters.Value<T> value
        ) {
            assert type.isAttributeType();
            Encoding.ValueType<?> valueType = type.valueType();
            if (valueType == BOOLEAN) {
                return graphMgr.data().getReadable(type, convertToBoolean(value.valueType(), value.value()));
            } else if (valueType == LONG) {
                return graphMgr.data().getReadable(type, convertToLong(value.valueType(), value.value(), true));
            } else if (valueType == DOUBLE) {
                return graphMgr.data().getReadable(type, convertToDouble(value.valueType(), value.value()));
            } else if (valueType == STRING) {
                return graphMgr.data().getReadable(type, convertToString(value.valueType(), value.value()));
            } else if (valueType == DATETIME) {
                return graphMgr.data().getReadable(type, convertToDateTime(value.valueType(), value.value()));
            }
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        private <VALUE> ThingVertex attributeVertexTarget(
                GraphManager graphMgr, TypeVertex type, AttributeVertex<VALUE> target, boolean roundDown
        ) {
            return attributeVertexTarget(graphMgr, type, target.valueType(), target.value(), roundDown);
        }

        private <VALUE> ThingVertex attributeVertexTarget(
                GraphManager graphMgr, TypeVertex type, Traversal.Parameters.Value<VALUE> param, boolean roundDown
        ) {
            return attributeVertexTarget(graphMgr, type, param.valueType(), param.value(), roundDown);
        }

        private <VALUE> ThingVertex attributeVertexTarget(
                GraphManager graphMgr, TypeVertex type, Encoding.ValueType<VALUE> sourceEncoding, VALUE sourceValue, boolean roundDown
        ) {
            if (type.valueType().equals(BOOLEAN)) {
                return ThingVertexImpl.Target.of(
                        graphMgr.data(), new VertexIID.Thing.Attribute.Boolean(type.iid(), convertToBoolean(sourceEncoding, sourceValue))
                );
            } else if (type.valueType().equals(DATETIME)) {
                return ThingVertexImpl.Target.of(
                        graphMgr.data(), new VertexIID.Thing.Attribute.DateTime(type.iid(), convertToDateTime(sourceEncoding, sourceValue))
                );
            } else if (type.valueType().equals(LONG)) {
                return ThingVertexImpl.Target.of(
                        graphMgr.data(), new VertexIID.Thing.Attribute.Long(type.iid(), convertToLong(sourceEncoding, sourceValue, roundDown))
                );
            } else if (type.valueType().equals(DOUBLE)) {
                return ThingVertexImpl.Target.of(
                        graphMgr.data(), new VertexIID.Thing.Attribute.Double(type.iid(), convertToDouble(sourceEncoding, sourceValue))
                );
            } else if (type.valueType().equals(STRING)) throw TypeDBException.of(UNSUPPORTED_OPERATION);
            else throw TypeDBException.of(ILLEGAL_STATE);
        }

    }

    public static class Type extends ProcedureVertex<TypeVertex, Properties.Type> {

        Type(Identifier identifier) {
            super(identifier);
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public <ORDER extends Order> Forwardable<? extends TypeVertex, ORDER> iterator(
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order, boolean forceValueSort
        ) {
            assert id().isVariable();
            Forwardable<TypeVertex, ORDER> iterator = null;

            if (!props().labels().isEmpty()) iterator = iterateLabels(graphMgr, order);
            if (!props().valueTypes().isEmpty()) iterator = iterateOrFilterValueTypes(graphMgr, iterator, order);
            if (props().isAbstract()) iterator = iterateOrFilterAbstract(graphMgr, iterator, order);
            if (props().regex().isPresent()) iterator = iterateAndFilterRegex(graphMgr, iterator, order);
            if (iterator == null) {
                if (mustBeAttributeType()) return graphMgr.schema().attributeTypes(order);
                else if (mustBeRelationType()) return graphMgr.schema().relationTypes(order);
                else if (mustBeRoleType()) return graphMgr.schema().roleTypes(order);
                else if (mustBeThingType()) return graphMgr.schema().thingTypes(order);
                else iterator = graphMgr.schema().thingTypes(order).merge(graphMgr.schema().roleTypes(order));
            }
            return iterator;
        }

        Forwardable<TypeVertex, Order.Asc> filter(Forwardable<TypeVertex, Order.Asc> iterator) {
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

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> iterateLabels(GraphManager graphMgr, ORDER order) {
            return iterate(props().labels()).mergeMapForwardable(l -> iterateSorted(order, graphMgr.schema().getType(l)), order);
        }

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> filterLabels(Forwardable<TypeVertex, ORDER> iterator) {
            assert !props().labels().isEmpty();
            return iterator.filter(t -> props().labels().contains(t.properLabel()));
        }

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> iterateOrFilterValueTypes(
                GraphManager graphMgr, Forwardable<TypeVertex, ORDER> iterator, ORDER order
        ) {
            assert !props().valueTypes().isEmpty();
            if (iterator == null) {
                List<Forwardable<TypeVertex, ORDER>> iterators = new ArrayList<>();
                for (Encoding.ValueType<?> valueType : props().valueTypes()) {
                    iterators.add(graphMgr.schema().attributeTypes(valueType, order));
                }
                return merge(iterate(iterators), order);
            } else return filterValueTypes(iterator);
        }

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> filterValueTypes(Forwardable<TypeVertex, ORDER> iterator) {
            assert !props().valueTypes().isEmpty();
            return iterator.filter(t -> props().valueTypes().contains(t.valueType()));
        }

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> iterateOrFilterAbstract(
                GraphManager graphMgr, Forwardable<TypeVertex, ORDER> iterator, ORDER order
        ) {
            if (iterator == null) return graphMgr.schema().thingTypes(order).filter(TypeVertex::isAbstract);
            else return filterAbstract(iterator);
        }

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> filterAbstract(Forwardable<TypeVertex, ORDER> iterator) {
            return iterator.filter(TypeVertex::isAbstract);
        }

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> iterateAndFilterRegex(
                GraphManager graphMgr, Forwardable<TypeVertex, ORDER> iterator, ORDER order
        ) {
            if (iterator == null) iterator = graphMgr.schema().attributeTypes(STRING, order);
            return filterRegex(iterator);
        }

        private <ORDER extends Order> Forwardable<TypeVertex, ORDER> filterRegex(Forwardable<TypeVertex, ORDER> iterator) {
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
