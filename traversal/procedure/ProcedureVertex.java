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
import com.vaticle.typedb.core.common.exception.TypeDBCheckedException;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterators;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.vaticle.typedb.common.collection.Collections.intersection;
import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.merge;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.BOOLEAN;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DATETIME;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.DOUBLE;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.LONG;
import static com.vaticle.typedb.core.encoding.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.ROLE_TYPE;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;

public abstract class ProcedureVertex<
        VERTEX extends Vertex<?, ?>,
        PROPERTIES extends TraversalVertex.Properties
        > extends TraversalVertex<ProcedureEdge<?, ?>, PROPERTIES> {

    private int order;
    private Set<ProcedureVertex<?, ?>> dependees;

    ProcedureVertex(Identifier identifier) {
        super(identifier);
    }

    public abstract <ORDER extends Order> Forwardable<? extends VERTEX, ORDER> iterator(
            GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
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
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
        ) {
            if (props().hasIID()) return iterateAndFilterFromIID(graphMgr, parameters, order);
            else return iterateAndFilterFromTypes(graphMgr, parameters, order);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilter(
                ThingVertex vertex, Traversal.Parameters params, ORDER order
        ) {
            if (!checkTypes(vertex) || props().hasIID() && !checkIID(vertex, params) ||
                    !props().predicates().isEmpty() && !checkPredicates(vertex, params)) {
                return emptySorted(order);
            } else {
                return iterateSorted(order, vertex);
            }
        }

        private boolean checkPredicates(ThingVertex vertex, Traversal.Parameters params) {
            assert !props().predicates().isEmpty() && id().isVariable();
            if (!vertex.isAttribute()) return false;
            for (Predicate.Value<?, ?> predicate : props().predicates()) {
                for (Traversal.Parameters.Value value : params.getValues(id().asVariable(), predicate)) {
                    if (!predicate.apply(vertex.asAttribute(), value)) return false;
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

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterPredicates(
                ThingVertex vertex, Traversal.Parameters params, ORDER order
        ) {
            if (!checkPredicates(vertex, params)) return emptySorted(order);
            else return iterateSorted(order, vertex);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterPredicates(
                List<? extends ThingVertex> vertices, Traversal.Parameters params, ORDER order
        ) {
            if (props().predicates().isEmpty()) return iterateSorted(vertices, order);
            else {
                TreeSet<AttributeVertex.Value<?>> filtered = new TreeSet<>();
                vertices.forEach(v -> {
                    if (checkPredicates(v, params)) filtered.add(v.asAttribute().toValue());
                });
                return iterateSorted(filtered, order);
            }
        }

        <ORDER extends Order> Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER> iterateAndFilterPredicatesOnEdges(
                List<KeyValue<ThingVertex, ThingVertex>> edges, Traversal.Parameters params, ORDER order
        ) {
            if (props().predicates().isEmpty()) return iterateSorted(edges, order);
            else {
                TreeSet<KeyValue<ThingVertex, ThingVertex>> filtered = new TreeSet<>();
                edges.forEach(kv -> {
                    if (checkPredicates(kv.key(), params)) {
                        filtered.add(new KeyValue<>(kv.key().asAttribute().toValue(), kv.value()));
                    }
                });
                return iterateSorted(filtered, order);
            }
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> mergeAndFilterPredicates(
                GraphManager graphMgr, List<Pair<TypeVertex, Forwardable<ThingVertex, ORDER>>> vertexIters,
                Traversal.Parameters params, ORDER order
        ) {
            if (props().predicates().isEmpty()) return merge(iterate(vertexIters).map(Pair::second), order);
            else {
                FunctionalIterator<Forwardable<AttributeVertex.Value<?>, ORDER>> asValues =
                        iterate(vertexIters)
                                // TODO: what if it contains non-attributes?
                                // TODO: what about optimising with forward()?
                                .map(pair -> {
                                    return pair.second().filter(a1 -> checkPredicates(a1, params))
                                            .mapSorted(a -> a.asAttribute().toValue(), v -> {
                                                assert v.isValue() && pair.first().valueType().comparables().contains(v.valueType());
                                                if (v.isBoolean()) {
                                                    return ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Boolean(pair.first().iid(), v.asBoolean().value()));
                                                } else if (v.isString()) {
                                                    try {
                                                        return ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.String(pair.first().iid(), v.asString().value()));
                                                    } catch (TypeDBCheckedException e) {
                                                        throw TypeDBException.of(e);
                                                    }
                                                } else if (v.isDateTime()) {
                                                    return ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.DateTime(pair.first().iid(), v.asDateTime().value()));
                                                } else if (v.isLong()) {
                                                    if (pair.first().valueType().equals(LONG)) {
                                                        return ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Long(pair.first().iid(), v.asLong().value()));
                                                    } else if (pair.first().valueType().equals(DOUBLE)) {
                                                        return ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Double(pair.first().iid(), v.asLong().value()));
                                                    } else throw TypeDBException.of(ILLEGAL_STATE);
                                                } else if (v.isDouble()) {
                                                    if (pair.first().valueType().equals(LONG)) {
                                                        long rounded = order.isAscending() ? v.asDouble().value().longValue() : (long) (v.asDouble().value() + 1);
                                                        return ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Long(pair.first().iid(), rounded));
                                                    } else if (pair.first().valueType().equals(DOUBLE)) {
                                                        return ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Double(pair.first().iid(), v.asDouble().value()));
                                                    } else throw TypeDBException.of(ILLEGAL_STATE);
                                                } else throw TypeDBException.of(ILLEGAL_STATE);
                                            }, order);
                                });
                return merge(asValues, order);
            }
        }

        <ORDER extends Order> Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER> mergeAndFilterPredicatesOnEdges(
                GraphManager graphMgr, List<Pair<TypeVertex, Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER>>> edgeIters,
                Traversal.Parameters params, ORDER order
        ) {
            if (props().predicates().isEmpty()) return merge(iterate(edgeIters).map(Pair::second), order);
            else {
                // TODO: we can't apply this for strings, since they aren't sorted by value
                FunctionalIterator<Forwardable<KeyValue<ThingVertex, ThingVertex>, ORDER>> asValues =
                        iterate(edgeIters)
                                // TODO: what if it contains non-attributes?
                                // TODO: what about optimising with forward()?
                                .map(pair -> {
                                    return pair.second()
                                            .filter(a1 -> checkPredicates(a1.key(), params))
                                            .mapSorted(
                                                    a -> new KeyValue<>(a.key().asAttribute().toValue(), a.value()),
                                                    v -> {
                                                        AttributeVertex.Value<?> value = v.key().asAttribute().toValue();
                                                        assert value.isValue() && pair.first().valueType().comparables().contains(value.valueType());
                                                        ThingVertex target;
                                                        if (value.isBoolean()) {
                                                            target = ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Boolean(pair.first().iid(), value.asBoolean().value()));
                                                        } else if (value.isString()) {
                                                            try {
                                                                target = ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.String(pair.first().iid(), value.asString().value()));
                                                            } catch (TypeDBCheckedException e) {
                                                                throw TypeDBException.of(e);
                                                            }
                                                        } else if (value.isDateTime()) {
                                                            target = ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.DateTime(pair.first().iid(), value.asDateTime().value()));
                                                        } else if (value.isLong()) {
                                                            if (pair.first().valueType().equals(LONG)) {
                                                                target = ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Long(pair.first().iid(), value.asLong().value()));
                                                            } else if (pair.first().valueType().equals(DOUBLE)) {
                                                                target = ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Double(pair.first().iid(), value.asLong().value()));
                                                            } else throw TypeDBException.of(ILLEGAL_STATE);
                                                        } else if (value.isDouble()) {
                                                            if (pair.first().valueType().equals(LONG)) {
                                                                long rounded = order.isAscending() ? value.asDouble().value().longValue() : (long) (value.asDouble().value() + 1);
                                                                target = ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Long(pair.first().iid(), rounded));
                                                            } else if (pair.first().valueType().equals(DOUBLE)) {
                                                                target = ThingVertexImpl.Target.of(graphMgr.data(), new VertexIID.Thing.Attribute.Double(pair.first().iid(), value.asDouble().value()));
                                                            } else throw TypeDBException.of(ILLEGAL_STATE);
                                                        } else throw TypeDBException.of(ILLEGAL_STATE);
                                                        return new KeyValue<>(target, v.value());
                                                    }, order);
                                });
                return merge(asValues, order);
            }
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromIID(
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
        ) {
            assert props().hasIID() && id().isVariable() && !props().types().isEmpty();
            Identifier.Variable id = id().asVariable();
            ThingVertex vertex = graphMgr.data().getReadable(parameters.getIID(id));
            if (vertex == null) return emptySorted(order);
            return iterateAndFilter(vertex, parameters, order); // TODO: re-applies IID filter
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromTypes(
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
        ) {
            return iterateAndFilterFromTypes(graphMgr, parameters, iterate(props().types()).map(graphMgr.schema()::getType), order);
        }

        <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> iterateAndFilterFromTypes(
                GraphManager graphMgr, Traversal.Parameters parameters, FunctionalIterator<TypeVertex> types, ORDER order
        ) {
            assert types.hasNext();
            Optional<Predicate.Value<?, ?>> eq = iterate(props().predicates()).filter(p -> p.operator().equals(EQ)).first();
            if (eq.isPresent()) {
                return iterateAndFilterPredicates(attributesEqual(graphMgr, parameters, eq.get()), parameters, order);
            } else {
                if (id().isVariable()) types = types.filter(t -> !t.encoding().equals(ROLE_TYPE));
                return mergeAndFilterPredicates(graphMgr, types.map(t -> new Pair<>(t, graphMgr.data().getReadable(t, order))).toList(), parameters, order);
            }
        }

        private <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> filterIID(
                Forwardable<? extends ThingVertex, ORDER> iterator, Traversal.Parameters parameters
        ) {
            assert parameters.getIID(id().asVariable()) != null;
            return iterator.filter(v -> v.iid().equals(parameters.getIID(id().asVariable())));
        }

        private <ORDER extends Order> Forwardable<? extends ThingVertex, ORDER> filterTypes(Forwardable<? extends ThingVertex, ORDER> iterator) {
            return iterator.filter(v -> props().types().contains(v.type().properLabel()));
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
            Set<Traversal.Parameters.Value> values = parameters.getValues(id().asVariable(), eqPredicate);
            if (values.size() > 1) return list();
            Traversal.Parameters.Value value = values.iterator().next();
            List<AttributeVertex<?>> attributes = new ArrayList<>();
            attributeTypes.map(t -> attributeVertex(graphMgr, t, value))
                    .filter(Objects::nonNull).forEachRemaining(attributes::add);
            return attributes;
        }

        private AttributeVertex<?> attributeVertex(GraphManager graphMgr, TypeVertex type, Traversal.Parameters.Value value) {
            assert type.isAttributeType();
            Encoding.ValueType<?> valueType = type.valueType();
            if (valueType == BOOLEAN) return graphMgr.data().getReadable(type, value.getBoolean());
            else if (valueType == LONG) return graphMgr.data().getReadable(type, value.getLong());
            else if (valueType == DOUBLE) return graphMgr.data().getReadable(type, value.getDouble());
            else if (valueType == STRING) return graphMgr.data().getReadable(type, value.getString());
            else if (valueType == DATETIME) return graphMgr.data().getReadable(type, value.getDateTime());
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        static <ORDER extends Order> Forwardable<AttributeVertex<?>, ORDER> filterAttributes(Forwardable<? extends ThingVertex, ORDER> iterator) {
            return mapToAttributes(iterator.filter(ThingVertex::isAttribute));
        }

        static <ORDER extends Order> Forwardable<AttributeVertex<?>, ORDER> mapToAttributes(Forwardable<? extends ThingVertex, ORDER> iterator) {
            // TODO: trying to achieve this without casting seems impossible due to the reverse mapping required by mapSorted?
            return ((Forwardable<ThingVertex, ORDER>) iterator).mapSorted(ThingVertex::asAttribute, v -> v, iterator.order());
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
                GraphManager graphMgr, Traversal.Parameters parameters, ORDER order
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
