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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterators;
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

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.emptySorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterators.Forwardable.iterateSorted;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Type.ROLE_TYPE;
import static com.vaticle.typedb.core.traversal.predicate.PredicateOperator.Equality.EQ;

public abstract class ProcedureVertex<
        VERTEX extends Vertex<?, ?>,
        PROPERTIES extends TraversalVertex.Properties
        > extends TraversalVertex<ProcedureEdge<?, ?>, PROPERTIES> {

    private final boolean isStartingVertex;
    private ProcedureEdge<?, ?> lastInEdge;
    private int order;

    ProcedureVertex(Identifier identifier, boolean isStartingVertex) {
        super(identifier);
        this.isStartingVertex = isStartingVertex;
    }

    public abstract Forwardable<? extends VERTEX, Order.Asc> iterator(GraphManager graphMgr, Traversal.Parameters parameters);

    @Override
    public void in(ProcedureEdge<?, ?> edge) {
        super.in(edge);
        if (lastInEdge == null || edge.order() > lastInEdge.order()) lastInEdge = edge;
    }

    public boolean isStartingVertex() {
        return isStartingVertex;
    }

    public ProcedureEdge<?, ?> lastInEdge() {
        if (ins().isEmpty() || isStartingVertex()) return null;
        else return lastInEdge;
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

    public boolean isScope() {
        return false;
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
        public Forwardable<? extends ThingVertex, Order.Asc> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex();
            if (props().hasIID()) return iterateAndFilterFromIID(graphMgr, parameters);
            else return iterateAndFilterFromTypes(graphMgr, parameters);
        }

        Forwardable<? extends ThingVertex, Order.Asc> filter(Forwardable<? extends ThingVertex, Order.Asc> iterator,
                                                             Traversal.Parameters params) {
            iterator = filterTypes(iterator);
            if (props().hasIID()) iterator = filterIID(iterator, params);
            if (!props().predicates().isEmpty()) iterator = filterPredicates(filterAttributes(iterator), params);
            return iterator;
        }

        Forwardable<? extends ThingVertex, Order.Asc> iterateAndFilterFromIID(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert props().hasIID() && id().isVariable() && !props().types().isEmpty();
            Identifier.Variable id = id().asVariable();
            ThingVertex vertex = graphMgr.data().getReadable(parameters.getIID(id));
            if (vertex == null) return emptySorted();
            Forwardable<? extends ThingVertex, Order.Asc> iter = filterTypes(iterateSorted(ASC, vertex));
            if (!props().predicates().isEmpty()) iter = filterPredicates(filterAttributes(iter), parameters);
            return iter;
        }

        Forwardable<? extends ThingVertex, Order.Asc> iterateAndFilterFromTypes(GraphManager graphMgr,
                                                                                Traversal.Parameters parameters) {
            assert !props().types().isEmpty();
            return iterateAndFilterFromTypes(graphMgr, parameters, iterate(props().types()).map(graphMgr.schema()::getType));
        }

        Forwardable<? extends ThingVertex, Order.Asc> iterateAndFilterFromTypes(GraphManager graphMgr,
                                                                                Traversal.Parameters parameters,
                                                                                FunctionalIterator<TypeVertex> types) {
            assert types.hasNext();
            Forwardable<? extends ThingVertex, Order.Asc> iter;
            Optional<Predicate.Value<?>> eq = iterate(props().predicates()).filter(p -> p.operator().equals(EQ)).first();
            if (eq.isPresent()) iter = iteratorOfAttributesWithTypes(graphMgr, parameters, eq.get());
            else {
                if (id().isVariable()) types = types.filter(t -> !t.encoding().equals(ROLE_TYPE));
                iter = types.mergeMap(t -> graphMgr.data().getReadable(t), ASC);
            }

            if (props().predicates().isEmpty()) return iter;
            else return filterPredicates(mapToAttributes(iter), parameters, eq.orElse(null));
        }

        private Forwardable<? extends ThingVertex, Order.Asc> filterIID(Forwardable<? extends ThingVertex, Order.Asc> iterator,
                                                                        Traversal.Parameters parameters) {
            assert parameters.getIID(id().asVariable()) != null;
            return iterator.filter(v -> v.iid().equals(parameters.getIID(id().asVariable())));
        }

        private Forwardable<? extends ThingVertex, Order.Asc> filterTypes(Forwardable<? extends ThingVertex, Order.Asc> iterator) {
            return iterator.filter(v -> props().types().contains(v.type().properLabel()));
        }

        Forwardable<? extends AttributeVertex<?>, Order.Asc> filterPredicates(Forwardable<? extends AttributeVertex<?>, Order.Asc> iterator,
                                                                              Traversal.Parameters parameters) {
            return filterPredicates(iterator, parameters, null);
        }

        Forwardable<? extends AttributeVertex<?>, Order.Asc> filterPredicates(Forwardable<? extends AttributeVertex<?>, Order.Asc> iterator,
                                                                              Traversal.Parameters parameters,
                                                                              @Nullable Predicate.Value<?> exclude) {
            // TODO we should be using forward() to optimise filtering for >, <, and =
            assert id().isVariable();
            for (Predicate.Value<?> predicate : props().predicates()) {
                if (Objects.equals(predicate, exclude)) continue;
                for (Traversal.Parameters.Value value : parameters.getValues(id().asVariable(), predicate)) {
                    iterator = iterator.filter(a -> predicate.apply(a.asAttribute(), value));
                }
            }
            return iterator;
        }

        Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> filterPredicatesOnEdge(Forwardable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> iterator,
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

        Forwardable<? extends AttributeVertex<?>, Order.Asc> iteratorOfAttributesWithTypes(GraphManager graphMgr,
                                                                                           Traversal.Parameters params,
                                                                                           Predicate.Value<?> eq) {
            FunctionalIterator<TypeVertex> attributeTypes = iterate(props().types().iterator())
                    .map(l -> graphMgr.schema().getType(l))
                    .map(t -> {
                        assert t.isAttributeType();
                        return t;
                    }).filter(t -> eq.valueType().assignables().contains(t.valueType()));
            return iteratorOfAttributes(graphMgr, attributeTypes, params, eq);
        }

        Forwardable<? extends AttributeVertex<?>, Order.Asc> iteratorOfAttributes(
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
            return iterateSorted(attributes, ASC);
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

        @Override
        public boolean isScope() {
            // TODO: cache
            return iterate(ins()).anyMatch(edge -> edge.isRolePlayer() || edge.from().id().isScoped()) ||
                    iterate(outs()).anyMatch(edge -> edge.isRolePlayer() || edge.to().id().isScoped());
        }

        static Forwardable<AttributeVertex<?>, Order.Asc> filterAttributes(Forwardable<? extends ThingVertex, Order.Asc> iterator) {
            return mapToAttributes(iterator.filter(ThingVertex::isAttribute));
        }

        static Forwardable<AttributeVertex<?>, Order.Asc> mapToAttributes(Forwardable<? extends ThingVertex, Order.Asc> iterator) {
            // TODO: trying to achieve this without casting seems impossible due to the reverse mapping required by mapSorted?
            return ((Forwardable<ThingVertex, Order.Asc>) iterator).mapSorted(ThingVertex::asAttribute, v -> v, ASC);
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
        public Forwardable<? extends TypeVertex, Order.Asc> iterator(GraphManager graphMgr, Traversal.Parameters parameters) {
            assert isStartingVertex() && id().isVariable();
            Forwardable<TypeVertex, Order.Asc> iterator = null;

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

        private Forwardable<TypeVertex, Order.Asc> iterateLabels(GraphManager graphMgr) {
            return iterate(props().labels()).mergeMap(l -> iterateSorted(ASC, graphMgr.schema().getType(l)), ASC);
        }

        private Forwardable<TypeVertex, Order.Asc> filterLabels(Forwardable<TypeVertex, Order.Asc> iterator) {
            assert !props().labels().isEmpty();
            return iterator.filter(t -> props().labels().contains(t.properLabel()));
        }

        private Forwardable<TypeVertex, Order.Asc> iterateOrFilterValueTypes(GraphManager graphMgr,
                                                                             Forwardable<TypeVertex, Order.Asc> iterator) {
            assert !props().valueTypes().isEmpty();
            if (iterator == null) {
                List<Forwardable<TypeVertex, Order.Asc>> iterators = new ArrayList<>();
                for (Encoding.ValueType valueType : props().valueTypes()) {
                    iterators.add(graphMgr.schema().attributeTypes(valueType));
                }
                return SortedIterators.Forwardable.merge(iterate(iterators), ASC);
            } else return filterValueTypes(iterator);
        }

        private Forwardable<TypeVertex, Order.Asc> filterValueTypes(Forwardable<TypeVertex, Order.Asc> iterator) {
            assert !props().valueTypes().isEmpty();
            return iterator.filter(t -> props().valueTypes().contains(t.valueType()));
        }

        private Forwardable<TypeVertex, Order.Asc> iterateOrFilterAbstract(GraphManager graphMgr,
                                                                           Forwardable<TypeVertex, Order.Asc> iterator) {
            if (iterator == null) return graphMgr.schema().thingTypes().filter(TypeVertex::isAbstract);
            else return filterAbstract(iterator);
        }

        private Forwardable<TypeVertex, Order.Asc> filterAbstract(Forwardable<TypeVertex, Order.Asc> iterator) {
            return iterator.filter(TypeVertex::isAbstract);
        }

        private Forwardable<TypeVertex, Order.Asc> iterateAndFilterRegex(GraphManager graphMgr,
                                                                         Forwardable<TypeVertex, Order.Asc> iterator) {
            if (iterator == null) iterator = graphMgr.schema().attributeTypes(STRING);
            return filterRegex(iterator);
        }

        private Forwardable<TypeVertex, Order.Asc> filterRegex(Forwardable<TypeVertex, Order.Asc> iterator) {
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
