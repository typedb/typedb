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

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concurrent.producer.FunctionalProducer;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.graph.vertex.Vertex;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.planner.Planner;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.predicate.PredicateArgument;
import com.vaticle.typedb.core.traversal.procedure.CombinationProcedure;
import com.vaticle.typedb.core.traversal.scanner.CombinationFinder;
import com.vaticle.typedb.core.traversal.structure.Structure;
import com.vaticle.typeql.lang.common.TypeQLArg;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.core.common.iterator.Iterators.cartesian;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.parameters.Arguments.Query.Producer.INCREMENTAL;
import static com.vaticle.typedb.core.concurrent.executor.Executors.async2;
import static com.vaticle.typedb.core.concurrent.producer.Producers.async;
import static com.vaticle.typedb.core.concurrent.producer.Producers.produce;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.ISA;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.HAS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.PLAYING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Base.RELATING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.OWNS_KEY;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.RELATES;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Type.SUB;
import static com.vaticle.typeql.lang.common.TypeQLToken.Predicate.SubString.LIKE;
import static java.util.stream.Collectors.toList;

public abstract class GraphTraversal extends Traversal {

    final Set<Identifier.Variable.Retrievable> filter;

    public GraphTraversal() {
        super();
        filter = new HashSet<>();
    }

    abstract Set<Identifier.Variable.Retrievable> filter();

    public abstract void filter(Set<? extends Identifier.Variable.Retrievable> filter);

    FunctionalIterator<VertexMap> permutation(GraphManager graphMgr, Collection<Planner> planners, boolean singleUse,
                                              Set<Identifier.Variable.Retrievable> filter) {
        assert !planners.isEmpty();
        if (planners.size() == 1) {
            planners.iterator().next().tryOptimise(graphMgr, singleUse);
            return planners.iterator().next().procedure().iterator(graphMgr, parameters, filter);
        } else {
            return cartesian(planners.parallelStream().map(planner -> {
                planner.tryOptimise(graphMgr, singleUse);
                return planner.procedure().iterator(graphMgr, parameters, filter);
            }).collect(toList())).map(partialAnswers -> {
                Map<Identifier.Variable.Retrievable, Vertex<?, ?>> combinedAnswers = new HashMap<>();
                partialAnswers.forEach(p -> combinedAnswers.putAll(p.map()));
                return VertexMap.of(combinedAnswers);
            });
        }
    }

    FunctionalIterator<Structure> structures() {
        return iterate(structure.asGraphs()).filter(p -> iterate(p.vertices()).anyMatch(
                v -> v.id().isRetrievable() && filter().contains(v.id().asVariable().asRetrievable())
        ));
    }

    public void labels(Identifier.Variable type, Set<Label> labels) {
        structure.typeVertex(type).props().labels(labels);
    }

    public void labels(Identifier.Variable type, Label label) {
        structure.typeVertex(type).props().labels(label);
    }

    public void equalTypes(Identifier.Variable type1, Identifier.Variable type2) {
        structure.equalEdge(structure.typeVertex(type1), structure.typeVertex(type2));
    }

    public void owns(Identifier.Variable thingType, Identifier.Variable attributeType, boolean isKey) {
        // TODO: Something smells here. We should really just have one encoding for OWNS, and a flag for @key
        structure.nativeEdge(structure.typeVertex(thingType), structure.typeVertex(attributeType), isKey ? OWNS_KEY : OWNS);
    }

    public void plays(Identifier.Variable thingType, Identifier.Variable roleType) {
        structure.nativeEdge(structure.typeVertex(thingType), structure.typeVertex(roleType), PLAYS);
    }

    public void relates(Identifier.Variable relationType, Identifier.Variable roleType) {
        structure.nativeEdge(structure.typeVertex(relationType), structure.typeVertex(roleType), RELATES);
    }

    public void sub(Identifier.Variable subtype, Identifier.Variable supertype, boolean isTransitive) {
        structure.nativeEdge(structure.typeVertex(subtype), structure.typeVertex(supertype), SUB, isTransitive);
    }

    public void isAbstract(Identifier.Variable type) {
        structure.typeVertex(type).props().setAbstract();
    }

    public void regex(Identifier.Variable type, String regex) {
        structure.typeVertex(type).props().regex(regex);
    }

    public void valueType(Identifier.Variable attributeType, TypeQLArg.ValueType valueType) {
        structure.typeVertex(attributeType).props().valueType(Encoding.ValueType.of(valueType));
    }

    public static class Type extends GraphTraversal {

        public Type() {
            super();
        }

        @Override
        FunctionalIterator<VertexMap> permutationIter(GraphManager graphMgr) {
            return permutation(graphMgr, structures().map(Planner::create).toList(), true, filter());
        }

        public Optional<Map<Identifier.Variable.Retrievable, Set<TypeVertex>>> combination(
                GraphManager graphMgr, Set<Identifier.Variable.Retrievable> concreteVarIds) {
            Set<CombinationProcedure> combinationProcedures = new HashSet<>();
            structures().forEachRemaining(structure -> combinationProcedures.add(CombinationProcedure.create(structure)));
            return new CombinationFinder(graphMgr, combinationProcedures, filter(), concreteVarIds).combination();
        }

        @Override
        public Set<Identifier.Variable.Retrievable> filter() {
            return filter;
        }

        @Override
        public void filter(Set<? extends Identifier.Variable.Retrievable> filter) {
            assert iterate(filter).noneMatch(Identifier::isLabel);
            this.filter.addAll(filter);
        }
    }

    public static class Thing extends GraphTraversal {

        private Map<Structure, Planner> planners;
        private boolean modifiable;

        public Thing() {
            super();
            modifiable = true;
            planners = new HashMap<>();
        }

        private void initialise(TraversalCache cache) {
            if (planners.isEmpty()) {
                structures().forEachRemaining(structure -> {
                    Planner planner = cache.getPlanner(structure, Planner::create);
                    planners.put(structure, planner);
                });
            }
        }

        FunctionalIterator<VertexMap> permutationIterCaching(GraphManager graphMgr, TraversalCache cache) {
            initialise(cache);
            FunctionalIterator<VertexMap> permutations = permutation(graphMgr, planners.values(), false, filter());
            cache.updatePlanner(planners);
            return permutations;
        }

        @Override
        FunctionalIterator<VertexMap> permutationIter(GraphManager graphMgr) {
            return permutation(graphMgr, structures().map(Planner::create).toList(), false, filter());
        }

        FunctionalProducer<VertexMap> permutationProducerCaching(GraphManager graphMgr, TraversalCache cache,
                                                                 Either<Arguments.Query.Producer, Long> context,
                                                                 int parallelisation) {
            initialise(cache);
            FunctionalProducer<VertexMap> producer;
            if (planners.size() == 1) {
                planners.values().iterator().next().tryOptimise(graphMgr, false);
                producer = planners.values().iterator().next().procedure().producer(graphMgr, parameters, filter(), parallelisation);
            } else {
                Either<Arguments.Query.Producer, Long> nestedCtx = context.isFirst() ? context : Either.first(INCREMENTAL);
                producer = async(cartesian(planners.values().parallelStream().map(planner -> {
                    planner.tryOptimise(graphMgr, false);
                    return planner.procedure().producer(graphMgr, parameters, filter(), parallelisation);
                }).map(p -> produce(p, nestedCtx, async2())).collect(toList())).map(partialAnswers -> {
                    Map<Identifier.Variable.Retrievable, Vertex<?, ?>> combinedAnswers = new HashMap<>();
                    partialAnswers.forEach(p -> combinedAnswers.putAll(p.map()));
                    return VertexMap.of(combinedAnswers);
                }));
            }
            cache.updatePlanner(planners);
            return producer;
        }

        // TODO: We should not dynamically calculate properties like this, and then guard against 'modifiable'.
        //       We should introduce a "builder pattern" to Traversal, such that users of this library will build
        //       traversals with Traversal.Builder, and call .build() in the end to produce a final Object.
        @Override
        Set<Identifier.Variable.Retrievable> filter() {
            if (filter.isEmpty()) {
                modifiable = false;
                iterate(structure.vertices()).filter(v -> v.id().isRetrievable()).map(v -> v.id().asVariable().asRetrievable())
                        .toSet(filter);
            }
            return filter;
        }

        @Override
        public void filter(Set<? extends Identifier.Variable.Retrievable> filter) {
            assert modifiable && iterate(filter).noneMatch(Identifier::isLabel);
            this.filter.addAll(filter);
        }

        public void equalThings(Identifier.Variable thing1, Identifier.Variable thing2) {
            assert modifiable;
            structure.equalEdge(structure.thingVertex(thing1), structure.thingVertex(thing2));
        }

        public void iid(Identifier.Variable thing, ByteArray iid) {
            assert modifiable;
            parameters.putIID(thing, VertexIID.Thing.of(iid));
            structure.thingVertex(thing).props().hasIID(true);
        }

        public void types(Identifier thing, Set<Label> labels) {
            assert modifiable;
            structure.thingVertex(thing).props().types(labels);
        }

        public void has(Identifier.Variable thing, Identifier.Variable attribute) {
            assert modifiable;
            structure.nativeEdge(structure.thingVertex(thing), structure.thingVertex(attribute), HAS);
        }

        public void isa(Identifier thing, Identifier.Variable type) {
            assert modifiable;
            isa(thing, type, true);
        }

        public void isa(Identifier thing, Identifier.Variable type, boolean isTransitive) {
            assert modifiable;
            structure.nativeEdge(structure.thingVertex(thing), structure.typeVertex(type), ISA, isTransitive);
        }

        public void relating(Identifier.Variable relation, Identifier.Scoped role) {
            assert modifiable;
            structure.nativeEdge(structure.thingVertex(relation), structure.thingVertex(role), RELATING);
        }

        public void playing(Identifier.Variable thing, Identifier.Scoped role) {
            assert modifiable;
            structure.nativeEdge(structure.thingVertex(thing), structure.thingVertex(role), PLAYING);
        }

        public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, int repetition) {
            assert modifiable;
            structure.rolePlayer(structure.thingVertex(relation), structure.thingVertex(player), repetition);
        }

        public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, Set<Label> roleTypes, int repetition) {
            assert modifiable;
            structure.rolePlayer(structure.thingVertex(relation), structure.thingVertex(player), roleTypes, repetition);
        }

        public void clearLabels(Identifier.Variable type) {
            assert modifiable;
            structure.typeVertex(type).props().clearLabels();
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate token, String value) {
            assert modifiable;
            Predicate.Value.String predicate = Predicate.Value.String.of(token);
            structure.thingVertex(attribute).props().predicate(predicate);
            if (token == LIKE) parameters.pushValue(attribute, predicate, new Parameters.Value(Pattern.compile(value)));
            else parameters.pushValue(attribute, predicate, new Parameters.Value(value));
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, Boolean value) {
            assert modifiable;
            Predicate.Value.Numerical predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.BOOLEAN);
            parameters.pushValue(attribute, predicate, new Parameters.Value(value));
            structure.thingVertex(attribute).props().predicate(predicate);
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, Long value) {
            assert modifiable;
            Predicate.Value.Numerical predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.LONG);
            parameters.pushValue(attribute, predicate, new Parameters.Value(value));
            structure.thingVertex(attribute).props().predicate(predicate);
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, Double value) {
            assert modifiable;
            long longValue = Math.round(value);
            if (Predicate.compareDoubles(value, longValue) == 0) {
                predicate(attribute, token, longValue);
            } else {
                Predicate.Value.Numerical predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.DOUBLE);
                parameters.pushValue(attribute, predicate, new Parameters.Value(value));
                structure.thingVertex(attribute).props().predicate(predicate);
            }
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, LocalDateTime value) {
            assert modifiable;
            Predicate.Value.Numerical predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.DATETIME);
            parameters.pushValue(attribute, predicate, new Parameters.Value(value));
            structure.thingVertex(attribute).props().predicate(predicate);
        }

        public void predicate(Identifier.Variable att1, TypeQLToken.Predicate.Equality token, Identifier.Variable att2) {
            assert modifiable;
            Predicate.Variable predicate = Predicate.Variable.of(token);
            structure.predicateEdge(structure.thingVertex(att1), structure.thingVertex(att2), predicate);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Thing that = (Thing) o;

            // We compare this.filter() instead of this.filter, as the property is dynamically calculated
            return (this.structure.equals(that.structure) &&
                    this.parameters.equals(that.parameters) &&
                    this.filter().equals(that.filter()));
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.structure, this.parameters, this.filter);
        }

    }
}
