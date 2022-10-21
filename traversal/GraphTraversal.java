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

package com.vaticle.typedb.core.traversal;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concurrent.producer.FunctionalProducer;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.common.VertexMap;
import com.vaticle.typedb.core.traversal.planner.Planner;
import com.vaticle.typedb.core.traversal.predicate.Predicate;
import com.vaticle.typedb.core.traversal.predicate.PredicateArgument;
import com.vaticle.typedb.core.traversal.procedure.CombinationProcedure;
import com.vaticle.typedb.core.traversal.scanner.CombinationFinder;
import com.vaticle.typeql.lang.common.TypeQLArg;
import com.vaticle.typeql.lang.common.TypeQLToken;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.vaticle.typedb.core.encoding.Encoding.Edge.ISA;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.HAS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.PLAYING;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Base.RELATING;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.OWNS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.OWNS_KEY;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.PLAYS;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.RELATES;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Type.SUB;
import static com.vaticle.typeql.lang.common.TypeQLToken.Predicate.SubString.LIKE;

// TODO: We should not use this object as a builder, as the hash and equality functions would change
//       We should introduce a separate "builder pattern" to Traversal, such that users of this library will build
//       traversals with Traversal.Builder, and call .build() in the end to produce a final Object.
public abstract class GraphTraversal extends Traversal {

    GraphTraversal() {
        super();
    }

    public void filter(Modifiers.Filter filter) {
        modifiers.filter(filter);
    }

    public void sort(Modifiers.Sorting sorting) {
        modifiers.sorting(sorting);
    }

    FunctionalIterator<VertexMap> permutationIterator(GraphManager graphMgr, Planner planner, boolean singleUse) {
        planner.tryOptimise(graphMgr, singleUse);
        return planner.procedure().iterator(graphMgr, parameters, modifiers);
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

    public void valueType(Identifier.Variable attributeType, Set<TypeQLArg.ValueType> valueTypes) {
        valueTypes.forEach(valueType ->
                structure.typeVertex(attributeType).props().valueType(Encoding.ValueType.of(valueType))
        );
    }

    public static class Type extends GraphTraversal {

        public Type() {
            super();
        }

        @Override
        FunctionalIterator<VertexMap> permutationIterator(GraphManager graphMgr) {
            return permutationIterator(graphMgr, Planner.create(structure, modifiers), true);
        }

        public Optional<Map<Identifier.Variable.Retrievable, Set<TypeVertex>>> combination(
                GraphManager graphMgr, Set<Identifier.Variable.Retrievable> concreteVarIds) {
            return new CombinationFinder(graphMgr, CombinationProcedure.create(structure), modifiers.filter(), concreteVarIds).combination();
        }
    }

    public static class Thing extends GraphTraversal {

        private Planner planner;
        private TraversalCache cache;

        public Thing() {
            super();
        }

        public void initialise(TraversalCache cache) {
            assert planner == null;
            this.cache = cache;
            planner = this.cache.getPlanner(structure, modifiers, sm -> Planner.create(sm.first(), sm.second()));
        }

        @Override
        FunctionalIterator<VertexMap> permutationIterator(GraphManager graphMgr) {
            assert planner != null && cache != null;
            FunctionalIterator<VertexMap> iter = permutationIterator(graphMgr, planner, false);
            cache.mayUpdatePlanner(structure, modifiers, planner);
            return iter;
        }

        FunctionalProducer<VertexMap> permutationProducer(GraphManager graphMgr, int parallelisation) {
            assert planner != null && cache != null;
            planner.tryOptimise(graphMgr, false);
            FunctionalProducer<VertexMap> producer = planner.procedure().producer(graphMgr, parameters, modifiers, parallelisation);
            cache.mayUpdatePlanner(structure, modifiers, planner);
            return producer;
        }

        public void equalThings(Identifier.Variable thing1, Identifier.Variable thing2) {
            structure.equalEdge(structure.thingVertex(thing1), structure.thingVertex(thing2));
        }

        public void iid(Identifier.Variable thing, ByteArray iid) {
            parameters.putIID(thing, VertexIID.Thing.of(iid));
            structure.thingVertex(thing).props().hasIID(true);
        }

        public void types(Identifier thing, Set<Label> labels) {
            structure.thingVertex(thing).props().types(labels);
        }

        public void has(Identifier.Variable thing, Identifier.Variable attribute) {
            structure.nativeEdge(structure.thingVertex(thing), structure.thingVertex(attribute), HAS);
        }

        public void isa(Identifier thing, Identifier.Variable type) {
            isa(thing, type, true);
        }

        public void isa(Identifier thing, Identifier.Variable type, boolean isTransitive) {
            structure.nativeEdge(structure.thingVertex(thing), structure.typeVertex(type), ISA, isTransitive);
        }

        public void relating(Identifier.Variable relation, Identifier.Scoped role) {
            structure.nativeEdge(structure.thingVertex(relation), structure.thingVertex(role), RELATING);
        }

        public void playing(Identifier.Variable thing, Identifier.Scoped role) {
            structure.nativeEdge(structure.thingVertex(thing), structure.thingVertex(role), PLAYING);
        }

        public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, Set<Label> roleTypes, int repetition) {
            structure.rolePlayer(structure.thingVertex(relation), structure.thingVertex(player), roleTypes, repetition);
        }

        public void clearLabels(Identifier.Variable type) {
            structure.typeVertex(type).props().clearLabels();
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate token, String value) {
            Predicate.Value.String predicate = Predicate.Value.String.of(token);
            structure.thingVertex(attribute).props().predicate(predicate);
            if (token == LIKE) parameters.pushValue(attribute, predicate, new Parameters.Value.Regex(value));
            else parameters.pushValue(attribute, predicate, new Parameters.Value.String(value));
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, Boolean value) {
            Predicate.Value.Numerical<Boolean> predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.BOOLEAN);
            parameters.pushValue(attribute, predicate, new Parameters.Value.Boolean(value));
            structure.thingVertex(attribute).props().predicate(predicate);
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, Long value) {
            Predicate.Value.Numerical<Long> predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.LONG);
            parameters.pushValue(attribute, predicate, new Parameters.Value.Long(value));
            structure.thingVertex(attribute).props().predicate(predicate);
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, Double value) {
            long longValue = Math.round(value);
            if (Encoding.ValueType.DOUBLE.comparator().compare(value, (double) longValue) == 0) {
                predicate(attribute, token, longValue);
            } else {
                Predicate.Value.Numerical<Double> predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.DOUBLE);
                parameters.pushValue(attribute, predicate, new Parameters.Value.Double(value));
                structure.thingVertex(attribute).props().predicate(predicate);
            }
        }

        public void predicate(Identifier.Variable attribute, TypeQLToken.Predicate.Equality token, LocalDateTime value) {
            Predicate.Value.Numerical<LocalDateTime> predicate = Predicate.Value.Numerical.of(token, PredicateArgument.Value.DATETIME);
            parameters.pushValue(attribute, predicate, new Parameters.Value.DateTime(value));
            structure.thingVertex(attribute).props().predicate(predicate);
        }

        public void predicate(Identifier.Variable att1, TypeQLToken.Predicate.Equality token, Identifier.Variable att2) {
            Predicate.Variable predicate = Predicate.Variable.of(token);
            structure.predicateEdge(structure.thingVertex(att1), structure.thingVertex(att2), predicate);
        }
    }
}
