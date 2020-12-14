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

package grakn.core.traversal;

import grakn.common.collection.Pair;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.common.producer.Producer;
import grakn.core.graph.GraphManager;
import grakn.core.graph.iid.VertexIID;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Predicate;
import grakn.core.traversal.common.VertexMap;
import grakn.core.traversal.planner.Planner;
import grakn.core.traversal.structure.Structure;
import graql.lang.common.GraqlArg;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.variable.Reference;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static grakn.common.collection.Collections.pair;
import static grakn.core.common.iterator.Iterators.cartesian;
import static grakn.core.common.producer.Producers.buffer;
import static grakn.core.common.producer.Producers.produce;
import static grakn.core.graph.util.Encoding.Edge.ISA;
import static grakn.core.graph.util.Encoding.Edge.Thing.HAS;
import static grakn.core.graph.util.Encoding.Edge.Thing.PLAYING;
import static grakn.core.graph.util.Encoding.Edge.Thing.RELATING;
import static grakn.core.graph.util.Encoding.Edge.Thing.ROLEPLAYER;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS;
import static grakn.core.graph.util.Encoding.Edge.Type.OWNS_KEY;
import static grakn.core.graph.util.Encoding.Edge.Type.PLAYS;
import static grakn.core.graph.util.Encoding.Edge.Type.RELATES;
import static grakn.core.graph.util.Encoding.Edge.Type.SUB;
import static grakn.core.graph.util.Encoding.ValueType.BOOLEAN;
import static grakn.core.graph.util.Encoding.ValueType.DATETIME;
import static grakn.core.graph.util.Encoding.ValueType.DOUBLE;
import static grakn.core.graph.util.Encoding.ValueType.LONG;
import static grakn.core.graph.util.Encoding.ValueType.STRING;
import static graql.lang.common.GraqlToken.Predicate.SubString.LIKE;
import static java.util.stream.Collectors.toList;

public class Traversal {

    private final Parameters parameters;
    private final Structure structure;
    private List<Planner> planners;

    public Traversal() {
        structure = new Structure();
        parameters = new Parameters();
    }

    public Identifier.Scoped newIdentifier(Identifier.Variable scope) {
        return structure.newIdentifier(scope);
    }

    void initialisePlanner(TraversalCache cache) {
        planners = structure.asGraphs().stream().map(s -> {
            return cache.get(s, Planner::create);
        }).collect(toList());
    }

    ResourceIterator<VertexMap> iterator(GraphManager graphMgr) {
        assert !planners.isEmpty();
        if (planners.size() == 1) {
            planners.get(0).tryOptimise(graphMgr);
            return planners.get(0).procedure().iterator(graphMgr, parameters);
        } else {
            return cartesian(planners.parallelStream().map(planner -> {
                planner.tryOptimise(graphMgr);
                return planner.procedure().iterator(graphMgr, parameters);
            }).collect(toList())).map(partialAnswers -> {
                Map<Reference, Vertex<?, ?>> combinedAnswers = new HashMap<>();
                partialAnswers.forEach(p -> combinedAnswers.putAll(p.map()));
                return VertexMap.of(combinedAnswers);
            });
        }
    }

    Producer<VertexMap> producer(GraphManager graphMgr, int parallelisation) {
        assert !planners.isEmpty();
        if (planners.size() == 1) {
            planners.get(0).tryOptimise(graphMgr);
            return planners.get(0).procedure().producer(graphMgr, parameters, parallelisation);
        } else {
            return produce(cartesian(planners.parallelStream().map(planner -> {
                planner.tryOptimise(graphMgr);
                return planner.procedure().producer(graphMgr, parameters, parallelisation);
            }).map(p -> buffer(p).iterator()).collect(toList())).map(partialAnswers -> {
                Map<Reference, Vertex<?, ?>> combinedAnswers = new HashMap<>();
                partialAnswers.forEach(p -> combinedAnswers.putAll(p.map()));
                return VertexMap.of(combinedAnswers);
            }));
        }
    }

    public void equalThings(Identifier.Variable thing1, Identifier.Variable thing2) {
        structure.equalEdge(structure.thingVertex(thing1), structure.thingVertex(thing2));
    }

    public void equalTypes(Identifier.Variable type1, Identifier.Variable type2) {
        structure.equalEdge(structure.typeVertex(type1), structure.typeVertex(type2));
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

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player) {
        structure.optimisedEdge(structure.thingVertex(relation), structure.thingVertex(player), ROLEPLAYER);
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, Set<Label> roleTypes) {
        structure.optimisedEdge(structure.thingVertex(relation), structure.thingVertex(player), ROLEPLAYER, roleTypes);
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

    public void iid(Identifier.Variable thing, byte[] iid) {
        parameters.putIID(thing, VertexIID.Thing.of(iid));
        structure.thingVertex(thing).props().hasIID(true);
    }

    public void types(Identifier thing, Set<Label> labels) {
        structure.thingVertex(thing).props().types(labels);
    }

    public void isAbstract(Identifier.Variable type) {
        structure.typeVertex(type).props().setAbstract();
    }

    public void labels(Identifier.Variable type, Label label) {
        structure.typeVertex(type).props().labels(label);
    }

    public void labels(Identifier.Variable type, Set<Label> label) {
        structure.typeVertex(type).props().labels(label);
    }

    public void regex(Identifier.Variable type, String regex) {
        structure.typeVertex(type).props().regex(regex);
    }

    public void valueType(Identifier.Variable attributeType, GraqlArg.ValueType valueType) {
        structure.typeVertex(attributeType).props().valueType(Encoding.ValueType.of(valueType));
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate token, String value) {
        Predicate.Value.SubString predicate = Predicate.Value.SubString.of(token);
        structure.thingVertex(attribute).props().predicate(predicate);
        if (token == LIKE) parameters.pushValue(attribute, predicate, new Parameters.Value(Pattern.compile(value)));
        else parameters.pushValue(attribute, predicate, new Parameters.Value(value));
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, Boolean value) {
        Predicate.Value.Equality predicate = Predicate.Value.Equality.of(token, Predicate.Argument.Value.BOOLEAN);
        parameters.pushValue(attribute, predicate, new Parameters.Value(value));
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, Long value) {
        Predicate.Value.Equality predicate = Predicate.Value.Equality.of(token, Predicate.Argument.Value.LONG);
        parameters.pushValue(attribute, predicate, new Parameters.Value(value));
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, Double value) {
        Predicate.Value.Equality predicate = Predicate.Value.Equality.of(token, Predicate.Argument.Value.DOUBLE);
        parameters.pushValue(attribute, predicate, new Parameters.Value(value));
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, LocalDateTime value) {
        Predicate.Value.Equality predicate = Predicate.Value.Equality.of(token, Predicate.Argument.Value.DATETIME);
        parameters.pushValue(attribute, predicate, new Parameters.Value(value));
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable att1, GraqlToken.Predicate.Equality token, Identifier.Variable att2) {
        Predicate.Variable predicate = Predicate.Variable.of(token);
        structure.predicateEdge(structure.thingVertex(att1), structure.thingVertex(att2), predicate);
    }

    public static class Parameters {

        private final Map<Identifier.Variable, VertexIID.Thing> iid;
        private final Map<Pair<Identifier.Variable, Predicate.Value<?>>, Set<Value>> values;

        public Parameters() {
            iid = new HashMap<>();
            values = new HashMap<>();
        }

        public void putIID(Identifier.Variable identifier, VertexIID.Thing iid) {
            this.iid.put(identifier, iid);
        }

        public void pushValue(Identifier.Variable identifier, Predicate.Value<?> predicate, Value value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new HashSet<>()).add(value);
        }

        public VertexIID.Thing getIID(Identifier.Variable identifier) {
            return iid.get(identifier);
        }

        public Set<Value> getValues(Identifier.Variable identifier, Predicate.Value<?> predicate) {
            return values.get(pair(identifier, predicate));
        }

        public static class Value {

            private final Encoding.ValueType valueType;
            private final Boolean booleanVal;
            private final Long longVal;
            private final Double doubleVal;
            private final String stringVal;
            private final LocalDateTime dateTimeVal;
            private final Pattern regexPattern;
            private final int hash;

            Value(boolean value) {
                this(BOOLEAN, value, null, null, null, null, null);
            }

            Value(long value) {
                this(LONG, null, value, null, null, null, null);
            }

            Value(double value) {
                this(DOUBLE, null, null, value, null, null, null);
            }

            Value(LocalDateTime value) {
                this(DATETIME, null, null, null, value, null, null);
            }

            Value(String value) {
                this(STRING, null, null, null, null, value, null);
            }

            Value(Pattern regex) {
                this(STRING, null, null, null, null, null, regex);
            }

            private Value(Encoding.ValueType valueType, Boolean booleanVal, Long longVal, Double doubleVal,
                          LocalDateTime dateTimeVal, String stringVal, Pattern regexPattern) {
                this.valueType = valueType;
                this.booleanVal = booleanVal;
                this.longVal = longVal;
                this.doubleVal = doubleVal;
                this.dateTimeVal = dateTimeVal;
                this.stringVal = stringVal;
                this.regexPattern = regexPattern;
                this.hash = Objects.hash(valueType, booleanVal, longVal, doubleVal, dateTimeVal, stringVal, regexPattern);
            }

            public Encoding.ValueType valueType() {
                return valueType;
            }

            public boolean isBoolean() { return booleanVal != null; }

            public boolean isLong() { return longVal != null; }

            public boolean isDouble() { return doubleVal != null; }

            public boolean isDateTime() { return dateTimeVal != null; }

            public boolean isString() { return stringVal != null; }

            public boolean isRegex() { return regexPattern != null; }

            public Boolean getBoolean() { return booleanVal; }

            public Long getLong() { return longVal; }

            public Double getDouble() { return doubleVal; }

            public LocalDateTime getDateTime() { return dateTimeVal; }

            public String getString() { return stringVal; }

            public Pattern getRegex() { return regexPattern; }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Value that = (Value) o;
                return (Objects.equals(this.valueType, that.valueType) &&
                        Objects.equals(this.booleanVal, that.booleanVal) &&
                        Objects.equals(this.longVal, that.longVal) &&
                        Objects.equals(this.doubleVal, that.doubleVal) &&
                        Objects.equals(this.dateTimeVal, that.dateTimeVal) &&
                        Objects.equals(this.stringVal, that.stringVal) &&
                        Objects.equals(this.regexPattern, that.regexPattern));
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }
}
