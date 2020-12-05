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
import grakn.core.common.cache.CommonCache;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import static java.util.stream.Collectors.toList;

public class Traversal {

    private final Parameters parameters;
    private final Structure structure;
    private List<Planner> planners;

    public Traversal() {
        structure = new Structure();
        parameters = new Parameters();
    }

    public Identifier.Generated newIdentifier() {
        return structure.newIdentifier();
    }

    void initialisePlanner(CommonCache<Structure, Planner> cache) {
        planners = structure.asGraphs().stream().map(s -> cache.get(s, Planner::create)).collect(toList());
    }

    Producer<VertexMap> execute(GraphManager graphMgr, int parallelisation) {
        assert !planners.isEmpty();
        if (planners.size() == 1) {
            planners.get(0).optimise(graphMgr);
            return planners.get(0).procedure().execute(graphMgr, parameters, parallelisation);
        } else {
            return produce(cartesian(planners.parallelStream().map(planner -> {
                planner.optimise(graphMgr);
                return planner.procedure().execute(graphMgr, parameters, parallelisation);
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

    public void relating(Identifier.Variable relation, Identifier.Generated role) {
        structure.nativeEdge(structure.thingVertex(relation), structure.thingVertex(role), RELATING);
    }

    public void playing(Identifier.Variable thing, Identifier.Generated role) {
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

    public void types(Identifier.Variable thing, Set<Label> labels) {
        structure.thingVertex(thing).props().types(labels);
    }

    public void isAbstract(Identifier.Variable type) {
        structure.typeVertex(type).props().isAbstract(true);
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
        Predicate.String predicate = new Predicate.String(token);
        structure.thingVertex(attribute).props().predicate(predicate);
        if (token == GraqlToken.Predicate.SubString.LIKE) {
            parameters.pushValue(attribute, predicate, Pattern.compile(value));
        } else {
            parameters.pushValue(attribute, predicate, value);
        }
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, Boolean value) {
        Predicate.Boolean predicate = new Predicate.Boolean(token);
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, Long value) {
        Predicate.Long predicate = new Predicate.Long(token);
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, Double value) {
        Predicate.Double predicate = new Predicate.Double(token);
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality token, LocalDateTime value) {
        Predicate.DateTime predicate = new Predicate.DateTime(token);
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).props().predicate(predicate);
    }

    public void predicate(Identifier.Variable att1, GraqlToken.Predicate.Equality predicate, Identifier.Variable att2) {
        structure.predicateEdge(structure.thingVertex(att1), structure.thingVertex(att2), predicate);
    }

    public static class Parameters {

        private final Map<Identifier.Variable, VertexIID.Thing> iid;
        private final Map<Pair<Identifier, Predicate<?>>, LinkedList<Value>> values;

        public Parameters() {
            iid = new HashMap<>();
            values = new HashMap<>();
        }

        public void putIID(Identifier.Variable identifier, VertexIID.Thing iid) {
            this.iid.put(identifier, iid);
        }

        public void pushValue(Identifier.Variable identifier, Predicate.Boolean predicate, boolean value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, Predicate.Long predicate, long value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, Predicate.Double predicate, double value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, Predicate.DateTime predicate, LocalDateTime value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, Predicate.String predicate, String value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, Predicate.String predicate, Pattern regex) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(regex));
        }

        public VertexIID.Thing getIID(Identifier.Variable identifier) {
            return iid.get(identifier);
        }

        public LinkedList<Value> getValues(Identifier.Variable identifier, Predicate<?> predicate) {
            return values.get(pair(identifier, predicate));
        }

        public static class Value {

            private final Encoding.ValueType valueType;
            private final Boolean booleanValue;
            private final Long longValue;
            private final Double doubleValue;
            private final String stringValue;
            private final LocalDateTime dateTimeValue;
            private final Pattern regexPattern;

            Value(boolean value) {
                valueType = Encoding.ValueType.BOOLEAN;
                booleanValue = value;
                longValue = null;
                doubleValue = null;
                stringValue = null;
                dateTimeValue = null;
                regexPattern = null;
            }

            Value(long value) {
                valueType = Encoding.ValueType.LONG;
                booleanValue = null;
                longValue = value;
                doubleValue = null;
                stringValue = null;
                dateTimeValue = null;
                regexPattern = null;
            }

            Value(double value) {
                valueType = Encoding.ValueType.DOUBLE;
                booleanValue = null;
                longValue = null;
                doubleValue = value;
                stringValue = null;
                dateTimeValue = null;
                regexPattern = null;
            }

            Value(LocalDateTime value) {
                valueType = Encoding.ValueType.DATETIME;
                booleanValue = null;
                longValue = null;
                doubleValue = null;
                stringValue = null;
                dateTimeValue = value;
                regexPattern = null;
            }

            Value(String value) {
                valueType = Encoding.ValueType.STRING;
                booleanValue = null;
                longValue = null;
                doubleValue = null;
                stringValue = value;
                dateTimeValue = null;
                regexPattern = null;
            }

            Value(Pattern regex) {
                valueType = Encoding.ValueType.STRING;
                booleanValue = null;
                longValue = null;
                doubleValue = null;
                stringValue = null;
                dateTimeValue = null;
                regexPattern = regex;
            }

            public Encoding.ValueType valueType() {
                return valueType;
            }

            public boolean isBoolean() { return booleanValue != null; }

            public boolean isLong() { return longValue != null; }

            public boolean isDouble() { return doubleValue != null; }

            public boolean isDateTime() { return dateTimeValue != null; }

            public boolean isString() { return stringValue != null; }

            public boolean isRegex() { return regexPattern != null; }

            public Boolean getBoolean() { return booleanValue; }

            public Long getLong() { return longValue; }

            public Double getDouble() { return doubleValue; }

            public LocalDateTime getDateTime() { return dateTimeValue; }

            public String getString() { return stringValue; }

            public Pattern getRegex() { return regexPattern; }
        }
    }
}
