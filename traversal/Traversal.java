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
import grakn.core.common.iterator.Iterators;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Label;
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.graph.TraversalEdge;
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

import static grakn.common.collection.Collections.pair;
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

    void initialisePlanner(TraversalCache cache) {
        planners = structure.asGraphs().stream().map(s -> cache.get(s, Planner::create)).collect(toList());
    }

    ResourceIterator<Map<Reference, Vertex<?, ?>>> execute(GraphManager graphMgr) {
        if (planners.size() == 1) {
            planners.get(0).optimise(graphMgr.schema());
            return planners.get(0).procedure().execute(graphMgr, parameters);
        } else {
            return Iterators.cartesian(planners.stream().map(planner -> {
                planner.optimise(graphMgr.schema());
                return planner.procedure().execute(graphMgr, parameters);
            }).collect(toList())).map(partialAnswers -> {
                Map<Reference, Vertex<?, ?>> combinedAnswers = new HashMap<>();
                partialAnswers.forEach(combinedAnswers::putAll);
                return combinedAnswers;
            });
        }
    }

    public void equalThings(Identifier.Variable thing1, Identifier.Variable thing2) {
        structure.edge(new TraversalEdge.Property.Equal(),
                       structure.thingVertex(thing1),
                       structure.thingVertex(thing2));
    }

    public void equalTypes(Identifier.Variable type1, Identifier.Variable type2) {
        structure.edge(new TraversalEdge.Property.Equal(),
                       structure.typeVertex(type1),
                       structure.typeVertex(type2));
    }

    public void has(Identifier.Variable thing, Identifier.Variable attribute) {
        structure.edge(new TraversalEdge.Property.Encoder(HAS),
                       structure.thingVertex(thing),
                       structure.thingVertex(attribute));
    }

    public void isa(Identifier thing, Identifier.Variable type) {
        isa(thing, type, true);
    }

    public void isa(Identifier thing, Identifier.Variable type, boolean isTransitive) {
        structure.edge(new TraversalEdge.Property.Encoder(ISA, isTransitive),
                       structure.thingVertex(thing),
                       structure.typeVertex(type));
    }

    public void relating(Identifier.Variable relation, Identifier.Generated role) {
        structure.edge(new TraversalEdge.Property.Encoder(RELATING),
                       structure.thingVertex(relation),
                       structure.thingVertex(role));
    }

    public void playing(Identifier.Variable thing, Identifier.Generated role) {
        structure.edge(new TraversalEdge.Property.Encoder(PLAYING),
                       structure.thingVertex(thing),
                       structure.thingVertex(role));
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player) {
        structure.edge(new TraversalEdge.Property.Encoder(ROLEPLAYER),
                       structure.thingVertex(relation),
                       structure.thingVertex(player));
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, Set<Label> labels) {
        structure.edge(new TraversalEdge.Property.Encoder(ROLEPLAYER, labels),
                       structure.thingVertex(relation),
                       structure.thingVertex(player));
    }

    public void owns(Identifier.Variable thingType, Identifier.Variable attributeType, boolean isKey) {
        structure.edge(new TraversalEdge.Property.Encoder(isKey ? OWNS_KEY : OWNS),
                       structure.typeVertex(thingType),
                       structure.typeVertex(attributeType));
    }

    public void plays(Identifier.Variable thingType, Identifier.Variable roleType) {
        structure.edge(new TraversalEdge.Property.Encoder(PLAYS),
                       structure.typeVertex(thingType),
                       structure.typeVertex(roleType));
    }

    public void relates(Identifier.Variable relationType, Identifier.Variable roleType) {
        structure.edge(new TraversalEdge.Property.Encoder(RELATES),
                       structure.typeVertex(relationType),
                       structure.typeVertex(roleType));
    }

    public void sub(Identifier.Variable subType, Identifier.Variable superType, boolean isTransitive) {
        structure.edge(new TraversalEdge.Property.Encoder(SUB, isTransitive),
                       structure.typeVertex(subType),
                       structure.typeVertex(superType));
    }

    public void iid(Identifier.Variable thing, byte[] iid) {
        parameters.putIID(thing, iid);
        structure.thingVertex(thing).properties().hasIID(true);
    }

    public void type(Identifier.Variable thing, Set<Label> labels) {
        structure.thingVertex(thing).properties().types(labels);
    }

    public void isAbstract(Identifier.Variable type) {
        structure.typeVertex(type).properties().isAbstract(true);
    }

    public void label(Identifier.Variable type, Label label) {
        structure.typeVertex(type).properties().label(label);
    }

    public void regex(Identifier.Variable type, String regex) {
        structure.typeVertex(type).properties().regex(regex);
    }

    public void valueType(Identifier.Variable attributeType, GraqlArg.ValueType valueType) {
        structure.typeVertex(attributeType).properties().valueType(Encoding.ValueType.of(valueType));
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate predicate, String value) {
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).properties().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality predicate, Boolean value) {
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).properties().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality predicate, Long value) {
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).properties().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality predicate, Double value) {
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).properties().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute, GraqlToken.Predicate.Equality predicate, LocalDateTime value) {
        parameters.pushValue(attribute, predicate, value);
        structure.thingVertex(attribute).properties().predicate(predicate);
    }

    public void predicate(Identifier.Variable attribute1, GraqlToken.Predicate.Equality predicate,
                          Identifier.Variable attribute2) {
        structure.edge(new TraversalEdge.Property.Predicate(predicate),
                       structure.thingVertex(attribute1),
                       structure.thingVertex(attribute2));
    }

    public static class Parameters {

        private final Map<Identifier, byte[]> iid;
        private final Map<Pair<Identifier, GraqlToken.Predicate>, LinkedList<Value>> values;

        public Parameters() {
            iid = new HashMap<>();
            values = new HashMap<>();
        }

        public void putIID(Identifier.Variable identifier, byte[] iid) {
            this.iid.put(identifier, iid);
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Predicate predicate, boolean value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Predicate predicate, long value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Predicate predicate, double value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Predicate predicate, String value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Predicate predicate, LocalDateTime value) {
            values.computeIfAbsent(pair(identifier, predicate), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public byte[] getIID(Identifier.Variable identifier) {
            return iid.get(identifier);
        }

        public Value popValue(Identifier.Variable identifier, GraqlToken.Predicate predicate) {
            return values.get(pair(identifier, predicate)).removeFirst();
        }

        static class Value {

            final Boolean booleanValue;
            final Long longValue;
            final Double doubleValue;
            final String stringValue;
            final LocalDateTime dateTimeValue;

            Value(boolean value) {
                booleanValue = value;
                longValue = null;
                doubleValue = null;
                stringValue = null;
                dateTimeValue = null;
            }

            Value(long value) {
                booleanValue = null;
                longValue = value;
                doubleValue = null;
                stringValue = null;
                dateTimeValue = null;
            }

            Value(double value) {
                booleanValue = null;
                longValue = null;
                doubleValue = value;
                stringValue = null;
                dateTimeValue = null;
            }

            Value(String value) {
                booleanValue = null;
                longValue = null;
                doubleValue = null;
                stringValue = value;
                dateTimeValue = null;
            }

            Value(LocalDateTime value) {
                booleanValue = null;
                longValue = null;
                doubleValue = null;
                stringValue = null;
                dateTimeValue = value;
            }

            boolean isBoolean() { return booleanValue != null; }

            boolean isLong() { return longValue != null; }

            boolean isDouble() { return doubleValue != null; }

            boolean isString() { return stringValue != null; }

            boolean isDateTime() { return dateTimeValue != null; }

            Boolean getBoolean() { return booleanValue; }

            Long getLong() { return longValue; }

            Double getDouble() { return doubleValue; }

            LocalDateTime getDateTime() { return dateTimeValue; }
        }
    }
}
