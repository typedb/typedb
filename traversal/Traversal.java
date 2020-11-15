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
import grakn.core.graph.GraphManager;
import grakn.core.graph.util.Encoding;
import grakn.core.graph.vertex.Vertex;
import grakn.core.traversal.graph.EdgeProperty;
import grakn.core.traversal.graph.VertexProperty;
import grakn.core.traversal.planner.Planner;
import grakn.core.traversal.structure.Structure;
import graql.lang.common.GraqlArg;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
            }).collect(toList())).map(list -> {
                Map<Reference, Vertex<?, ?>> answer = new HashMap<>();
                list.forEach(answer::putAll);
                return answer;
            });
        }
    }

    public void equalThings(Identifier.Variable thing1, Identifier.Variable thing2) {
        structure.edge(new EdgeProperty.Equal(),
                       structure.thingVertex(thing1),
                       structure.thingVertex(thing2));
    }

    public void equalTypes(Identifier.Variable type1, Identifier.Variable type2) {
        structure.edge(new EdgeProperty.Equal(),
                       structure.typeVertex(type1),
                       structure.typeVertex(type2));
    }

    public void has(Identifier.Variable thing, Identifier.Variable attribute) {
        structure.edge(new EdgeProperty.Type(HAS),
                       structure.thingVertex(thing),
                       structure.thingVertex(attribute));
    }

    public void isa(Identifier thing, Identifier.Variable type) {
        isa(thing, type, true);
    }

    public void isa(Identifier thing, Identifier.Variable type, boolean isTransitive) {
        structure.edge(new EdgeProperty.Type(ISA, isTransitive),
                       structure.thingVertex(thing),
                       structure.typeVertex(type));
    }

    public void relating(Identifier.Variable relation, Identifier.Generated role) {
        structure.edge(new EdgeProperty.Type(RELATING),
                       structure.thingVertex(relation),
                       structure.thingVertex(role));
    }

    public void playing(Identifier.Variable thing, Identifier.Generated role) {
        structure.edge(new EdgeProperty.Type(PLAYING),
                       structure.thingVertex(thing),
                       structure.thingVertex(role));
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player) {
        structure.edge(new EdgeProperty.Type(ROLEPLAYER),
                       structure.thingVertex(relation),
                       structure.thingVertex(player));
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, String[] labels) {
        structure.edge(new EdgeProperty.Type(ROLEPLAYER, labels),
                       structure.thingVertex(relation),
                       structure.thingVertex(player));
    }

    public void owns(Identifier.Variable thingType, Identifier.Variable attributeType, boolean isKey) {
        structure.edge(new EdgeProperty.Type(isKey ? OWNS_KEY : OWNS),
                       structure.typeVertex(thingType),
                       structure.typeVertex(attributeType));
    }

    public void plays(Identifier.Variable thingType, Identifier.Variable roleType) {
        structure.edge(new EdgeProperty.Type(PLAYS),
                       structure.typeVertex(thingType),
                       structure.typeVertex(roleType));
    }

    public void relates(Identifier.Variable relationType, Identifier.Variable roleType) {
        structure.edge(new EdgeProperty.Type(RELATES),
                       structure.typeVertex(relationType),
                       structure.typeVertex(roleType));
    }

    public void sub(Identifier.Variable subType, Identifier.Variable superType, boolean isTransitive) {
        structure.edge(new EdgeProperty.Type(SUB, isTransitive),
                       structure.typeVertex(subType),
                       structure.typeVertex(superType));
    }

    public void iid(Identifier.Variable thing, byte[] iid) {
        parameters.putIID(thing, iid);
        structure.thingVertex(thing).property(new VertexProperty.Thing.IID(thing));
    }

    public void type(Identifier.Variable thing, String[] labels) {
        structure.thingVertex(thing).property(new VertexProperty.Thing.Isa(labels));
    }

    public void isAbstract(Identifier.Variable type) {
        structure.typeVertex(type).property(new VertexProperty.Type.Abstract());
    }

    public void label(Identifier.Variable type, String label, @Nullable String scope) {
        structure.typeVertex(type).property(new VertexProperty.Type.Label(label, scope));
    }

    public void regex(Identifier.Variable type, String regex) {
        structure.typeVertex(type).property(new VertexProperty.Type.Regex(regex));
    }

    public void valueType(Identifier.Variable attributeType, GraqlArg.ValueType valueType) {
        structure.typeVertex(attributeType).property(new VertexProperty.Type.ValueType(Encoding.ValueType.of(valueType)));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator comparator, String value) {
        parameters.pushValue(attribute, comparator, value);
        structure.thingVertex(attribute).property(new VertexProperty.Thing.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Boolean value) {
        parameters.pushValue(attribute, comparator, value);
        structure.thingVertex(attribute).property(new VertexProperty.Thing.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Long value) {
        parameters.pushValue(attribute, comparator, value);
        structure.thingVertex(attribute).property(new VertexProperty.Thing.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Double value) {
        parameters.pushValue(attribute, comparator, value);
        structure.thingVertex(attribute).property(new VertexProperty.Thing.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, LocalDateTime value) {
        parameters.pushValue(attribute, comparator, value);
        structure.thingVertex(attribute).property(new VertexProperty.Thing.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute1, GraqlToken.Comparator.Equality comparator, Identifier.Variable attribute2) {
        structure.edge(new EdgeProperty.Comparator(comparator),
                       structure.thingVertex(attribute1),
                       structure.thingVertex(attribute2));
    }

    public static class Parameters {

        private final Map<Identifier, byte[]> iid;
        private final Map<Pair<Identifier, GraqlToken.Comparator>, LinkedList<Value>> values;

        public Parameters() {
            iid = new HashMap<>();
            values = new HashMap<>();
        }

        public void putIID(Identifier.Variable identifier, byte[] iid) {
            this.iid.put(identifier, iid);
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, boolean value) {
            values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, long value) {
            values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, double value) {
            values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, String value) {
            values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, LocalDateTime value) {
            values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
        }

        public byte[] getIID(Identifier.Variable identifier) {
            return iid.get(identifier);
        }

        public Value popValue(Identifier.Variable identifier, GraqlToken.Comparator comparator) {
            return values.get(pair(identifier, comparator)).removeFirst();
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
