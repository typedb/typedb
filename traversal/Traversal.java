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
import grakn.core.traversal.planner.Planner;
import grakn.core.traversal.structure.Structure;
import grakn.core.traversal.structure.StructureVertex;
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
        planners = structure.graphs().stream().map(p1 -> cache.get(p1, p2 -> {
            // TODO
            return new Planner();
        })).collect(toList());
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

    public void is(Identifier.Variable concept1, Identifier.Variable concept2) {
        structure.edge(concept1, concept2);
    }

    public void has(Identifier.Variable thing, Identifier.Variable attribute) {
        structure.edge(HAS, thing, attribute);
    }

    public void isa(Identifier thing, Identifier.Variable type) {
        isa(thing, type, true);
    }

    public void isa(Identifier thing, Identifier.Variable type, boolean isTransitive) {
        structure.edge(ISA, thing, type, isTransitive);
    }

    public void relating(Identifier.Variable relation, Identifier.Generated role) {
        structure.edge(RELATING, relation, role);
    }

    public void playing(Identifier.Variable thing, Identifier.Generated role) {
        structure.edge(PLAYING, thing, role);
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player) {
        rolePlayer(relation, player, new String[]{});
    }

    public void rolePlayer(Identifier.Variable relation, Identifier.Variable player, String[] labels) {
        structure.edge(ROLEPLAYER, relation, player, labels);
    }

    public void owns(Identifier.Variable thingType, Identifier.Variable attributeType, boolean isKey) {
        if (isKey) structure.edge(OWNS_KEY, thingType, attributeType);
        else structure.edge(OWNS, thingType, attributeType);
    }

    public void plays(Identifier.Variable thingType, Identifier.Variable roleType) {
        structure.edge(PLAYS, thingType, roleType);
    }

    public void relates(Identifier.Variable relationType, Identifier.Variable roleType) {
        structure.edge(RELATES, relationType, roleType);
    }

    public void sub(Identifier.Variable subType, Identifier.Variable superType, boolean isTransitive) {
        structure.edge(SUB, subType, superType, isTransitive);
    }

    public void iid(Identifier.Variable thing, byte[] iid) {
        parameters.putIID(thing, iid);
        structure.vertex(thing).property(new StructureVertex.Property.IID(thing));
    }

    public void type(Identifier.Variable thing, String[] labels) {
        structure.vertex(thing).property(new StructureVertex.Property.Type(labels));
    }

    public void isAbstract(Identifier.Variable type) {
        structure.vertex(type).property(new StructureVertex.Property.Abstract());
    }

    public void label(Identifier.Variable type, String label, @Nullable String scope) {
        structure.vertex(type).property(new StructureVertex.Property.Label(label, scope));
    }

    public void regex(Identifier.Variable type, String regex) {
        structure.vertex(type).property(new StructureVertex.Property.Regex(regex));
    }

    public void valueType(Identifier.Variable attributeType, GraqlArg.ValueType valueType) {
        structure.vertex(attributeType).property(new StructureVertex.Property.ValueType(Encoding.ValueType.of(valueType)));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator comparator, String value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new StructureVertex.Property.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Boolean value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new StructureVertex.Property.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Long value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new StructureVertex.Property.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, Double value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new StructureVertex.Property.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute, GraqlToken.Comparator.Equality comparator, LocalDateTime value) {
        parameters.pushValue(attribute, comparator, value);
        structure.vertex(attribute).property(new StructureVertex.Property.Value(comparator, attribute));
    }

    public void value(Identifier.Variable attribute1, GraqlToken.Comparator.Equality comparator, Identifier.Variable attribute2) {
        structure.edge(comparator, attribute1, attribute2);
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
