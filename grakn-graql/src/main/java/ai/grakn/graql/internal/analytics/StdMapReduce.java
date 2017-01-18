/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.analytics;

import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeName;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class StdMapReduce extends GraknMapReduce<Map<String, Double>> {

    public static final String MEMORY_KEY = "std";
    private static final String RESOURCE_DATA_TYPE_KEY = "RESOURCE_DATA_TYPE_KEY";

    public static final String COUNT = "C";
    public static final String SUM = "S";
    public static final String SQUARE_SUM = "SM";

    public StdMapReduce() {
    }

    public StdMapReduce(Set<TypeName> selectedTypes, String resourceDataType) {
        this.selectedTypes = selectedTypes;
        String resourceDataTypeValue = resourceDataType.equals(ResourceType.DataType.LONG.getName()) ?
                Schema.ConceptProperty.VALUE_LONG.name() : Schema.ConceptProperty.VALUE_DOUBLE.name();
        persistentProperties.put(RESOURCE_DATA_TYPE_KEY, resourceDataTypeValue);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Map<String, Double>> emitter) {
        if (selectedTypes.contains(Utility.getVertexType(vertex)) &&
                ((Long) vertex.value(DegreeVertexProgram.DEGREE)) > 0) {
            Map<String, Double> tuple = new HashMap<>(3);
            Double degree = ((Long) vertex.value(DegreeVertexProgram.DEGREE)).doubleValue();
            double value =
                    ((Number) vertex.value((String) persistentProperties.get(RESOURCE_DATA_TYPE_KEY))).doubleValue();
            tuple.put(SUM, value * degree);
            tuple.put(SQUARE_SUM, value * value * degree);
            tuple.put(COUNT, degree);
            emitter.emit(MEMORY_KEY, tuple);
            return;
        }
        Map<String, Double> emptyTuple = new HashMap<>(3);
        emptyTuple.put(SUM, 0D);
        emptyTuple.put(SQUARE_SUM, 0D);
        emptyTuple.put(COUNT, 0D);
        emitter.emit(MEMORY_KEY, emptyTuple);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Map<String, Double>> values,
                       final ReduceEmitter<Serializable, Map<String, Double>> emitter) {
        Map<String, Double> emptyTuple = new HashMap<>(3);
        emptyTuple.put(SUM, 0D);
        emptyTuple.put(SQUARE_SUM, 0D);
        emptyTuple.put(COUNT, 0D);
        emitter.emit(key, IteratorUtils.reduce(values, emptyTuple,
                (a, b) -> {
                    a.put(COUNT, a.get(COUNT) + b.get(COUNT));
                    a.put(SUM, a.get(SUM) + b.get(SUM));
                    a.put(SQUARE_SUM, a.get(SQUARE_SUM) + b.get(SQUARE_SUM));
                    return a;
                }));
    }

    @Override
    public void combine(final Serializable key, final Iterator<Map<String, Double>> values,
                        final ReduceEmitter<Serializable, Map<String, Double>> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Map<String, Double>> generateFinalResult(
            Iterator<KeyValue<Serializable, Map<String, Double>>> keyValues) {
        final Map<Serializable, Map<String, Double>> std = new HashMap<>();
        keyValues.forEachRemaining(pair -> std.put(pair.getKey(), pair.getValue()));
        return std;
    }
}
