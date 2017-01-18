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

public class SumMapReduce extends GraknMapReduce<Number> {

    public static final String MEMORY_KEY = "sum";
    private static final String RESOURCE_DATA_TYPE_KEY = "RESOURCE_DATA_TYPE_KEY";

    public SumMapReduce() {
    }

    public SumMapReduce(Set<TypeName> selectedTypes, String resourceDataType) {
        this.selectedTypes = selectedTypes;
        persistentProperties.put(RESOURCE_DATA_TYPE_KEY, resourceDataType);
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Number> emitter) {
        if (persistentProperties.get(RESOURCE_DATA_TYPE_KEY).equals(ResourceType.DataType.LONG.getName())) {
            if (selectedTypes.contains(Utility.getVertexType(vertex)) &&
                    ((Long) vertex.value(DegreeVertexProgram.DEGREE)) > 0) {
                emitter.emit(MEMORY_KEY,
                        ((Long) vertex.value(Schema.ConceptProperty.VALUE_LONG.name())) *
                                ((Long) vertex.value(DegreeVertexProgram.DEGREE)));
                return;
            }
            emitter.emit(MEMORY_KEY, 0L);
        } else {
            if (selectedTypes.contains(Utility.getVertexType(vertex)) &&
                    ((Long) vertex.value(DegreeVertexProgram.DEGREE)) > 0) {
                emitter.emit(MEMORY_KEY,
                        ((Double) vertex.value(Schema.ConceptProperty.VALUE_DOUBLE.name())) *
                                ((Long) vertex.value(DegreeVertexProgram.DEGREE)));
                return;
            }
            emitter.emit(MEMORY_KEY, 0D);
        }
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Number> values,
                       final ReduceEmitter<Serializable, Number> emitter) {
        if (persistentProperties.get(RESOURCE_DATA_TYPE_KEY).equals(ResourceType.DataType.LONG.getName())) {
            emitter.emit(key, IteratorUtils.reduce(values, 0L,
                    (a, b) -> a.longValue() + b.longValue()));
        } else {
            emitter.emit(key, IteratorUtils.reduce(values, 0D,
                    (a, b) -> a.doubleValue() + b.doubleValue()));
        }
    }

    @Override
    public void combine(final Serializable key, final Iterator<Number> values,
                        final ReduceEmitter<Serializable, Number> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public Map<Serializable, Number> generateFinalResult(Iterator<KeyValue<Serializable, Number>> keyValues) {
        final Map<Serializable, Number> sum = new HashMap<>();
        keyValues.forEachRemaining(pair -> sum.put(pair.getKey(), pair.getValue()));
        return sum;
    }
}
