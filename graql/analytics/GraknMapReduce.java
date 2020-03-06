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
 */

package grakn.core.graql.analytics;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.LabelId;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A map reduce task specific to Grakn with common method implementations.
 * <p>
 *
 * @param <T> type type of element that is being reduced
 */

public abstract class GraknMapReduce<T> extends CommonOLAP
        implements MapReduce<Serializable, T, Serializable, T, Map<Serializable, T>> {

    private static final String RESOURCE_DATA_TYPE_KEY = "RESOURCE_DATA_TYPE_KEY";

    // In MapReduce, vertex emits type label id, but has-resource edge can not. Instead, a message is sent via the edge,
    // and a vertex property is added. So the resource vertex can emit an extra key value pair, key being this constant.
    // Here, -10 is just a number that is not used as a type id.
    public static final int RESERVED_TYPE_LABEL_KEY = -10;

    GraknMapReduce(Set<LabelId> selectedTypes) {
        this.selectedTypes = selectedTypes;
    }

    GraknMapReduce(Set<LabelId> selectedTypes, AttributeType.DataType resourceDataType) {
        this(selectedTypes);
        persistentProperties.put(RESOURCE_DATA_TYPE_KEY, resourceDataType.name());
    }

    // Needed internally for OLAP tasks
    GraknMapReduce() {
    }

    /**
     * An alternative to the execute method when ghost vertices are an issue. Our "Ghostbuster".
     *
     * @param vertex  a vertex that may be a ghost
     * @param emitter Tinker emitter object
     */
    abstract void safeMap(Vertex vertex, MapEmitter<Serializable, T> emitter);

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);

        // store class name for reflection on spark executor
        configuration.setProperty(MAP_REDUCE, this.getClass().getName());
    }

    @Override
    public final void map(Vertex vertex, MapEmitter<Serializable, T> emitter) {
        // try to deal with ghost vertex issues by ignoring them
        if (Utility.isAlive(vertex)) {
            safeMap(vertex, emitter);
        }
    }

    @Override
    public final void reduce(Serializable key, Iterator<T> values, ReduceEmitter<Serializable, T> emitter) {
        emitter.emit(key, reduceValues(values));
    }

    abstract T reduceValues(Iterator<T> values);

    @Override
    public String getMemoryKey() {
        return this.getClass().getName();
    }

    // super.clone() will always return something of the correct type
    @SuppressWarnings("unchecked")
    @Override
    public MapReduce<Serializable, T, Serializable, T, Map<Serializable, T>> clone() {
        try {
            return (GraknMapReduce) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw GraknAnalyticsException.unreachableStatement(e);
        }
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public final void combine(Serializable key, Iterator<T> values, ReduceEmitter<Serializable, T> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public Map<Serializable, T> generateFinalResult(Iterator<KeyValue<Serializable, T>> iterator) {
        return Utility.keyValuesToMap(iterator);
    }

    final Number resourceValue(Vertex vertex) {
        return usingLong() ? vertex.value(Schema.VertexProperty.VALUE_LONG.name()) :
                vertex.value(Schema.VertexProperty.VALUE_DOUBLE.name());
    }

    final Number minValue() {
        return usingLong() ? Long.MIN_VALUE : Double.MIN_VALUE;
    }

    final Number maxValue() {
        return usingLong() ? Long.MAX_VALUE : Double.MAX_VALUE;
    }

    final boolean usingLong() {
        return persistentProperties.get(RESOURCE_DATA_TYPE_KEY).equals(AttributeType.DataType.LONG.name());
    }
}
