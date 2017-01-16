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

import ai.grakn.concept.TypeName;
import ai.grakn.util.ErrorMessage;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A map reduce task specific to Grakn with common method implementations.
 */
public abstract class GraknMapReduce<T> extends CommonOLAP
        implements MapReduce<Serializable, T, Serializable, T, Map<Serializable, T>> {

    static final Logger LOGGER = LoggerFactory.getLogger(GraknMapReduce.class);

    public static final String RESOURCE_DATA_TYPE_KEY = "RESOURCE_DATA_TYPE_KEY";
    public static final String MAP_REDUCE_MEMORY_KEY = "GraknMapReduce.memoryKey";

    public GraknMapReduce(Set<TypeName> selectedTypes) {this.selectedTypes = selectedTypes;}

    public GraknMapReduce(Set<TypeName> selectedTypes, String resourceDataType) {
        this(selectedTypes);
        persistentProperties.put(RESOURCE_DATA_TYPE_KEY, resourceDataType);
    }

    // Needed internally for OLAP tasks
    public GraknMapReduce() {
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
    public void map(Vertex vertex, MapEmitter<Serializable, T> emitter) {
        // try to deal with ghost vertex issues by ignoring them
        if (Utility.isAlive(vertex)) {
            safeMap(vertex, emitter);
        }
    }

    @Override
    public String getMemoryKey() {
        return MAP_REDUCE_MEMORY_KEY;
    }

    // super.clone() will always return something of the correct type
    @SuppressWarnings("unchecked")
    @Override
    public MapReduce<Serializable, T, Serializable, T, Map<Serializable, T>> clone() {
        try {
            return (GraknMapReduce) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(),
                    e.getMessage()), e);
        }
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void combine(Serializable key, Iterator<T> values, ReduceEmitter<Serializable, T> emitter) {
       this.reduce(key, values, emitter);
    }

    @Override
    public Map<Serializable, T> generateFinalResult(Iterator<KeyValue<Serializable, T>> iterator) {
        final Map<Serializable, T> result = new HashMap<>();
        iterator.forEachRemaining(pair -> result.put(pair.getKey(), pair.getValue()));
        return result;
    }
}
