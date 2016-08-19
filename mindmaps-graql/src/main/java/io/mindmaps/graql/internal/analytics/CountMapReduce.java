/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.analytics;

import com.google.common.collect.Sets;
import io.mindmaps.constants.DataType;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */

public class CountMapReduce implements MapReduce<Serializable, Long, Serializable, Long, Map<Serializable, Long>> {

    public static final String COUNT_MEMORY_KEY = "analytics.countMapReduce.memoryKey";
    public static final String DEFAULT_KEY = "count";

    private String memoryKey = DEFAULT_KEY;
    private HashSet<String> baseTypes = Sets.newHashSet(
            DataType.BaseType.ENTITY.name(),
            DataType.BaseType.RELATION.name(),
            DataType.BaseType.RESOURCE.name());

    private HashSet<String> analyticsResourceTypes;

    public CountMapReduce() {
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(COUNT_MEMORY_KEY, this.memoryKey);
        configuration.setProperty(MAP_REDUCE, CountMapReduce.class.getName());
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.memoryKey = configuration.getString(COUNT_MEMORY_KEY, DEFAULT_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (baseTypes.contains(vertex.label()))
            emitter.emit(this.memoryKey, 1l);
    }

    @Override
    public void combine(final Serializable key, final Iterator<Long> values, final ReduceEmitter<Serializable, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Long> values, final ReduceEmitter<Serializable, Long> emitter) {
        long count = 0l;
        while (values.hasNext()) {
            count = count + values.next();
        }
        emitter.emit(key, count);
    }

    @Override
    public Map<Serializable, Long> generateFinalResult(final Iterator<KeyValue<Serializable, Long>> keyValues) {
        final Map<Serializable, Long> count = new HashMap<>();
        keyValues.forEachRemaining(pair -> count.put(pair.getKey(), pair.getValue()));
        return count;
    }

    @Override
    public String getMemoryKey() {
        return this.memoryKey;
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.memoryKey);
    }

    @Override
    public MapReduce<Serializable, Long, Serializable, Long, Map<Serializable, Long>> clone() {
        try {
            final CountMapReduce clone = (CountMapReduce) super.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

}
