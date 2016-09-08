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
import io.mindmaps.util.Schema;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.concept.Type;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.analytics.Analytics.TYPE;
import static io.mindmaps.graql.internal.analytics.Analytics.getVertexType;

/**
 *
 */

class CountMapReduce implements MapReduce<Serializable, Long, Serializable, Long, Map<Serializable, Long>> {

    public static final String COUNT_MEMORY_KEY = "analytics.countMapReduce.memoryKey";
    public static final String DEFAULT_MEMORY_KEY = "count";

    private String memoryKey = DEFAULT_MEMORY_KEY;

    private Set<String> baseTypes = Sets.newHashSet(
            Schema.BaseType.ENTITY.name(),
            Schema.BaseType.RELATION.name(),
            Schema.BaseType.RESOURCE.name());

    private Set<String> selectedTypes = null;

    public CountMapReduce() {

    }

    public CountMapReduce(Set<Type> types) {
        selectedTypes = types.stream().map(Type::getId).collect(Collectors.toSet());
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(COUNT_MEMORY_KEY, this.memoryKey);
        configuration.setProperty(MAP_REDUCE, CountMapReduce.class.getName());
        Iterator iterator = selectedTypes.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            configuration.addProperty(TYPE + "." + count, iterator.next());
            count++;
        }
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.memoryKey = configuration.getString(COUNT_MEMORY_KEY, DEFAULT_MEMORY_KEY);
        this.selectedTypes = new HashSet<>();
        configuration.getKeys(TYPE).forEachRemaining(key -> selectedTypes.add(configuration.getString(key)));
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (selectedTypes != null) {
            if (selectedTypes.contains(getVertexType(vertex))) {
                emitter.emit(this.memoryKey, 1l);
            }
        } else if (baseTypes.contains(vertex.label())) {
            emitter.emit(this.memoryKey, 1l);
        }
    }

    @Override
    public void combine(final Serializable key, final Iterator<Long> values, final ReduceEmitter<Serializable, Long> emitter) {
        this.reduce(key, values, emitter);
    }

    @Override
    public void reduce(final Serializable key, final Iterator<Long> values, final ReduceEmitter<Serializable, Long> emitter) {
        emitter.emit(key, IteratorUtils.reduce(values, 0L, (a, b) -> a + b));
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
            throw new IllegalStateException(ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(),e.getMessage()),e);
        }
    }

}
