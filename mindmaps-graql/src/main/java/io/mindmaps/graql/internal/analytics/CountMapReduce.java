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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

class CountMapReduce extends MindmapsMapReduce<Long> {

    public static final String MEMORY_KEY = "count";

    public CountMapReduce() {}

    public CountMapReduce(Set<String> types) {
        selectedTypes = types;
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        if (!selectedTypes.isEmpty()) {
            if (selectedTypes.contains(getVertexType(vertex))) {
                emitter.emit(MEMORY_KEY, 1l);
                return;
            }
        } else if (baseTypes.contains(vertex.label())) {
            emitter.emit(MEMORY_KEY, 1l);
            return;
        }

        // TODO: this is a bug with hasNext implementation - must send a message
        emitter.emit(MEMORY_KEY, 0l);
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
}
