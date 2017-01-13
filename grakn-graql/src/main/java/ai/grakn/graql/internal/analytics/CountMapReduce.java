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
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class CountMapReduce extends GraknMapReduce<Long> {

    public static final String MEMORY_KEY = "count";

    //Needed internally for OLAP tasks
    public CountMapReduce() {
    }

    public CountMapReduce(Set<TypeName> types) {
        selectedTypes = types;
    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public void safeMap(final Vertex vertex, final MapEmitter<Serializable, Long> emitter) {
        // use the ghost node detector here again
        if (!selectedTypes.isEmpty()) {
            if (selectedTypes.contains(Utility.getVertexType(vertex))) {
                emitter.emit(MEMORY_KEY, 1L);
                return;
            }
        } else if (baseTypes.contains(vertex.label())) {
            emitter.emit(MEMORY_KEY, 1L);
            return;
        }

        // TODO: this is a bug with hasNext implementation - must send a message
        emitter.emit(MEMORY_KEY, 0L);
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
